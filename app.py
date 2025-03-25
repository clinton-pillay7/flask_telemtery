import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import os
from flask import Flask, request, render_template, jsonify
import traceback
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# Set a service name
resource = Resource.create({"service.name": "flask-app"})

# Set up Flask init variables
app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Set up OpenTelemetry Tracer
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

# Use OTLP HTTP Exporter (not gRPC)
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")


# Instrument Flask app
FlaskInstrumentor().instrument_app(app)


trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(otlp_exporter)
)

# DB connection params
conn_str = (
    'DRIVER=ODBC Driver 17 for SQL Server;'
    'SERVER=192.168.8.100\\SQLEXPRESS;'
    'DATABASE=flask;'
    'UID=flaskuser;'
    'PWD=flaskpassword'
)

# Connection function
def connection(conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        print("Connection successful")
        return conn
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None  # Return None instead of string "Error"

@app.route('/')
def index():
    with tracer.start_as_current_span("home-span"):
         return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    with tracer.start_as_current_span("upload and insert"):
       if 'file' not in request.files:
           print("No file part")
           return jsonify({"error": "No file part"}), 400

       file = request.files['file']
       if file.filename == '':
           print("No selected file")
           return jsonify({"error": "No selected file"}), 400

       if file and file.filename.endswith('.csv'):
           filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
           file.save(filepath)

           try:
               df = pd.read_csv(filepath, delimiter=",")

               dbconn = connection(conn_str)
               if not dbconn:
                   return jsonify({"error": "Database connection failed"}), 500

               cursor = dbconn.cursor()
               with tracer.start_as_current_span("sql-insert"):
                    for _, row in df.iterrows():
                        cursor.execute(
                            "INSERT INTO flask.dbo.flasktable (uuid, ipv6, device_category, mac) VALUES (?, ?, ?, ?)",
                            row["uuid"], row["ipv6"], row["device_category"], row["mac"]
                        )

               dbconn.commit()
               return jsonify({"message": "File processed and data inserted successfully"}), 200

           except Exception as e:
               traceback.print_exc()
               return jsonify({"error": str(e)}), 500

           finally:
               if dbconn:
                   dbconn.close()

       return jsonify({"error": "Invalid file format"}), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
