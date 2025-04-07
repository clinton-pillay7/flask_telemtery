import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import os
from flask import Flask, request, render_template, jsonify, url_for, redirect, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
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
from opentelemetry.trace import get_current_span


# Set a service name for otel
resource = Resource.create({"service.name": "flask-app"})

# Set up OpenTelemetry Tracer
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

# Use OTLP HTTP Exporter (not gRPC)
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")

# Instrument Flask app
#FlaskInstrumentor().instrument_app(app)

# creates object to send traces to OTLP via batches
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))


# Set up Flask init variables
app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.secret_key = "secret_key"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Authentication initialisation
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

#Mock User Database
users = {'admin': {'password': 'password123'},'clinton': {'password': 'password123'}}

# User class inherits from UserMixin, which provides default implementations of methods
class User(UserMixin):
    def __init__(self, username):
        self.id = username

# Flask-Login Callback, to validate if user is registered
@login_manager.user_loader
def load_user(user_id):
    if user_id in users:
        return User(user_id)
    return None


# login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in users and users[username]['password'] == password:
            user = User(username)
            login_user(user)
            #return redirect(url_for('index'))
            return render_template('base.html', username=username)
    return render_template('login.html')


# logout function
@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


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
@login_required
def index():
    with tracer.start_as_current_span("home-span"):
         username_id = login()
         return render_template('base.html', username_id=username_id)


@app.route('/upload')
@login_required
def upload_page():
    with tracer.start_as_current_span("home-span"):
         return render_template('upload.html')


@app.route('/query')
@login_required
def query():
    with tracer.start_as_current_span("query"):
        dbconn = connection(conn_str)
        if not dbconn:
            return jsonify({"error": "Database connection failed during query function"}), 500

        try:
            cursor = dbconn.cursor()
            cursor.execute("SELECT * FROM flask.dbo.flasktable")  # Customize query
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            return render_template("query.html", columns=columns, rows=rows)
        except Exception as e:
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500


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
               span = get_current_span()
               trace_id = format(span.get_span_context().trace_id, "x")
               print(trace_id)
               return jsonify({"message": "File processed and data inserted successfully", "trace_id": trace_id}), 200

           except Exception as e:
               traceback.print_exc()
               return jsonify({"error": str(e)}), 500

           finally:
               if dbconn:
                   dbconn.close()

       return jsonify({"error": "Invalid file format"}), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
