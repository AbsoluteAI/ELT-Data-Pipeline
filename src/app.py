from flask import Flask, render_template
from confluent_kafka import Producer
import os

KAFKA_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')

producer = Producer(bootstrap_servers=KAFKA_SERVERS)

app = Flask(__name__)

@app.route('/')

def template():
    return render_template('template.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)