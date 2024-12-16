from flask import Flask, render_template, jsonify
import os
import json
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ships')
def ships():
    # Pobierz ostatnie dane z BQ
    client = bigquery.Client()
    query = """
    SELECT * FROM `ais-collision-detection.ais_dataset.ships_positions`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
    """
    rows = client.query(query).result()
    data = [dict(r) for r in rows]
    return jsonify(data)

@app.route('/collisions')
def collisions():
    client = bigquery.Client()
    query = """
    SELECT * FROM `ais-collision-detection.ais_dataset.collisions`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    """
    rows = client.query(query).result()
    data = [dict(r) for r in rows]
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)