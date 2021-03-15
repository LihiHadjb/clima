from flask import Flask
from psycopg2 import Error
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import request
from db import login_to_db, get_data_for_location, get_summary_for_location, create_table
from flask import jsonify



app = Flask(__name__)

@app.route('/weather/data')
def data():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return jsonify(get_data_for_location(lat, lon))


@app.route('/weather/summarize')
def summarize():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return get_summary_for_location(lat, lon)


if __name__ == '__main__':
    app.run()
