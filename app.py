from flask import Flask
from psycopg2 import Error
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import request
from db import login_to_db, LON, LAT, FOR_TIME, TEMP, PERCP


app = Flask(__name__)

connection = login_to_db()
cursor = connection.cursor(cursor_factory=RealDictCursor)

def get_summary_for_location(lat, lon):
    query = """
    SELECT MAX(temp_celsius), MAX(precipitation_hr), MIN(temp_celsius), MIN(precipitation_hr), AVG(temp_celsius), AVG(precipitation_hr)
    FROM forecasts 
    WHERE Latitude = %s AND Longitude = %s
    """

    params = (lat, lon,)
    cursor.execute(query, params)
    result = cursor.fetchall()
    print(result)
    return result


def get_data_for_location(lat, lon):
    query = """
    SELECT forecast_time, Temperature Celsius, Precipitation Rate mm/hr
    FROM forecasts 
    WHERE Latitude = %s AND Longitude = %s
    """

    params = (lat, lon,)
    cursor.execute(query, params)
    result = cursor.fetchall()


@app.route('/weather/data')
def data():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return get_data_for_location(lat, lon)[0]


@app.route('/weather/summarize')
def summarize():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return get_summary_for_location(lat, lon)


if __name__ == '__main__':
    app.run()
