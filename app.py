from flask import Flask
from psycopg2 import Error
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import request
from db import init_db


app = Flask(__name__)

try:

    connection = psycopg2.connect(host="ec2-52-71-161-140.compute-1.amazonaws.com",
                                  dbname="d8a5obfa9du048",
                                  user="nobeehmaarddfb",
                                  password="15956bfd9f88359095c6850be56b5e8ed030d15bdb8449a50834f90fe718a70a")
except (Exception, Error) as error:
    print("Connecting to PostgreSQL failed!", error)

cursor = connection.cursor(cursor_factory=RealDictCursor)


@app.route('/')
def hello_world():
    cursor.execute("SELECT * FROM forecasts WHERE latitude = -90 AND longitude = -179")
    result = cursor.fetchall()
    return result[0]

def get_summary_for_location(lat, lon):
    query = """
    SELECT MAX(temp_celsius), MAX(precipitation_hr), MIN(temp_celsius), MIN(precipitation_hr), AVG(temp_celsius), AVG(precipitation_hr)
    FROM forecasts 
    WHERE latitude = %s AND longitude = %s
    """

    params = (lat, lon,)
    cursor.execute(query, params)
    result = cursor.fetchall()
    print(result)
    return result

@app.route('/weather/summarize')
def summarize():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return get_summary_for_location(lat, lon)


if __name__ == '__main__':
    app.run()
