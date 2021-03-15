import psycopg2
from psycopg2.extras import RealDictCursor
import json
#
# LON = "Longitude"
# LAT = "Latitude"
# FOR_TIME = "forecast_time"
# TEMP = "Temperature Celsius"
# PERCP = "Precipitation Rate mm/hr"


def login_to_db():
    connection = psycopg2.connect(host="ec2-54-164-241-193.compute-1.amazonaws.com",
                                  dbname="d95c61aiaqslf",
                                  user="sopngofbxlguxk",
                                  password="32f82f798dab715513be30c5b671932cd0a09e808202dfca61f680500fb98dc9")
    return connection


def create_table():
    connection = login_to_db()
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS forecasts(
        longitude real,
        latitude real,
        forecastTime timestamp,
        Temperature real,
        Precipitation real)
    """)
    cursor.close()
    connection.commit()


def load_data():
    connection = login_to_db()
    cursor = connection.cursor()

    # directory = "data"
    # for file in os.listdir(directory):
    #     file_name = os.fsdecode(file)
    #for file_name in ["file1.csv", "file2.csv", "file3.csv"]:
    for file_name in ["file1.csv"]:
        with open(file_name, 'r') as f:
            next(f)
            cursor.copy_from(f, 'forecasts',
                             columns=('longitude', 'latitude', 'forecastTime','Temperature', 'Precipitation'),
                             sep=',')
    connection.commit()


def execute_query_for_location(lat, lon, query, isResultList):
    connection = login_to_db()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    params = (lat, lon,)
    cursor.execute(query, params)

    if(isResultList):
        return cursor.fetchall()
    else:
        return cursor.fetchone()



def get_summary_for_location(lat, lon):
    query = """
    SELECT AVG(Temperature) AS "Temperature_avg",
          AVG(Precipitation) AS "Precipitation_avg",
          MAX(Temperature) AS "temperature_celsius_max",
          MAX(Precipitation) AS "Precipitation_max",
          MIN(Temperature) AS "Temperature_min",
          MIN(Precipitation) AS "Precipitation_min"

    FROM forecasts
    WHERE latitude = %s AND longitude = %s
    """

    name_to_value = execute_query_for_location(lat, lon, query, False)
    return
    max_values ={}
    max_values['Temperature'] = name_to_value['Temperature_max']
    max_values['Precipitation'] = name_to_value['Precipitation_max']

    min_values ={}
    min_values['Temperature'] = name_to_value['Temperature_min']
    min_values['Precipitation'] = name_to_value['Precipitation_min']

    avg_values ={}
    avg_values['Temperature'] = name_to_value['Temperature_avg']
    avg_values['Precipitation'] = name_to_value['Precipitation_avg']

    result = {}
    result['max'] = max_values
    result['min'] = min_values
    result['avg'] = avg_values

    return result


def get_data_for_location(lat, lon):
    query = """
    SELECT forecastTime, Temperature, Precipitation
    FROM forecasts 
    WHERE latitude = %s AND longitude = %s
    """

    return execute_query_for_location(lat, lon, query, True)

