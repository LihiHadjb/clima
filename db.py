import psycopg2
from psycopg2.extras import RealDictCursor

LON = "Longitude"
LAT = "Latitude"
FOR_TIME = "forecast_time"
TEMP = "Temperature Celsius"
PERCP = "Precipitation Rate mm/hr"


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
        forecast_time timestamp,
        temperature_celsius real,
        precipitation_rate_mm_hr real)
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
                             columns=('longitude', 'latitude', 'forecast_time', 'temperature_celsius', 'precipitation_rate_mm_hr'),
                             sep=',')
    connection.commit()


def get_summary_for_location(lat, lon):
    connection = login_to_db()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    query = """
    SELECT AVG(temperature_celsius)
    FROM forecasts 
    WHERE latitude = %s AND longitude = %s
    """

    params = (lat, lon,)
    cursor.execute(query, params)
    return cursor.fetchall()


def get_data_for_location(lat, lon):
    connection = login_to_db()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    query = """
    SELECT forecast_time, temperature_celsius, precipitation_rate_mm_hr
    FROM forecasts 
    WHERE latitude = %s AND longitude = %s
    """

    params = (lat, lon,)
    cursor.execute(query, params)
    return cursor.fetchall()

