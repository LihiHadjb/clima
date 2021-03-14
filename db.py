import psycopg2

LON = "Longitude"
LAT = "Latitude"
FOR_TIME = "forecast_time"
TEMP = "Temperature Celsius"
PERCP = "Precipitation Rate mm/hr"


def login_to_db():
    connection = psycopg2.connect(host="ec2-52-71-161-140.compute-1.amazonaws.com",
                                  dbname="d8a5obfa9du048",
                                  user="nobeehmaarddfb",
                                  password="15956bfd9f88359095c6850be56b5e8ed030d15bdb8449a50834f90fe718a70a")
    return connection


def create_table():
    connection = login_to_db()
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE forecasts(
        id SERIAL PRIMARY KEY,
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
    data_for_location = get_data_for_location(lat, lon)
    print(data_for_location)
    return "hello3"


def get_data_for_location(lat, lon):
    connection = login_to_db()
    cursor = connection.cursor()

    query = """
    SELECT 'forecast_time', 'temperature_celsius', 'precipitation_rate_mm_hr'
    FROM forecasts 
    WHERE Latitude = %s AND Longitude = %s
    """

    params = (lat, lon,)
    cursor.execute(query, params)
    return cursor.fetchall()

