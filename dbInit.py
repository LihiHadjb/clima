import psycopg2
import os

HOST="ec2-54-164-241-193.compute-1.amazonaws.com"
DBNAME="d95c61aiaqslf"
USER="sopngofbxlguxk"
PASSWORD="32f82f798dab715513be30c5b671932cd0a09e808202dfca61f680500fb98dc9"


class DBInitializer():
    def login(self):
        connection = psycopg2.connect(host=HOST,
                                      dbname=DBNAME,
                                      user=USER,
                                      password=PASSWORD)
        return connection

    def create_table(self):
        connection = self.login()
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

    def load_data(self, data_dir):
        print("loading!!!!")
        connection = self.login()
        cursor = connection.cursor()

        for file in os.listdir(data_dir):
            file_name = os.path.join(data_dir, file)
            print(file_name)
            with open(file_name, 'r') as f:
                next(f)
                cursor.copy_from(f, 'forecasts',
                                 columns=('longitude', 'latitude', 'forecastTime', 'Temperature', 'Precipitation'),
                                 sep=',')
        cursor.close()
        connection.commit()
