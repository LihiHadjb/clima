import psycopg2

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

    def load_data(self):
        connection = self.login()
        cursor = connection.cursor()

        # directory = "data"
        # for file in os.listdir(directory):
        #     file_name = os.fsdecode(file)
        # for file_name in ["file1.csv", "file2.csv", "file3.csv"]:
        for file_name in ["file1.csv"]:
            with open(file_name, 'r') as f:
                next(f)
                cursor.copy_from(f, 'forecasts',
                                 columns=('longitude', 'latitude', 'forecastTime', 'Temperature', 'Precipitation'),
                                 sep=',')
        cursor.close()
        connection.commit()
