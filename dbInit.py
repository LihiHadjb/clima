import os
from login import login

class DBInitializer():
    def create_table(self):
        connection = login()
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
        connection = login()
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

init = DBInitializer()
init.create_table()
init.load_data("data")