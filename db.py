import psycopg2
import os

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
cur = conn.cursor()

directory = "data"
for file_name in os.listdir(directory):
    with open(file_name, 'r') as f:
        next(f)
        cur.copy_from(f, 'forecasts', columns=('longitude', 'latitude', 'time', 'temp_celsius', 'precipitation_hr'), sep=',')
conn.commit()