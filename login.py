import psycopg2

HOST="ec2-54-164-241-193.compute-1.amazonaws.com"
DBNAME="d95c61aiaqslf"
USER="sopngofbxlguxk"
PASSWORD="32f82f798dab715513be30c5b671932cd0a09e808202dfca61f680500fb98dc9"


def login():
    connection = psycopg2.connect(host=HOST,
                                  dbname=DBNAME,
                                  user=USER,
                                  password=PASSWORD)
    return connection