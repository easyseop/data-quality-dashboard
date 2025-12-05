import pymysql
from pymysql.cursors import DictCursor

def get_connection():
    return pymysql.connect(
        host="mysql",         
        user="dquser",
        password="1234",
        database="data_quality",
        cursorclass=DictCursor
    )