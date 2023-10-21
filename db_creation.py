import mysql.connector

from dotenv import load_dotenv
from os import getenv

load_dotenv()

# This is for creating a database in mysql without manual help
def create_database(db_name: str) -> None:
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=getenv("MYSQL_PASSWORD")
    )

    mycursor = mydb.cursor()
    
    mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")

    mydb.close()