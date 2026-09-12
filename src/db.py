import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Returns ew psycopg2 connection using environment configuration"""
    db_config = {
        "dbname" : os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password" : os.getenv("DB_PASSWORD"),
        "host" : os.getenv("DB_HOST"),
        "port" : "5432"
    }
    return psycopg2.connect(**db_config)