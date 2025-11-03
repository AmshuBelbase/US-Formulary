import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
conn = None  # Global connection variable store

def connect_db():
    global conn
    if conn is not None:
        # Check if connection is still open
        try:
            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.close()
            return conn  # Return existing valid connection
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            # Connection is closed or broken, so reset it
            conn = None

    # Create new connection if none exists or previous one failed
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn

# Usage example
# Later, when you need a connection:
# conn = connect_db()