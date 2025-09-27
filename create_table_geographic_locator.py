import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cur = conn.cursor()

# Create table SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS geographic_locator (
    county_code VARCHAR(500),
    statename VARCHAR(500),
    county VARCHAR(500),
    ma_region_code VARCHAR(500),
    ma_region VARCHAR(500),
    pdp_region_code VARCHAR(500),
    pdp_region VARCHAR(500)
)
"""
cur.execute(create_table_sql)

# cur.executemany(insert_sql, data)
conn.commit()

# Clean up
cur.close()
conn.close()
