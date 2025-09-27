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
    county_code VARCHAR(10),
    statename VARCHAR(100),
    county VARCHAR(100),
    ma_region_code VARCHAR(10),
    ma_region VARCHAR(100),
    pdp_region_code VARCHAR(10),
    pdp_region VARCHAR(100)
)
"""
cur.execute(create_table_sql)

# # Insert sample data
# insert_sql = """
# INSERT INTO geographic_locator (
#     county_code, statename, county, ma_region_code, ma_region, pdp_region_code, pdp_region
# ) VALUES (%s, %s, %s, %s, %s, %s, %s)
# """

# data = [
#     ("01000", "Alabama", "Autauga", "10", "Alabama and Tennessee", "12", "Alabama, Tennessee"),
#     ("01010", "Alabama", "Baldwin", "10", "Alabama and Tennessee", "12", "Alabama, Tennessee"),
#     ("01020", "Alabama", "Barbour", "10", "Alabama and Tennessee", "12", "Alabama, Tennessee"),
#     ("01030", "Alabama", "Bibb", "10", "Alabama and Tennessee", "12", "Alabama, Tennessee"),
#     ("01040", "Alabama", "Blount", "10", "Alabama and Tennessee", "12", "Alabama, Tennessee"),
# ]

# cur.executemany(insert_sql, data)
conn.commit()

# Clean up
cur.close()
conn.close()
