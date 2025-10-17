
import psycopg2
from dotenv import load_dotenv
import os

filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\geographic locator file  20250831\geographic locator file 20250831.txt'

# Load environment variables from .env file
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

with open(filename, encoding='utf-8') as file:
    lines = file.readlines()

header_skipped = False
data = []
c = 0
for line in lines:
    c+=1
    if c%500 == 0:
        print(line)  # Debug: print each 500 line to verify content
    if not header_skipped:
        header_skipped = True
        continue
    fields = [x.strip() for x in line.strip().split('|')]
    if len(fields) == 7:
        data.append(tuple(fields))

# limit to first 10 rows for testing
# data = data[:10]
# data = data[1050:]

insert_sql = """
INSERT INTO geographic_locator (
    county_code, statename, county, ma_region_code, ma_region, pdp_region_code, pdp_region
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

try:
    batch_size = 60
    cur = conn.cursor() 
    for i in range(0, len(data), batch_size):
        temp_data = data[i:i+batch_size]
        cur.executemany(insert_sql, temp_data)
        conn.commit()
        print(f"Inserted rows {i} to {i+batch_size}")
except Exception as e:
    print("Error during batch insert:", e)
    conn.rollback()
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()


print(f"Inserted {len(data)} rows into geographic_locator.")