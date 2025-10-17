import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os


filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\plan information  20250831\plan information  20250831.txt'

# Load environment variables from .env file
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

# with open(filename, encoding='utf-8') as file:
    # lines = file.readlines()

with open(filename, encoding='utf-8', errors='ignore') as file:
    lines = file.readlines()


header_skipped = False
data = []
c = 0
for line in lines:
    c+=1
    if c%5000 == 0:
        print(line)  # Debug: print each 500 line to verify content
    if not header_skipped:
        header_skipped = True
        continue
    fields = [x.strip() for x in line.strip().split('|')]
    if len(fields) == 14:
        data.append(tuple(fields))


print(f"Inserting {len(data)} rows.")


insert_sql = """
INSERT INTO plan_info (
  CONTRACT_ID, PLAN_ID, SEGMENT_ID, CONTRACT_NAME, PLAN_NAME, FORMULARY_ID,
  PREMIUM, DEDUCTIBLE, MA_REGION_CODE, PDP_REGION_CODE, STATE, COUNTY_CODE,
  SNP, PLAN_SUPPRESSED_YN
) VALUES %s
"""



try:
    batch_size = 1000  # Tune batch size for performance
    with conn.cursor() as cur:
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            execute_values(cur, insert_sql, batch)
            conn.commit()
            print(f"Inserted rows {i+1} to {i+len(batch)}")
except Exception as e:
    print("Error during batch insert:", e)
    conn.rollback()
finally:
    conn.close()

# print(f"Inserted {len(data)} rows into basic_drugs_formulary.")