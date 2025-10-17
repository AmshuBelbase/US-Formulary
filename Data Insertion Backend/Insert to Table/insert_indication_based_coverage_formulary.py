import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\indication based coverage formulary file  20250831\Indication Based Coverage Formulary File  20250831.txt'

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
    c += 1
    if not header_skipped:
        header_skipped = True
        continue
    fields = [x.strip() for x in line.strip().split('|')]
    # Assuming data columns: CONTRACT_ID|PLAN_ID|RXCUI|DISEASE exactly as sample and 4 fields per line
    if len(fields) == 4:
        try:
            rxcui_int = int(fields[2]) if fields[2] != '' else None
            data.append((fields[0], fields[1], rxcui_int, fields[3]))
        except ValueError:
            print(f"Skipping line {c} due to RXCUI conversion error: {fields[2]}")
            continue

insert_sql = """
INSERT INTO indication_based_coverage_formulary (contract_id, plan_id, rxcui, disease)
VALUES %s
ON CONFLICT DO NOTHING
"""

try:
    batch_size = 5000
    with conn.cursor() as cur:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_values(cur, insert_sql, batch)
            conn.commit()
            print(f"Inserted rows {i+1} to {i+len(batch)}")
except Exception as e:
    print("Error during batch insert:", e)
    conn.rollback()
finally:
    conn.close()

print(f"Inserted {len(data)} rows into indication_based_coverage_formulary.")
