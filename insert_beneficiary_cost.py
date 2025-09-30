import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os


filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\beneficiary cost file  20250831\beneficiary cost file  20250831.txt'

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
    if c%5000 == 0:
        print(line)  # Debug: print each 500 line to verify content
    if not header_skipped:
        header_skipped = True
        continue
    fields = [x.strip() for x in line.strip().split('|')]
    if len(fields) == 24:
        # if fields[7] == '':  # QUANTITY_LIMIT_AMOUNT
        #     fields[7] = None
        # if fields[8] == '':  # QUANTITY_LIMIT_DAYS
        #     fields[8] = None
        # fields[1] = int(fields[1]) if fields[1] != '' else None  # FORMULARY_VERSION
        # fields[2] = int(fields[2]) if fields[2] != '' else None  # CONTRACT_YEAR
        # fields[3] = int(fields[3]) if fields[3] != '' else None  # RXCUI
        # fields[5] = int(fields[5]) if fields[5] != '' else None  # TIER_LEVEL_VALUE
        data.append(tuple(fields))


print(f"Inserting {len(data)} rows.")


insert_sql = """
INSERT INTO beneficiary_cost (
  CONTRACT_ID, PLAN_ID, SEGMENT_ID, COVERAGE_LEVEL, TIER, DAYS_SUPPLY,
  COST_TYPE_PREF, COST_AMT_PREF, COST_MIN_AMT_PREF, COST_MAX_AMT_PREF,
  COST_TYPE_NONPREF, COST_AMT_NONPREF, COST_MIN_AMT_NONPREF, COST_MAX_AMT_NONPREF,
  COST_TYPE_MAIL_PREF, COST_AMT_MAIL_PREF, COST_MIN_AMT_MAIL_PREF, COST_MAX_AMT_MAIL_PREF,
  COST_TYPE_MAIL_NONPREF, COST_AMT_MAIL_NONPREF, COST_MIN_AMT_MAIL_NONPREF, COST_MAX_AMT_MAIL_NONPREF,
  TIER_SPECIALTY_YN, DED_APPLIES_YN
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