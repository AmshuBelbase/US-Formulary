import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\insulin beneficiary cost file  20250831\insulin beneficiary cost file  20250831.txt'

# Load environment variables from .env file
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

with open(filename, encoding='utf-8', errors='ignore') as file:
    lines = file.readlines()

header_skipped = False
data = []
c = 0
for line in lines:
    c += 1
    if c % 5000 == 0:
        print(line)  # Debug: print each 5000th line to verify content
    if not header_skipped:
        header_skipped = True
        continue
    fields = [x.strip() for x in line.strip().split('|')]
    if len(fields) == 9:
        # Convert empty strings in numeric fields to None, then cast appropriate fields to int or float
        # Fields: CONTRACT_ID|PLAN_ID|SEGMENT_ID|TIER|DAYS_SUPPLY|copay_amt_pref_insln|copay_amt_nonpref_insln|copay_amt_mail_pref_insln|copay_amt_mail_nonpref_insln
        # Convert tier and days_supply to int if possible; copay columns to float
        if fields[3] == '.':
            # Tier is '.' in sample data, keep as string for now
            tier_value = fields[3]
        else:
            try:
                tier_value = int(fields[3])
            except ValueError:
                tier_value = fields[3]
        # days_supply integer conversion
        try:
            days_supply_value = int(fields[4])
        except ValueError:
            days_supply_value = None

        def parse_float(val):
            if val == '' or val is None:
                return None
            try:
                return float(val)
            except ValueError:
                return None

        copay_amt_pref_insln = parse_float(fields[5])
        copay_amt_nonpref_insln = parse_float(fields[6])
        copay_amt_mail_pref_insln = parse_float(fields[7])
        copay_amt_mail_nonpref_insln = parse_float(fields[8])

        data.append((
            fields[0],  # contract_id
            fields[1],  # plan_id
            fields[2],  # segment_id
            tier_value,
            days_supply_value,
            copay_amt_pref_insln,
            copay_amt_nonpref_insln,
            copay_amt_mail_pref_insln,
            copay_amt_mail_nonpref_insln
        ))

print(f"Inserting {len(data)} rows.")

insert_sql = """
INSERT INTO insulin_beneficiary_cost (
  contract_id, plan_id, segment_id, tier, days_supply,
  copay_amt_pref_insln, copay_amt_nonpref_insln,
  copay_amt_mail_pref_insln, copay_amt_mail_nonpref_insln
) VALUES %s
"""

try:
    batch_size = 1000  # Tune batch size for performance
    with conn.cursor() as cur:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_values(cur, insert_sql, batch)
            conn.commit()
            print(f"Inserted rows {i + 1} to {i + len(batch)}")
except Exception as e:
    print("Error during batch insert:", e)
    conn.rollback()
finally:
    conn.close()
