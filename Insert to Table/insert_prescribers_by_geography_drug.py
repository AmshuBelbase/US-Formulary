import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

filename = r'Medicare Part D Prescribers - by Geography and Drug\2023\MUP_DPR_RY25_P04_V10_DY23_Geo.csv'

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
batch_year = 2023 # Example: add the year for all rows
c = 0

def convert_empty_to_none(value, is_int=False):
    if value == '' or value is None:
        return None
    if is_int:
        return int(value)
    try:
        return float(value)
    except ValueError:
        return value  # Fallback, probably a string

for line in lines:
    c += 1
    if c % 5000 == 0:
        print(line)  # Debug: print every 5000th line to verify content
    if not header_skipped:
        header_skipped = True
        continue
    fields = [x.strip() for x in line.strip().split(',')]
    # Expecting 22 fields as per table columns excluding Year which is added here
    if len(fields) == 22:
        # Insert year as first column
        # data.append((batch_year, *fields))
        transformed = [
            fields[0],  # Prscrbr_Geo_Lvl (string)
            fields[1],  # Prscrbr_Geo_Cd (string)
            fields[2],  # Prscrbr_Geo_Desc (string)
            fields[3],  # Brnd_Name (string)
            fields[4],  # Gnrc_Name (string)
            convert_empty_to_none(fields[5], is_int=True),  # Tot_Prscrbrs
            convert_empty_to_none(fields[6], is_int=True),  # Tot_Clms
            convert_empty_to_none(fields[7], is_int=False),  # Tot_30day_Fills
            convert_empty_to_none(fields[8], is_int=False),  # Tot_Drug_Cst
            convert_empty_to_none(fields[9], is_int=True),  # Tot_Benes
            fields[10] if fields[10] != '' else None,  # GE65_Sprsn_Flag (char)
            convert_empty_to_none(fields[11], is_int=True),  # GE65_Tot_Clms
            convert_empty_to_none(fields[12], is_int=False),  # GE65_Tot_30day_Fills
            convert_empty_to_none(fields[13], is_int=False),  # GE65_Tot_Drug_Cst
            fields[14] if fields[14] != '' else None,  # GE65_Bene_Sprsn_Flag (char)
            convert_empty_to_none(fields[15], is_int=True),  # GE65_Tot_Benes
            convert_empty_to_none(fields[16], is_int=False),  # LIS_Bene_Cst_Shr
            convert_empty_to_none(fields[17], is_int=False),  # NonLIS_Bene_Cst_Shr
            fields[18] if fields[18] != '' else None,  # Opioid_Drug_Flag (char)
            fields[19] if fields[19] != '' else None,  # Opioid_LA_Drug_Flag (char)
            fields[20] if fields[20] != '' else None,  # Antbtc_Drug_Flag (char)
            fields[21] if fields[21] != '' else None   # Antpsyct_Drug_Flag (char)
        ]
        data.append((batch_year, *transformed))

# data = data[:74001]  # Limit to first 10,000 rows for testing
# data = data[-2863:]

print(f"Inserting {len(data)} rows.")

insert_sql = """
INSERT INTO prescribers_by_geography_drug (
    Year,
    Prscrbr_Geo_Lvl,
    Prscrbr_Geo_Cd,
    Prscrbr_Geo_Desc,
    Brnd_Name,
    Gnrc_Name,
    Tot_Prscrbrs,
    Tot_Clms,
    Tot_30day_Fills,
    Tot_Drug_Cst,
    Tot_Benes,
    GE65_Sprsn_Flag,
    GE65_Tot_Clms,
    GE65_Tot_30day_Fills,
    GE65_Tot_Drug_Cst,
    GE65_Bene_Sprsn_Flag,
    GE65_Tot_Benes,
    LIS_Bene_Cst_Shr,
    NonLIS_Bene_Cst_Shr,
    Opioid_Drug_Flag,
    Opioid_LA_Drug_Flag,
    Antbtc_Drug_Flag,
    Antpsyct_Drug_Flag
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
