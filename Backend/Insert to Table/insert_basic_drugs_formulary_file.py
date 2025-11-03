import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os


filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\basic drugs formulary file  20250831\basic drugs formulary file  20250831.txt'

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
    if len(fields) == 11:
        if fields[7] == '':  # QUANTITY_LIMIT_AMOUNT
            fields[7] = None
        if fields[8] == '':  # QUANTITY_LIMIT_DAYS
            fields[8] = None
        fields[1] = int(fields[1]) if fields[1] != '' else None  # FORMULARY_VERSION
        fields[2] = int(fields[2]) if fields[2] != '' else None  # CONTRACT_YEAR
        fields[3] = int(fields[3]) if fields[3] != '' else None  # RXCUI
        fields[5] = int(fields[5]) if fields[5] != '' else None  # TIER_LEVEL_VALUE
        data.append(tuple(fields))

insert_sql = """
INSERT INTO basic_drugs_formulary (
    FORMULARY_ID, FORMULARY_VERSION, CONTRACT_YEAR, RXCUI, NDC,
    TIER_LEVEL_VALUE, QUANTITY_LIMIT_YN, QUANTITY_LIMIT_AMOUNT,
    QUANTITY_LIMIT_DAYS, PRIOR_AUTHORIZATION_YN, STEP_THERAPY_YN
) VALUES %s
"""

try:
    batch_size = 5000  # Tune batch size for performance
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

print(f"Inserted {len(data)} rows into basic_drugs_formulary.")




# import psycopg2
# from dotenv import load_dotenv
# import os

# filename = r'Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-08\2025_20250821\basic drugs formulary file  20250831\basic drugs formulary file  20250831.txt'

# # Load environment variables from .env file
# load_dotenv()

# conn = psycopg2.connect(
#     host=os.getenv("DB_HOST"),
#     port=os.getenv("DB_PORT"),
#     dbname=os.getenv("DB_NAME"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD")
# )

# with open(filename, encoding='utf-8') as file:
#     lines = file.readlines()

# header_skipped = False
# data = []
# c = 0
# for line in lines:
#     c+=1
#     if c%500 == 0:
#         print(line)  # Debug: print each 500 line to verify content
#     if not header_skipped:
#         header_skipped = True
#         continue
#     fields = [x.strip() for x in line.strip().split('|')]
#     if len(fields) == 11:
#         # Convert empty strings in numeric columns to None
#         if fields[7] == '':  # QUANTITY_LIMIT_AMOUNT
#             fields[7] = None
#         if fields[8] == '':  # QUANTITY_LIMIT_DAYS
#             fields[8] = None
#         # Also convert int fields appropriately if needed:
#         fields[1] = int(fields[1]) if fields[1] != '' else None  # FORMULARY_VERSION
#         fields[2] = int(fields[2]) if fields[2] != '' else None  # CONTRACT_YEAR
#         fields[3] = int(fields[3]) if fields[3] != '' else None  # RXCUI
#         fields[5] = int(fields[5]) if fields[5] != '' else None  # TIER_LEVEL_VALUE
#         data.append(tuple(fields))

# # limit to first 10 rows for testing
# # data = data[:10]
# # data = data[1050:]

# insert_sql = """
# INSERT INTO basic_drugs_formulary (
#     FORMULARY_ID, FORMULARY_VERSION, CONTRACT_YEAR, RXCUI, NDC, TIER_LEVEL_VALUE, 
#     QUANTITY_LIMIT_YN, QUANTITY_LIMIT_AMOUNT, QUANTITY_LIMIT_DAYS, PRIOR_AUTHORIZATION_YN, STEP_THERAPY_YN
# ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# """


# try:
#     batch_size = 75
#     cur = conn.cursor() 
#     for i in range(0, len(data), batch_size):
#         temp_data = data[i:i+batch_size]
#         cur.executemany(insert_sql, temp_data)
#         conn.commit()
#         print(f"Inserted rows {i} to {i+batch_size}")
# except Exception as e:
#     print("Error during batch insert:", e)
#     conn.rollback()
# finally:
#     if cur:
#         cur.close()
#     if conn:
#         conn.close()


# print(f"Inserted {len(data)} rows into geographic_locator.")