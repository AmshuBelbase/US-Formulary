from connect_db import connect_db

conn = connect_db()

cur = conn.cursor()

# Create table SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS plan_info (
  CONTRACT_ID VARCHAR(10) NOT NULL,
  PLAN_ID VARCHAR(10),
  SEGMENT_ID VARCHAR(10),
  CONTRACT_NAME VARCHAR(100),
  PLAN_NAME VARCHAR(150),
  FORMULARY_ID VARCHAR(20),
  PREMIUM DECIMAL(10,2),
  DEDUCTIBLE INT,
  MA_REGION_CODE VARCHAR(10),
  PDP_REGION_CODE VARCHAR(10),
  STATE CHAR(2),
  COUNTY_CODE VARCHAR(10),
  SNP INT,
  PLAN_SUPPRESSED_YN CHAR(1)
)
"""

try:
    cur.execute(create_table_sql)
except Exception as e:
    print("Error creating table:", e)
    conn.rollback()

# cur.executemany(insert_sql, data)
conn.commit()

# Clean up
cur.close()
conn.close()
