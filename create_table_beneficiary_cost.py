from connect_db import connect_db

conn = connect_db()

cur = conn.cursor()

# Create table SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS beneficiary_cost (
  CONTRACT_ID VARCHAR(10) NOT NULL,
  PLAN_ID VARCHAR(10),
  SEGMENT_ID VARCHAR(10),
  COVERAGE_LEVEL INT,
  TIER INT,
  DAYS_SUPPLY INT,
  COST_TYPE_PREF INT,
  COST_AMT_PREF DECIMAL(10,2),
  COST_MIN_AMT_PREF DECIMAL(10,2),
  COST_MAX_AMT_PREF DECIMAL(10,2),
  COST_TYPE_NONPREF INT,
  COST_AMT_NONPREF DECIMAL(10,2),
  COST_MIN_AMT_NONPREF DECIMAL(10,2),
  COST_MAX_AMT_NONPREF DECIMAL(10,2),
  COST_TYPE_MAIL_PREF INT,
  COST_AMT_MAIL_PREF DECIMAL(10,2),
  COST_MIN_AMT_MAIL_PREF DECIMAL(10,2),
  COST_MAX_AMT_MAIL_PREF DECIMAL(10,2),
  COST_TYPE_MAIL_NONPREF INT,
  COST_AMT_MAIL_NONPREF DECIMAL(10,2),
  COST_MIN_AMT_MAIL_NONPREF DECIMAL(10,2),
  COST_MAX_AMT_MAIL_NONPREF DECIMAL(10,2),
  TIER_SPECIALTY_YN CHAR(1),
  DED_APPLIES_YN CHAR(1)
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
