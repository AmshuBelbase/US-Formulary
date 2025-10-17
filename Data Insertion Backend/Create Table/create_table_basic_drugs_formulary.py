from connect_db import connect_db

conn = connect_db()

cur = conn.cursor()

# Create table SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS basic_drugs_formulary (
  FORMULARY_ID VARCHAR(20) NOT NULL,
  FORMULARY_VERSION INT,
  CONTRACT_YEAR INT,
  RXCUI INT,
  NDC VARCHAR(20),
  TIER_LEVEL_VALUE INT,
  QUANTITY_LIMIT_YN CHAR(1),
  QUANTITY_LIMIT_AMOUNT DECIMAL(5,2),
  QUANTITY_LIMIT_DAYS INT,
  PRIOR_AUTHORIZATION_YN CHAR(1),
  STEP_THERAPY_YN CHAR(1)
)
"""
# cur.execute("""
# ALTER TABLE basic_drugs_formulary 
# ALTER COLUMN QUANTITY_LIMIT_AMOUNT TYPE DECIMAL(7,2);
# """)

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
