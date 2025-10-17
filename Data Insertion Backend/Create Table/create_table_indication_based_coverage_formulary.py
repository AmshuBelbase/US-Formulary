from connect_db import connect_db
from psycopg2 import sql
from psycopg2.extras import execute_values

# connect (connect_db should return a psycopg2 connection)
conn = connect_db()
cur = conn.cursor()

# create table SQL (match sample columns)
create_table_sql = """
CREATE TABLE IF NOT EXISTS indication_based_coverage_formulary (
  contract_id VARCHAR(20) NOT NULL,
  plan_id VARCHAR(20) NOT NULL,
  rxcui BIGINT NOT NULL,
  disease TEXT NOT NULL,
  PRIMARY KEY (contract_id, plan_id, rxcui, disease)
)
"""

try:
    cur.execute(create_table_sql)
    conn.commit()
except Exception as e:
    print("Error creating table:", e)
    conn.rollback()
    cur.close()
    conn.close()
    raise
finally:
    cur.close()
    conn.close()