from connect_db import connect_db
from psycopg2 import sql
from psycopg2.extras import execute_values


# connect (connect_db should return a psycopg2 connection)
conn = connect_db()
cur = conn.cursor()

# create table SQL matching sample data columns
create_table_sql = """
CREATE TABLE IF NOT EXISTS insulin_beneficiary_cost (
  contract_id VARCHAR(20) NOT NULL,
  plan_id VARCHAR(20) NOT NULL,
  segment_id VARCHAR(20) NOT NULL,
  tier VARCHAR(10) NOT NULL,
  days_supply INTEGER NOT NULL,
  copay_amt_pref_insln NUMERIC(10,2),
  copay_amt_nonpref_insln NUMERIC(10,2),
  copay_amt_mail_pref_insln NUMERIC(10,2),
  copay_amt_mail_nonpref_insln NUMERIC(10,2),
  PRIMARY KEY (contract_id, plan_id, segment_id, tier, days_supply)
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
