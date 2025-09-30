from connect_db import connect_db

conn = connect_db()

cur = conn.cursor()

# Create table SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS geographic_locator (
    county_code VARCHAR(50),
    statename VARCHAR(500),
    county VARCHAR(500),
    ma_region_code VARCHAR(500),
    ma_region VARCHAR(500),
    pdp_region_code VARCHAR(500),
    pdp_region VARCHAR(500)
)
"""

# cur.execute("""
# ALTER TABLE geographic_locator 
#   ALTER COLUMN county_code TYPE varchar(500),
#   ALTER COLUMN statename TYPE varchar(500),
#   ALTER COLUMN county TYPE varchar(500),
#   ALTER COLUMN ma_region_code TYPE varchar(500),
#   ALTER COLUMN ma_region TYPE varchar(500),
#   ALTER COLUMN pdp_region_code TYPE varchar(500),
#   ALTER COLUMN pdp_region TYPE varchar(500);
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
