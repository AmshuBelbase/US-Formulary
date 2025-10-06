from connect_db import connect_db

conn = connect_db()
cur = conn.cursor()

# Create table SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS prescribers_by_geography_drug (
    Year INT,
    Prscrbr_Geo_Lvl VARCHAR(50),
    Prscrbr_Geo_Cd VARCHAR(20),
    Prscrbr_Geo_Desc VARCHAR(100),
    Brnd_Name VARCHAR(150),
    Gnrc_Name VARCHAR(150),
    Tot_Prscrbrs INT,
    Tot_Clms INT,
    Tot_30day_Fills DECIMAL(15,2),
    Tot_Drug_Cst DECIMAL(20,2),
    Tot_Benes INT,
    GE65_Sprsn_Flag CHAR(1),
    GE65_Tot_Clms INT,
    GE65_Tot_30day_Fills DECIMAL(15,2),
    GE65_Tot_Drug_Cst DECIMAL(20,2),
    GE65_Bene_Sprsn_Flag CHAR(1),
    GE65_Tot_Benes INT,
    LIS_Bene_Cst_Shr DECIMAL(15,2),
    NonLIS_Bene_Cst_Shr DECIMAL(15,2),
    Opioid_Drug_Flag CHAR(1),
    Opioid_LA_Drug_Flag CHAR(1),
    Antbtc_Drug_Flag CHAR(1),
    Antpsyct_Drug_Flag CHAR(1)
)
"""

try:
    cur.execute(create_table_sql)
    conn.commit()
    print("Table 'prescriber_drug_summary' created successfully.")
except Exception as e:
    print("Error creating table:", e)
    conn.rollback()

# Clean up
cur.close()
conn.close()
