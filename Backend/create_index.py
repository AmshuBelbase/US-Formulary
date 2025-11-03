import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# ---------------------------
# 1) DB connection (your snippet)
# ---------------------------
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True
cur = conn.cursor()

# Define all index statements
index_statements = [

    # --- geographic_locator ---
    "CREATE INDEX IF NOT EXISTS idx_geo_ma_region_code   ON geographic_locator(ma_region_code)",
    "CREATE INDEX IF NOT EXISTS idx_geo_pdp_region_code  ON geographic_locator(pdp_region_code)",
    "CREATE INDEX IF NOT EXISTS idx_geo_county_code      ON geographic_locator(county_code)",
    "CREATE INDEX IF NOT EXISTS idx_geo_statename        ON geographic_locator(statename)",

    # --- basic_drugs_formulary ---
    "CREATE INDEX IF NOT EXISTS idx_bdf_formulary_id        ON basic_drugs_formulary(formulary_id)",
    "CREATE INDEX IF NOT EXISTS idx_bdf_formulary_version   ON basic_drugs_formulary(formulary_version)",
    "CREATE INDEX IF NOT EXISTS idx_bdf_contract_year       ON basic_drugs_formulary(contract_year)",
    "CREATE INDEX IF NOT EXISTS idx_bdf_tier_level_value    ON basic_drugs_formulary(tier_level_value)",
    "CREATE INDEX IF NOT EXISTS idx_bdf_rxcui               ON basic_drugs_formulary(rxcui)",
    "CREATE INDEX IF NOT EXISTS idx_bdf_ndc                 ON basic_drugs_formulary(ndc)",
    "CREATE INDEX IF NOT EXISTS idx_bdf_formulary_year_tier ON basic_drugs_formulary(formulary_id, contract_year, tier_level_value)",

    # --- prescribers_by_geography_drug ---
    "CREATE INDEX IF NOT EXISTS idx_pbgd_year_geo_lvl      ON prescribers_by_geography_drug(year, prscrbr_geo_lvl)",
    "CREATE INDEX IF NOT EXISTS idx_pbgd_geo_cd            ON prescribers_by_geography_drug(prscrbr_geo_cd)",
    "CREATE INDEX IF NOT EXISTS idx_pbgd_geo_desc          ON prescribers_by_geography_drug(prscrbr_geo_desc)",
    "CREATE INDEX IF NOT EXISTS idx_pbgd_brand_name        ON prescribers_by_geography_drug(brnd_name)",
    "CREATE INDEX IF NOT EXISTS idx_pbgd_generic_name      ON prescribers_by_geography_drug(gnrc_name)",

    # --- beneficiary_cost ---
    "CREATE INDEX IF NOT EXISTS idx_bc_contract_plan_seg ON beneficiary_cost(contract_id, plan_id, segment_id)",
    "CREATE INDEX IF NOT EXISTS idx_bc_tier              ON beneficiary_cost(tier)",
    "CREATE INDEX IF NOT EXISTS idx_bc_coverage_level    ON beneficiary_cost(coverage_level)",

    # --- plan_info ---
    "CREATE INDEX IF NOT EXISTS idx_pi_contract_plan_seg   ON plan_info(contract_id, plan_id, segment_id)",
    "CREATE INDEX IF NOT EXISTS idx_pi_formulary_id        ON plan_info(formulary_id)",
    "CREATE INDEX IF NOT EXISTS idx_pi_state               ON plan_info(state)",
    "CREATE INDEX IF NOT EXISTS idx_pi_ma_region_code      ON plan_info(ma_region_code)",
    "CREATE INDEX IF NOT EXISTS idx_pi_pdp_region_code     ON plan_info(pdp_region_code)",
    "CREATE INDEX IF NOT EXISTS idx_pi_county_code         ON plan_info(county_code)",

    # --- excluded_drugs_formulary ---
    "CREATE INDEX IF NOT EXISTS idx_edf_contract_plan      ON excluded_drugs_formulary(contract_id, plan_id)",
    "CREATE INDEX IF NOT EXISTS idx_edf_rxcui              ON excluded_drugs_formulary(rxcui)",
    "CREATE INDEX IF NOT EXISTS idx_edf_tier               ON excluded_drugs_formulary(tier)",

    # --- indication_based_coverage_formulary ---
    "CREATE INDEX IF NOT EXISTS idx_ibcf_contract_plan  ON indication_based_coverage_formulary(contract_id, plan_id)",
    "CREATE INDEX IF NOT EXISTS idx_ibcf_rxcui          ON indication_based_coverage_formulary(rxcui)",
    "CREATE INDEX IF NOT EXISTS idx_ibcf_disease        ON indication_based_coverage_formulary USING gin (to_tsvector('english', disease))",
]

# Execute all indexes safely
for stmt in index_statements:
    try:
        cur.execute(sql.SQL(stmt))
        print(f"✅ Created: {stmt.split('ON')[0].strip()}")
    except Exception as e:
        print(f"❌ Failed: {stmt}\n   Error: {e}")

cur.close()
conn.close()
print("\n🎯 All index creation commands executed.")
