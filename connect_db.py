import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

# # ------------- Connect and display Version

# # Create a cursor to perform database operations
# cur = conn.cursor()

# # Example query
# cur.execute("SELECT version();")
# result = cur.fetchone()
# print("Database version:", result)

# # Clean up
# cur.close()
# conn.close()


# # ------------- Connect and display all tables

# cur = conn.cursor()

# # List all tables in the 'public' schema
# cur.execute("""
#     SELECT table_name
#     FROM information_schema.tables
#     WHERE table_schema = 'public'
# """)

# tables = cur.fetchall()
# for table in tables:
#     print(table[0])

# cur.close()
# conn.close()


# ------------- Connect and display structure and row count of all tables

tables = [
    "geographic_locator",
    # "plan_info",
    # "formulary_info",
    # "drug_info",
    # "excluded_formulary",
    # "benefit_costs",
    # "users",
    # "staging_api_raw"
]



cur = conn.cursor()

for table in tables:
    print(f"Table: {table}")
    
    # Get table structure: column names and data types
    cur.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    
    for col in columns:
        print(f"  {col[0]} ({col[1]})")
    
    # Get row count
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row_count = cur.fetchone()[0]
    print(f"Rows: {row_count}\n")

cur.close()
conn.close()
