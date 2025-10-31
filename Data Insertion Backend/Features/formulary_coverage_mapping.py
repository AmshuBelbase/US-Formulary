import psycopg2
from dotenv import load_dotenv
import os
from fastapi import FastAPI, Query, HTTPException, status
from typing import Optional
from fastapi.responses import JSONResponse 

# Load environment variables from .env file
load_dotenv()
conn = None  # Global connection variable

def connect_db():
    global conn
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.close()
            return conn
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            conn = None
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn

app = FastAPI()

@app.get("/health")
async def health_check():
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return JSONResponse(content={"status": "ok"})
    except Exception as err:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "Database unavailable",
                "details": str(err)
            }
        )

@app.get("/drug-details")
async def get_drug_details(ndc: Optional[int] = Query(None), rxcui: Optional[int] = Query(None), limit: int = Query(10)):
    if ndc is None and rxcui is None:
        raise HTTPException(status_code=400, detail="Either 'ndc' or 'rxcui' must be provided")
    
    conn = connect_db()
    cur = conn.cursor()

    query = """
    SELECT formulary_id, formulary_version, contract_year, rxcui, ndc, tier_level_value,
           quantity_limit_yn, quantity_limit_amount, quantity_limit_days,
           prior_authorization_yn, step_therapy_yn
    FROM basic_drugs_formulary
    WHERE 1=1
    """
    params = []
    if ndc is not None:
        query += " AND ndc = %s"
        params.append(ndc)
    if rxcui is not None:
        query += " AND rxcui = %s"
        params.append(rxcui)

    query += " LIMIT %s"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()

    results = []
    for row in rows:
        result = {
            "formulary_id": row[0],
            "formulary_version": row[1],
            "contract_year": row[2],
            "rxcui": row[3],
            "ndc": row[4],
            "tier_level_value": row[5],
            "quantity_limit_yn": row[6],
            "quantity_limit_amount": float(row[7]) if row[7] is not None else None,
            "quantity_limit_days": row[8],
            "prior_authorization_yn": row[9],
            "step_therapy_yn": row[10]
        }
        results.append(result)

    return {"data": results}
