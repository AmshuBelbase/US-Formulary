from dotenv import load_dotenv
import os
from fastapi import FastAPI, Query, HTTPException, status, Request
from typing import Optional
from fastapi.responses import JSONResponse 
import asyncpg  # asynchronous Postgres client 
from contextlib import asynccontextmanager
from datetime import datetime, time
import pytz
import sys
import logging
from fastapi.encoders import jsonable_encoder 

def is_outside_server_time(): 
    ist = pytz.timezone('Asia/Kolkata') 
    now = datetime.now(ist).time()
    start = time(0, 0)  # 12:00 AM
    end = time(6, 0)    # 6:00 AM 
    return start <= now < end

if is_outside_server_time():
    logging.critical("Server operations are restricted to 12:00 AM - 6:00 AM IST. Terminating.")
    sys.exit("Fatal Error: Operation attempted outside allowed time window.") 
    
# Load environment variables from .env file
load_dotenv()

# Define a global variable for pool
pool: asyncpg.Pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting DB")
    app.state.pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        min_size=1,
        max_size=10,
    )
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)

# Assume a function to parse pagination parameters
def parse_pagination(query_params):
    try:
        limit = int(query_params.get('limit', 100))
        offset = int(query_params.get('offset', 0))
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if offset < 0:
            raise ValueError("Offset cannot be negative")
        return limit, offset
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid pagination parameters')

@app.get("/health")
async def health_check(request: Request):
    pool = request.app.state.pool
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return JSONResponse(content={"status": "ok"})
    except Exception as err:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "Database unavailable",
                "details": str(err)
            }
        )

@app.get("/api/drugdetails")
async def get_drug_details(
    request: Request,
    ndc: Optional[int] = Query(None),
    rxcui: Optional[int] = Query(None),
    limit: int = Query(10, gt=0)
):
    if ndc is None and rxcui is None:
        raise HTTPException(status_code=400, detail="Either 'ndc' or 'rxcui' must be provided")

    pool = request.app.state.pool

    query = """
        SELECT formulary_id, formulary_version, contract_year, rxcui, ndc, tier_level_value,
               quantity_limit_yn, quantity_limit_amount, quantity_limit_days,
               prior_authorization_yn, step_therapy_yn
        FROM basic_drugs_formulary
        WHERE 1=1
    """

    params = []
    conditions = []
    param_index = 1

    if ndc is not None:
        conditions.append(f"ndc = ${param_index}")
        params.append(ndc)
        param_index += 1
    if rxcui is not None:
        conditions.append(f"rxcui = ${param_index}")
        params.append(rxcui)
        param_index += 1

    if conditions:
        query += " AND " + " AND ".join(conditions)

    # Add limit param
    query += f" LIMIT ${param_index}"
    params.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    results = []
    for row in rows:
        results.append({
            "formulary_id": row["formulary_id"],
            "formulary_version": row["formulary_version"],
            "contract_year": row["contract_year"],
            "rxcui": row["rxcui"],
            "ndc": row["ndc"],
            "tier_level_value": row["tier_level_value"],
            "quantity_limit_yn": row["quantity_limit_yn"],
            "quantity_limit_amount": float(row["quantity_limit_amount"]) if row["quantity_limit_amount"] is not None else None,
            "quantity_limit_days": row["quantity_limit_days"],
            "prior_authorization_yn": row["prior_authorization_yn"],
            "step_therapy_yn": row["step_therapy_yn"]
        })

    return {"data": results}

@app.get("/api/trends") 
async def get_trends(
    request: Request,
    year: int = Query(...), 
    limit: int = Query(100, gt=0), 
    offset: int = Query(0, ge=0)
):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT year, COALESCE(brnd_name, gnrc_name) AS drug_name,
                   tot_prscrbrs, tot_clms, tot_30day_fills, tot_drug_cst, tot_benes
            FROM prescribers_by_geography_drug
            WHERE year = $1
            ORDER BY tot_clms DESC
            LIMIT $2 OFFSET $3
            """,
            year, limit, offset
        )
    data = [
        {
            "drugName": r["drug_name"],
            "year": r["year"],
            "totalPrescribers": float(r["tot_prscrbrs"]),
            "totalClaims": float(r["tot_clms"]),
            "total30DayFills": float(r["tot_30day_fills"]),
            "totalDrugCost": float(r["tot_drug_cst"]),
            "totalBeneficiaries": float(r["tot_benes"]),
        }
        for r in rows
    ]
    response_content = {
        "metadata": {
            "year": year,
            "limit": limit,
            "offset": offset,
            "count": len(data),
        },
        "data": data,
    }
    json_compatible_content = jsonable_encoder(response_content)
    return JSONResponse(content=json_compatible_content)