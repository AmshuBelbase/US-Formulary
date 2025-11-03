# uvicorn main:app --reload

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


# Parameters : ?pa=N&st=N&ql=Y&plan_id=001&contract_id=H0034&tier=3&year=2023&limit=10&offset=0&startYear=2022&endYear=2023&drug=naproxen&level=State&region=California&limit=5&rxcui=617314&ndc=00002143380&limitb=5&limitp=1&plan_name=Health%20Plan%20-%20MyCare%20Ohio%20%28Medicare-Medicaid%20Plan%29&contract_name=BUCKEYE%20COMMUNITY%20HEALTH%20PLAN%2C%20INC.


# http://127.0.0.1:8000/api/health
@app.get("/api/health")
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

# http://127.0.0.1:8000/api/trends?year=2023&limit=100&offset=0
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
            SELECT year, brnd_name, gnrc_name,
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
            "brnd_name": r["brnd_name"],
            "gnrc_name": r["gnrc_name"],
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

# http://127.0.0.1:8000/api/pbg/search?drug=naproxen&startYear=2022&endYear=2023
@app.get("/api/pbg/search")
async def search_drugs(
    request: Request,
    drug: str = Query(..., min_length=1),
    startYear: Optional[int] = Query(None),
    endYear: Optional[int] = Query(None),
):
    if not drug:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Missing required parameter: drug"},
        )
    else:
        print(drug)
    
    pool = request.app.state.pool

    where_clauses = []
    params = []
    param_index = 1

    pattern = f"%{drug}%"
    where_clauses.append(f"(brnd_name ILIKE ${param_index} OR gnrc_name ILIKE ${param_index})")
    params.append(pattern)
    param_index += 1

    if startYear is not None:
        where_clauses.append(f"year >= ${param_index}")
        params.append(startYear)
        param_index += 1

    if endYear is not None:
        where_clauses.append(f"year <= ${param_index}")
        params.append(endYear)
        param_index += 1

    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT year,
               brnd_name, gnrc_name,
               tot_prscrbrs,
               tot_clms,
               tot_30day_fills,
               tot_drug_cst,
               tot_benes,
               prscrbr_geo_lvl,
               prscrbr_geo_cd,
               prscrbr_geo_desc
        FROM prescribers_by_geography_drug
        WHERE {where_sql}
        ORDER BY year DESC, tot_clms DESC
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        data = [
            {
                "brnd_name": r["brnd_name"],
                "gnrc_name": r["gnrc_name"],
                "year": r["year"],
                "totalPrescribers": float(r["tot_prscrbrs"]) if r["tot_prscrbrs"] is not None else None,
                "totalClaims": float(r["tot_clms"]) if r["tot_clms"] is not None else None,
                "total30DayFills": float(r["tot_30day_fills"]) if r["tot_30day_fills"] is not None else None,
                "totalDrugCost": float(r["tot_drug_cst"]) if r["tot_drug_cst"] is not None else None,
                "totalBeneficiaries": float(r["tot_benes"]) if r["tot_benes"] is not None else None,
                "prscrbr_geo_lvl": r["prscrbr_geo_lvl"],
                "prscrbr_geo_cd": r["prscrbr_geo_cd"],
                "prscrbr_geo_desc": r["prscrbr_geo_desc"],
            }
            for r in rows
        ]


        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"data": data, "count": len(data)},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Database error while searching by drug", "details": str(e)},
        )

# http://127.0.0.1:8000/api/years
@app.get("/api/years")
async def get_years(request: Request):
    pool = request.app.state.pool

    query = """
        SELECT DISTINCT year
        FROM prescribers_by_geography_drug
        ORDER BY year ASC
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)

        years = [r["year"] for r in rows]

        # Return a simple JSON array of years
        return JSONResponse(status_code=200, content=years)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Database error while listing years", "details": str(e)},
        )

# http://127.0.0.1:8000/api/national_totals
@app.get("/api/national_totals")
async def get_national_totals(request: Request):
    pool = request.app.state.pool

    query = """
        SELECT year,
               SUM(tot_prscrbrs) AS total_prescribers,
               SUM(tot_clms) AS total_claims,
               SUM(tot_30day_fills) AS total_30day_fills,
               SUM(tot_drug_cst) AS total_drug_cost,
               SUM(tot_benes) AS total_beneficiaries
        FROM prescribers_by_geography_drug
        GROUP BY year
        ORDER BY year ASC
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)

        data = [
            {
                "year": r["year"],
                "totalPrescribers": float(r["total_prescribers"]) if r["total_prescribers"] is not None else None,
                "totalClaims": float(r["total_claims"]) if r["total_claims"] is not None else None,
                "total30DayFills": float(r["total_30day_fills"]) if r["total_30day_fills"] is not None else None,
                "totalDrugCost": float(r["total_drug_cost"]) if r["total_drug_cost"] is not None else None,
                "totalBeneficiaries": float(r["total_beneficiaries"]) if r["total_beneficiaries"] is not None else None
            }
            for r in rows
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"data": data, "count": len(data)},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Database error while fetching national totals", "details": str(e)},
        )

# http://127.0.0.1:8000/api/geo_detail?year=2023&drug=naproxen
@app.get("/api/geo_detail")
async def get_geo_detail(
    request: Request,
    year: int = Query(...),
    drug: Optional[str] = Query(None),
):
    pool = request.app.state.pool

    print(f"Year: {year}, Drug: {drug}")

    # Validate year is integer - done by FastAPI Query type

    where_clauses = ["year = $1"]
    params = [year]
    param_index = 2

    if drug:
        where_clauses.append(f"(brnd_name ILIKE ${param_index} OR gnrc_name ILIKE ${param_index})")
        params.append(f"%{drug}%")
        param_index += 1

    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT year,
               COALESCE(brnd_name, gnrc_name) AS drug_name,
               tot_prscrbrs,
               tot_clms,
               tot_30day_fills,
               tot_drug_cst,
               tot_benes,
               prscrbr_geo_lvl,
               prscrbr_geo_cd,
               prscrbr_geo_desc
        FROM prescribers_by_geography_drug
        WHERE {where_sql}
        ORDER BY prscrbr_geo_lvl ASC, prscrbr_geo_cd ASC, tot_clms DESC
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        data = [
            {
                "drugName": r["drug_name"],
                "year": r["year"],
                "totalPrescribers": float(r["tot_prscrbrs"]) if r["tot_prscrbrs"] is not None else None,
                "totalClaims": float(r["tot_clms"]) if r["tot_clms"] is not None else None,
                "total30DayFills": float(r["tot_30day_fills"]) if r["tot_30day_fills"] is not None else None,
                "totalDrugCost": float(r["tot_drug_cst"]) if r["tot_drug_cst"] is not None else None,
                "totalBeneficiaries": float(r["tot_benes"]) if r["tot_benes"] is not None else None,
                "prscrbr_geo_lvl": r["prscrbr_geo_lvl"],
                "prscrbr_geo_cd": r["prscrbr_geo_cd"],
                "prscrbr_geo_desc": r["prscrbr_geo_desc"],
            }
            for r in rows
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"data": data, "count": len(data)},
        )

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Database error while fetching geographic detail", "details": str(e)},
        )

# http://127.0.0.1:8000/api/region_detail?level=State&region=California&year=2023&limit=5
@app.get("/api/region_detail")
async def get_region_detail(
    request: Request,
    level: str = Query(..., min_length=1),
    region: str = Query(..., min_length=1),
    year: Optional[int] = Query(None),
    limit: int = Query(100, gt=0),
    offset: int = Query(0, ge=0),
):
    pool = request.app.state.pool

    # Validate level and region parameters are enforced by FastAPI Query parameters


    where_clauses = []
    params = []
    param_index = 1

    where_clauses.append(f"prscrbr_geo_lvl = ${param_index}")
    params.append(level)
    param_index += 1

    where_clauses.append(f"prscrbr_geo_desc = ${param_index}")
    params.append(region)
    param_index += 1

    if year is not None:
        where_clauses.append(f"year = ${param_index}")
        params.append(year)
        param_index += 1

    where_sql = " AND ".join(where_clauses)

    # Add pagination params
    params.append(limit)
    params.append(offset)

    query = f"""
        SELECT year,
               COALESCE(brnd_name, gnrc_name) AS drug_name,
               tot_prscrbrs,
               tot_clms,
               tot_30day_fills,
               tot_drug_cst,
               tot_benes,
               prscrbr_geo_lvl,
               prscrbr_geo_cd,
               prscrbr_geo_desc
        FROM prescribers_by_geography_drug
        WHERE {where_sql}
        ORDER BY tot_clms DESC
        LIMIT ${param_index} OFFSET ${param_index + 1}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        data = [
            {
                "drugName": r["drug_name"],
                "year": r["year"],
                "totalPrescribers": float(r["tot_prscrbrs"]) if r["tot_prscrbrs"] is not None else None,
                "totalClaims": float(r["tot_clms"]) if r["tot_clms"] is not None else None,
                "total30DayFills": float(r["tot_30day_fills"]) if r["tot_30day_fills"] is not None else None,
                "totalDrugCost": float(r["tot_drug_cst"]) if r["tot_drug_cst"] is not None else None,
                "totalBeneficiaries": float(r["tot_benes"]) if r["tot_benes"] is not None else None,
                "prscrbr_geo_lvl": r["prscrbr_geo_lvl"],
                "prscrbr_geo_cd": r["prscrbr_geo_cd"],
                "prscrbr_geo_desc": r["prscrbr_geo_desc"],
            }
            for r in rows
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"data": data, "limit": limit, "offset": offset, "count": len(data)},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Database error while fetching region detail", "details": str(e)},
        )

# http://127.0.0.1:8000/api/bdf_pi/search?ndc=58151015577&rxcui=617314&plan_id=001&contract_id=H0034
@app.get("/api/bdf_pi/search")
async def formulary_lookup(
    request: Request,
    rxcui: Optional[int] = Query(None),
    ndc: Optional[str] = Query(None),
    plan_id: Optional[str] = Query(None), # 001
    contract_id: Optional[str] = Query(None), # H0034
):
    pool = request.app.state.pool

    if rxcui is None and ndc is None:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "One of the parameter 'rxcui' or 'ndc' must be given"},
        )
    
    where_clauses = []
    params = []
    param_index = 1

    # Handle drug_id type and validation
    if rxcui is not None:
        try:
            rxcui_val = int(rxcui)
        except ValueError:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Parameter 'rxcui' must be an integer"},
            )
        where_clauses.append(f"bf.rxcui = ${param_index}")
        params.append(rxcui_val)
        param_index += 1
    elif ndc is not None:
        # ndc stored as varchar
        where_clauses.append(f"bf.ndc = ${param_index}")
        params.append(ndc)
        param_index += 1

    # Filter by plan_id or formulary_id in plan_info

    if plan_id is not None and plan_id.strip() != "":
        where_clauses.append(f"(pi.plan_id = ${param_index} OR pi.formulary_id = ${param_index})")
        params.append(plan_id)
        param_index += 1

    # Optional contract filter
    if contract_id is not None and contract_id.strip() != "":
        where_clauses.append(f"pi.contract_id = ${param_index}")
        params.append(contract_id)
        param_index += 1

    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT
            bf.formulary_id,
            bf.formulary_version, 
            bf.contract_year,
            bf.rxcui,
            bf.ndc,
            bf.tier_level_value,
            bf.quantity_limit_yn,
            bf.quantity_limit_amount,
            bf.quantity_limit_days,
            bf.prior_authorization_yn,
            bf.step_therapy_yn,
            pi.plan_id AS plan_id,
            pi.contract_id AS contract_id,
            pi.plan_name AS plan_name,
            pi.segment_id AS segment_id,
            pi.contract_name AS contract_name
        FROM basic_drugs_formulary bf
        INNER JOIN plan_info pi ON pi.formulary_id = bf.formulary_id
        WHERE {where_sql}
        ORDER BY bf.tier_level_value ASC, bf.ndc ASC
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        if not rows:
            # 404 Not Found with empty data per spec
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"data": [], "count": 0},
            )

        data = [
            {
                "formularyId": r["formulary_id"],
                "formularyVersion": r["formulary_version"],
                "contractYear": r["contract_year"],
                "rxcui": r["rxcui"],
                "ndc": r["ndc"],
                "tierLevel": r["tier_level_value"],
                "quantityLimit": r["quantity_limit_yn"],
                "quantityLimitAmount": float(r["quantity_limit_amount"]) if r["quantity_limit_amount"] is not None else None,
                "quantityLimitDays": r["quantity_limit_days"],
                "paRequired": r["prior_authorization_yn"],
                "stepTherapyRequired": r["step_therapy_yn"],
                "planId": r["plan_id"],
                "contractId": r["contract_id"],
                "segmentId": r["segment_id"],
                "planName": r["plan_name"],
                "contractName": r["contract_name"],
                "coveredStatus": "Covered",
            }
            for r in rows
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"count": len(data), "data": data},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Database error while performing formulary lookup", "details": str(e)},
        )

# http://127.0.0.1:8000/api/bdf/search?rxcui=617314&ndc=58151015577&pa=N&st=Y&ql=Y&tier=1&limit=10&offset=0
@app.get("/api/bdf/search")
async def formulary_search(
    request: Request,
    rxcui: Optional[int] = Query(None),
    ndc: Optional[str] = Query(None),
    tier: Optional[int] = Query(None),
    pa: Optional[str] = Query(None),
    st: Optional[str] = Query(None),
    ql: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_dir: Optional[str] = Query(None),
    limit: int = Query(100, gt=0),
    offset: int = Query(0, ge=0),
):
    pool = request.app.state.pool

    # Validate that at least one filter is provided
    if all(param is None for param in [rxcui, ndc, tier, pa, st, ql]):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Provide at least one filter: rxcui, ndc, tier, pa, st, or ql"},
        )

    # Validate flag parameters if provided
    valid_flag = lambda v: v in ('Y', 'N')
    if pa and not valid_flag(pa.upper()):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Parameter 'pa' must be 'Y' or 'N'"},
        )
    if st and not valid_flag(st.upper()):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Parameter 'st' must be 'Y' or 'N'"},
        )
    if ql and not valid_flag(ql.upper()):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Parameter 'ql' must be 'Y' or 'N'"},
        )

    # Map sort options
    sort_column_map = {
        "formularyId": "bf.formulary_id",
        "tierLevel": "bf.tier_level_value",
        "paRequired": "bf.prior_authorization_yn",
        "stepTherapyRequired": "bf.step_therapy_yn",
        "quantityLimit": "bf.quantity_limit_yn",
    }
    sort_by_column = sort_column_map.get(sort_by, "bf.tier_level_value")
    sort_direction = "DESC" if (sort_dir and sort_dir.upper() == "DESC") else "ASC"

    # Build WHERE clauses dynamically
    where_clauses = []
    params = []
    param_idx = 1
    if rxcui is not None:
        where_clauses.append(f"bf.rxcui = ${param_idx}")
        params.append(rxcui)
        param_idx += 1
    if ndc:
        where_clauses.append(f"bf.ndc = ${param_idx}")
        params.append(ndc)
        param_idx += 1
    if tier is not None:
        where_clauses.append(f"bf.tier_level_value = ${param_idx}")
        params.append(tier)
        param_idx += 1
    if pa:
        where_clauses.append(f"bf.prior_authorization_yn = ${param_idx}")
        params.append(pa.upper())
        param_idx += 1
    if st:
        where_clauses.append(f"bf.step_therapy_yn = ${param_idx}")
        params.append(st.upper())
        param_idx += 1
    if ql:
        where_clauses.append(f"bf.quantity_limit_yn = ${param_idx}")
        params.append(ql.upper())
        param_idx += 1

    where_sql = " AND ".join(where_clauses)
    
    params.append(limit)
    params.append(offset)


    query = f"""
        SELECT *
        FROM basic_drugs_formulary bf
        WHERE {where_sql}
        ORDER BY {sort_by_column} {sort_direction}, bf.ndc ASC
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        data = [
            {
                "formularyId": r["formulary_id"],
                "formularyVersion": r["formulary_version"],
                "contractYear": r["contract_year"],
                "rxcui": r["rxcui"],
                "ndc": r["ndc"],
                "tierLevel": r["tier_level_value"],
                "paRequired": r["prior_authorization_yn"],
                "stepTherapyRequired": r["step_therapy_yn"],
                "quantityLimit": r["quantity_limit_yn"],
                "quantityLimitAmount": float(r["quantity_limit_amount"]) if r["quantity_limit_amount"] is not None else None,
                "quantityLimitDays": r["quantity_limit_days"],
            }
            for r in rows
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"limit": limit, "offset": offset, "count": len(data), "data": data},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Database error while searching formulary", "details": str(e)},
        )

from decimal import Decimal

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

# http://127.0.0.1:8000/api/drug_profit_analysis?offset=0&rxcui=617314&limit=2
# http://127.0.0.1:8000/api/drug_profit_analysis?offset=0&rxcui=617314&limitb=5&limitp=1&plan_name=Health%20Plan%20-%20MyCare%20Ohio%20%28Medicare-Medicaid%20Plan%29&contract_name=BUCKEYE%20COMMUNITY%20HEALTH%20PLAN%2C%20INC.

@app.get("/api/drug_profit_analysis")
async def drug_profit_analysis(
    request: Request,
    rxcui: Optional[int] = Query(None),
    ndc: Optional[str] = Query(None),
    limitb: Optional[int] = Query(5, gt=0),
    limitp: Optional[int] = Query(5, gt=0),
    offset: Optional[int] = Query(0, ge=0),
    plan_name: Optional[str] = Query(None),
    contract_name: Optional[str] = Query(None),
    tier: Optional[int] = Query(None),
):
    # Validate that either rxcui or ndc is provided
    if rxcui is None and ndc is None: 
        return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Please provide either 'rxcui' or 'ndc' parameter"},
            )
    
    pool = request.app.state.pool

    try:
        conditions = []
        params = []
        param_index = 1
        
        # Step 1: Fetch formulary records for the drug
        if rxcui is not None: 
            conditions.append(f"rxcui = ${param_index}")
            params.append(rxcui)
            param_index += 1
        else: 
            conditions.append(f"ndc = ${param_index}")
            params.append(ndc)
            param_index += 1

        tier_level_value = tier
        if tier_level_value is not None:
            conditions.append(f"tier_level_value = ${param_index}")
            params.append(tier_level_value)
            param_index += 1

        where_sql = " AND ".join(conditions)
        sql = f"SELECT * FROM basic_drugs_formulary WHERE {where_sql} LIMIT {limitb} OFFSET {offset}"

        formulary_res = await pool.fetch(sql, *params)

        if len(formulary_res) == 0:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "No formulary data found for the provided drug code"},
            )
        
        # formulary_ids = [r["formulary_id"] for r in formulary_res]
        # Additional optional filters for plan info
        plan_filters = []

        # Add plan_name filter if provided
        if plan_name:
            plan_filters.append(f"p.plan_name ILIKE '%' || ${len(plan_filters)+1} || '%'")
        # Add contract_name filter if provided
        if contract_name:
            plan_filters.append(f"p.contract_name ILIKE '%' || ${len(plan_filters)+1} || '%'")

        plan_where_clause = ""
        plan_params = []

        if plan_filters:
            plan_where_clause = " WHERE " + " AND ".join(plan_filters)
            if plan_name:
                plan_params.append(plan_name)
            if contract_name:
                plan_params.append(contract_name)

        plan_sql = f"""
            SELECT DISTINCT p.contract_id, p.plan_id, p.segment_id,
                            p.contract_name, p.plan_name, p.premium, p.deductible,
                            p.ma_region_code, p.pdp_region_code, p.state, p.county_code, p.snp
            FROM plan_info p
            {plan_where_clause}
            LIMIT {limitp} OFFSET {offset}
        """

        plan_res = await pool.fetch(plan_sql, *plan_params)

        # Step 2: For each plan, get beneficiary costs and formulary tiers
        analysis_map = {}

        for plan in plan_res:
            contract_id = plan["contract_id"]
            plan_id = plan["plan_id"]
            segment_id = plan["segment_id"]
            key = f"{contract_id}_{plan_id}_{segment_id}"

            # Fetch beneficiary_cost table data
            cost_sql = """
                SELECT tier, days_supply,
                       cost_min_amt_pref, cost_max_amt_pref,
                       cost_min_amt_nonpref, cost_max_amt_nonpref,
                       cost_min_amt_mail_pref, cost_max_amt_mail_pref,
                       cost_min_amt_mail_nonpref, cost_max_amt_mail_nonpref
                FROM beneficiary_cost
                WHERE contract_id = $1 AND plan_id = $2 AND segment_id = $3 AND tier = ANY($4)
            """
            tier_levels = [r["tier_level_value"] for r in formulary_res]
            costs = await pool.fetch(cost_sql, contract_id, plan_id, segment_id, tier_levels)

            # Fetch formulary tier requirements
            reqs_sql = """
                SELECT tier_level_value, prior_authorization_yn, step_therapy_yn, quantity_limit_yn
                FROM basic_drugs_formulary
                WHERE rxcui = $1 AND tier_level_value = ANY($2)
            """
            reqs = await pool.fetch(reqs_sql, rxcui, tier_levels)

            # Group tier info
            tiers_analysis = []
            for tier in tier_levels:
                tier_costs = [c for c in costs if c["tier"] == tier]
                req = next((r for r in reqs if r["tier_level_value"] == tier), None)

                # Calculate min and max costs ignoring nulls
                min_costs = []
                max_costs = []

                for c in tier_costs:
                    cost_values = [
                        c["cost_min_amt_pref"],
                        c["cost_min_amt_nonpref"],
                        c["cost_min_amt_mail_pref"],
                        c["cost_min_amt_mail_nonpref"],
                    ]
                    filtered_costs = [v for v in cost_values if v is not None]
                    min_costs.extend(filtered_costs)

                    max_costs.extend([
                        c["cost_max_amt_pref"] or 0,
                        c["cost_max_amt_nonpref"] or 0,
                        c["cost_max_amt_mail_pref"] or 0,
                        c["cost_max_amt_mail_nonpref"] or 0,
                    ])
                min_patient_cost = min(min_costs) if min_costs else None
                max_patient_cost = max(max_costs) if max_costs else None

                tiers_analysis.append({
                    "tier": tier,
                    "minPatientCost": min_patient_cost,
                    "maxPatientCost": max_patient_cost,
                    "priorAuthorizationRequired": req["prior_authorization_yn"] == "Y" if req else False,
                    "stepTherapyRequired": req["step_therapy_yn"] == "Y" if req else False,
                    "quantityLimit": req["quantity_limit_yn"] == "Y" if req else False,
                })

            analysis_map[key] = {
                "plan_name": plan["plan_name"],
                "contract_name": plan["contract_name"],
                "tiers": tiers_analysis,
            }

        # Generate the final result
        analysis_results = list(analysis_map.values())

        # Example: filter plans that require step therapy or quantity limit
        improvement_suggestions = [
            plan for plan in analysis_results
            if any(tier["stepTherapyRequired"] or tier["quantityLimit"] for tier in plan["tiers"])
        ]

        return JSONResponse(
            status_code=200,
            content={
                "drugInfo": convert_decimals([dict(r) for r in formulary_res]),
                "planAnalysis": convert_decimals(analysis_results),
                "improvementSuggestions": convert_decimals(improvement_suggestions)
            }
        )

    except Exception as e:
        return HTTPException(status_code=500, detail=f"Error fetching profit analysis data: {str(e)}")


# REMAINING SHIFT : /api/drug_full_data
