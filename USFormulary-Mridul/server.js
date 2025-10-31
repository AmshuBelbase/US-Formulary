'use strict';

const express = require('express');
const { Pool } = require('pg');
// const fetch = require('node-fetch');

const app = express();

const DB_CONFIG = {
  host: process.env.PGHOST || 'srmist.cjwqmm4a21xa.ap-south-1.rds.amazonaws.com',
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER || 'srmist_fl',
  password: process.env.PGPASSWORD || 'w8Qn3bZ2vR1xLt0p',
  database: process.env.PGDATABASE || 'srmist_fl_db',
  max: Number(process.env.PGPOOL_MAX || 10),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
  connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10000),
  ssl: process.env.PGSSLMODE === 'disable' ? undefined : { rejectUnauthorized: false },
};

const pool = new Pool(DB_CONFIG);

function successResponse(res, data, statusCode = 200, extra) {
  const payload = { success: true, data, ...(extra || {}) };
  return res.status(statusCode).json(payload);
}

function errorResponse(res, message, statusCode = 500, details) {
  const payload = { success: false, error: message };
  if (details) payload.details = details;
  return res.status(statusCode).json(payload);
}

// Health endpoint
app.get('/health', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT 1');
    return successResponse(res, { status: 'ok' });
  } catch (err) {
    return errorResponse(res, 'Database unavailable', 503, String(err));
  }
});

// 1) Primary Trends Endpoint (Required Year Filter)
app.get('/api/trends', async (req, res) => {
  const { year: yearParam } = req.query;

  if (yearParam == null) {
    return errorResponse(res, 'Missing required parameter: year', 400);
  }

  const year = Number(yearParam);
  if (!Number.isInteger(year)) {
    return errorResponse(res, "Parameter 'year' must be an integer", 400);
  }

  let limit, offset;
  try {
    ({ limit, offset } = parsePagination(req.query));
  } catch (e) {
    const status = e && e.statusCode ? e.statusCode : 400;
    return errorResponse(res, e.message || 'Invalid pagination parameters', status);
  }

  const sql = `
    SELECT year,
           COALESCE(brnd_name, gnrc_name) AS drug_name,
           tot_prscrbrs,
           tot_clms,
           tot_30day_fills,
           tot_drug_cst,
           tot_benes
    FROM prescribers_by_geography_drug
    WHERE year = $1
    ORDER BY tot_clms DESC
    LIMIT $2 OFFSET $3
  `;

  try {
    const { rows } = await pool.query(sql, [year, limit, offset]);
    const data = rows.map((r) => ({
      drugName: r.drug_name,
      year: r.year,
      totalPrescribers: r.tot_prscrbrs,
      totalClaims: r.tot_clms,
      total30DayFills: r.tot_30day_fills,
      totalDrugCost: r.tot_drug_cst,
      totalBeneficiaries: r.tot_benes,
    }));
    return successResponse(res, data, 200, { limit, offset, count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while fetching trends', 500, String(err));
  }
});

// 2) Search by Drug Name (Across All Years)
app.get('/api/search', async (req, res) => {
  const { drug } = req.query;
  if (!drug) {
    return errorResponse(res, 'Missing required parameter: drug', 400);
  }

  // Validate optional year range
  const { startYear: startYearRaw, endYear: endYearRaw } = req.query;
  let startYear, endYear;
  if (startYearRaw != null && startYearRaw !== '') {
    startYear = Number(startYearRaw);
    if (!Number.isInteger(startYear)) {
      return errorResponse(res, "Parameter 'startYear' must be an integer", 400);
    }
  }
  if (endYearRaw != null && endYearRaw !== '') {
    endYear = Number(endYearRaw);
    if (!Number.isInteger(endYear)) {
      return errorResponse(res, "Parameter 'endYear' must be an integer", 400);
    }
  }

  // Dynamic WHERE clauses with parameter counter
  const whereClauses = [];
  const params = [];
  let p = 1;
  const pattern = `%${drug}%`;
  whereClauses.push(`(brnd_name ILIKE $${p} OR gnrc_name ILIKE $${p})`);
  params.push(pattern);
  p += 1;
  if (startYear != null) {
    whereClauses.push(`year >= $${p}`);
    params.push(startYear);
    p += 1;
  }
  if (endYear != null) {
    whereClauses.push(`year <= $${p}`);
    params.push(endYear);
    p += 1;
  }

  const sql = `
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
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY year DESC, tot_clms DESC
  `;

  try {
    const { rows } = await pool.query(sql, params);
    const data = rows.map((r) => ({
      drugName: r.drug_name,
      year: r.year,
      totalPrescribers: r.tot_prscrbrs,
      totalClaims: r.tot_clms,
      total30DayFills: r.tot_30day_fills,
      totalDrugCost: r.tot_drug_cst,
      totalBeneficiaries: r.tot_benes,
      prscrbr_geo_lvl: r.prscrbr_geo_lvl,
      prscrbr_geo_cd: r.prscrbr_geo_cd,
      prscrbr_geo_desc: r.prscrbr_geo_desc,
    }));
    return successResponse(res, data, 200, { count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while searching by drug', 500, String(err));
  }
});

// 4) National Aggregates (Time Series)
app.get('/api/national_totals', async (req, res) => {
  const sql = `
    SELECT year,
           SUM(tot_prscrbrs) AS total_prescribers,
           SUM(tot_clms) AS total_claims,
           SUM(tot_30day_fills) AS total_30day_fills,
           SUM(tot_drug_cst) AS total_drug_cost,
           SUM(tot_benes) AS total_beneficiaries
    FROM prescribers_by_geography_drug
    GROUP BY year
    ORDER BY year ASC
  `;
  try {
    const { rows } = await pool.query(sql);
    const data = rows.map((r) => ({
      year: r.year,
      totalPrescribers: r.total_prescribers,
      totalClaims: r.total_claims,
      total30DayFills: r.total_30day_fills,
      totalDrugCost: r.total_drug_cost,
      totalBeneficiaries: r.total_beneficiaries,
    }));
    return successResponse(res, data, 200, { count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while fetching national totals', 500, String(err));
  }
});

// 5) Geographic Detail
app.get('/api/geo_detail', async (req, res) => {
  const { year: yearParam, drug } = req.query;
  if (yearParam == null) {
    return errorResponse(res, 'Missing required parameter: year', 400);
  }
  const year = Number(yearParam);
  if (!Number.isInteger(year)) {
    return errorResponse(res, "Parameter 'year' must be an integer", 400);
  }

  // Build dynamic filters: required year, optional drug pattern on brand/generic
  const whereClauses = ['year = $1'];
  const params = [year];
  let p = 2;
  if (drug) {
    whereClauses.push(`(brnd_name ILIKE $${p} OR gnrc_name ILIKE $${p})`);
    params.push(`%${drug}%`);
    p += 1;
  }

  const sql = `
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
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY prscrbr_geo_lvl ASC, prscrbr_geo_cd ASC, tot_clms DESC
  `;

  try {
    const { rows } = await pool.query(sql, params);
    const data = rows.map((r) => ({
      drugName: r.drug_name,
      year: r.year,
      totalPrescribers: r.tot_prscrbrs,
      totalClaims: r.tot_clms,
      total30DayFills: r.tot_30day_fills,
      totalDrugCost: r.tot_drug_cst,
      totalBeneficiaries: r.tot_benes,
      prscrbr_geo_lvl: r.prscrbr_geo_lvl,
      prscrbr_geo_cd: r.prscrbr_geo_cd,
      prscrbr_geo_desc: r.prscrbr_geo_desc,
    }));
    return successResponse(res, data, 200, { count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while fetching geographic detail', 500, String(err));
  }
});

// 6) Region detail by geographic level and description
app.get('/api/region_detail', async (req, res) => {
  const { level, region, year: yearParam } = req.query;

  // Validate required params
  if (!level || typeof level !== 'string' || level.trim() === '') {
    return errorResponse(res, "Missing or invalid required parameter: level", 400);
  }
  if (!region || typeof region !== 'string' || region.trim() === '') {
    return errorResponse(res, "Missing or invalid required parameter: region", 400);
  }

  // Validate optional year
  let year;
  if (yearParam != null && yearParam !== '') {
    year = Number(yearParam);
    if (!Number.isInteger(year)) {
      return errorResponse(res, "Parameter 'year' must be an integer", 400);
    }
  }

  // Pagination
  let limit, offset;
  try {
    ({ limit, offset } = parsePagination(req.query));
  } catch (e) {
    const status = e && e.statusCode ? e.statusCode : 400;
    return errorResponse(res, e.message || 'Invalid pagination parameters', status);
  }

  // Dynamic WHERE clauses with parameter counter
  const whereClauses = [];
  const params = [];
  let p = 1;

  whereClauses.push(`prscrbr_geo_lvl = $${p}`); params.push(level); p += 1;
  whereClauses.push(`prscrbr_geo_desc = $${p}`); params.push(region); p += 1;

  if (year != null) {
    whereClauses.push(`year = $${p}`);
    params.push(year);
    p += 1;
  }

  // Add pagination params
  params.push(limit, offset);

  const sql = `
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
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY tot_clms DESC
    LIMIT $${p} OFFSET $${p + 1}
  `;

  try {
    const { rows } = await pool.query(sql, params);
    const data = rows.map((r) => ({
      drugName: r.drug_name,
      year: r.year,
      totalPrescribers: r.tot_prscrbrs,
      totalClaims: r.tot_clms,
      total30DayFills: r.tot_30day_fills,
      totalDrugCost: r.tot_drug_cst,
      totalBeneficiaries: r.tot_benes,
      prscrbr_geo_lvl: r.prscrbr_geo_lvl,
      prscrbr_geo_cd: r.prscrbr_geo_cd,
      prscrbr_geo_desc: r.prscrbr_geo_desc,
    }));
    return successResponse(res, data, 200, { limit, offset, count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while fetching region detail', 500, String(err));
  }
});

// 7) Formulary lookup by drug identifier and plan context
app.get('/api/formulary/lookup', async (req, res) => {
  const { drug_id: drugId, id_type: idTypeRaw, plan_id: planId, contract_id: contractId } = req.query;

  // Validate required parameters
  if (!drugId || typeof drugId !== 'string' || drugId.trim() === '') {
    return errorResponse(res, "Missing or invalid required parameter: drug_id", 400);
  }
  if (!idTypeRaw || typeof idTypeRaw !== 'string') {
    return errorResponse(res, "Missing or invalid required parameter: id_type", 400);
  }
  const idType = idTypeRaw.toLowerCase();
  if (idType !== 'rxcui' && idType !== 'ndc') {
    return errorResponse(res, "Parameter 'id_type' must be either 'rxcui' or 'ndc'", 400);
  }
  if (!planId || typeof planId !== 'string' || planId.trim() === '') {
    return errorResponse(res, "Missing or invalid required parameter: plan_id", 400);
  }

  // Build dynamic filters with parameterized query
  const whereClauses = [];
  const params = [];
  let p = 1;

  // Join to plan_info for plan_name and plan identifiers
  // Drug identifier filter (type-based)
  if (idType === 'rxcui') {
    const rxcuiVal = Number(drugId);
    if (!Number.isInteger(rxcuiVal)) {
      return errorResponse(res, "Parameter 'drug_id' must be an integer when id_type='rxcui'", 400);
    }
    whereClauses.push(`bf.rxcui = $${p}`);
    params.push(rxcuiVal);
    p += 1;
  } else {
    // ndc is stored as varchar(20)
    whereClauses.push(`bf.ndc = $${p}`);
    params.push(String(drugId));
    p += 1;
  }

  // Required plan context: allow matching by plan_id OR formulary_id (some callers may pass formulary_id)
  whereClauses.push(`(pi.plan_id = $${p} OR pi.formulary_id = $${p})`);
  params.push(planId);
  p += 1;

  // Optional contract filter
  if (contractId && String(contractId).trim() !== '') {
    whereClauses.push(`pi.contract_id = $${p}`);
    params.push(contractId);
    p += 1;
  }

  const sql = `
    SELECT
      bf.formulary_id,
      bf.rxcui,
      bf.ndc,
      pi.plan_id AS plan_id,
      pi.contract_id AS contract_id,
      pi.plan_name AS plan_name,
      bf.tier_level_value,
      bf.prior_authorization_yn,
      bf.step_therapy_yn,
      bf.quantity_limit_yn
    FROM basic_drugs_formulary bf
    INNER JOIN plan_info pi ON pi.formulary_id = bf.formulary_id
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY bf.tier_level_value ASC, bf.ndc ASC
  `;

  try {
    const { rows } = await pool.query(sql, params);
    if (!rows || rows.length === 0) {
      // 404 with empty array per spec intent
      return successResponse(res, [], 404, { count: 0 });
    }
    const data = rows.map((r) => ({
      formularyId: r.formulary_id,
      rxcui: r.rxcui,
      ndc: r.ndc,
      planId: r.plan_id,
      contractId: r.contract_id,
      planName: r.plan_name,
      tierLevel: r.tier_level_value,
      paRequired: r.prior_authorization_yn,
      stepTherapyRequired: r.step_therapy_yn,
      quantityLimit: r.quantity_limit_yn,
      coveredStatus: 'Covered',
    }));
    return successResponse(res, data, 200, { count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while performing formulary lookup', 500, String(err));
  }
});

// 8) Formulary search with dynamic filters (rxcui, ndc, tier, flags)
app.get('/api/formulary/search', async (req, res) => {
  const { rxcui: rxcuiRaw, ndc, tier: tierRaw, pa: paRaw, st: stRaw, ql: qlRaw, sort_by: sortByRaw, sort_dir: sortDirRaw } = req.query;

  // Validate optional numeric filters
  let rxcui, tier;
  if (rxcuiRaw != null && rxcuiRaw !== '') {
    rxcui = Number(rxcuiRaw);
    if (!Number.isInteger(rxcui)) {
      return errorResponse(res, "Parameter 'rxcui' must be an integer", 400);
    }
  }
  if (tierRaw != null && tierRaw !== '') {
    tier = Number(tierRaw);
    if (!Number.isInteger(tier)) {
      return errorResponse(res, "Parameter 'tier' must be an integer", 400);
    }
  }

  // Validate optional flag filters (expect 'Y' or 'N')
  const validFlag = (v) => v === 'Y' || v === 'N';
  const pa = paRaw != null && paRaw !== '' ? String(paRaw).toUpperCase() : undefined;
  const st = stRaw != null && stRaw !== '' ? String(stRaw).toUpperCase() : undefined;
  const ql = qlRaw != null && qlRaw !== '' ? String(qlRaw).toUpperCase() : undefined;
  if (pa && !validFlag(pa)) return errorResponse(res, "Parameter 'pa' must be 'Y' or 'N'", 400);
  if (st && !validFlag(st)) return errorResponse(res, "Parameter 'st' must be 'Y' or 'N'", 400);
  if (ql && !validFlag(ql)) return errorResponse(res, "Parameter 'ql' must be 'Y' or 'N'", 400);

  // Pagination
  let limit, offset;
  try {
    ({ limit, offset } = parsePagination(req.query));
  } catch (e) {
    const status = e && e.statusCode ? e.statusCode : 400;
    return errorResponse(res, e.message || 'Invalid pagination parameters', status);
  }

  // Dynamic WHERE clauses
  const whereClauses = [];
  const params = [];
  let p = 1;
  if (rxcui != null) { whereClauses.push(`bf.rxcui = $${p}`); params.push(rxcui); p += 1; }
  if (ndc != null && ndc !== '') { whereClauses.push(`bf.ndc = $${p}`); params.push(String(ndc)); p += 1; }
  if (tier != null) { whereClauses.push(`bf.tier_level_value = $${p}`); params.push(tier); p += 1; }
  if (pa) { whereClauses.push(`bf.prior_authorization_yn = $${p}`); params.push(pa); p += 1; }
  if (st) { whereClauses.push(`bf.step_therapy_yn = $${p}`); params.push(st); p += 1; }
  if (ql) { whereClauses.push(`bf.quantity_limit_yn = $${p}`); params.push(ql); p += 1; }

  // If no filters provided, prevent full table scan: require at least one filter
  if (whereClauses.length === 0) {
    return errorResponse(res, 'Provide at least one filter: rxcui, ndc, tier, pa, st, or ql', 400);
  }

  // Sorting (whitelist)
  const sortColumnMap = {
    formularyId: 'bf.formulary_id',
    tierLevel: 'bf.tier_level_value',
    paRequired: 'bf.prior_authorization_yn',
    stepTherapyRequired: 'bf.step_therapy_yn',
    quantityLimit: 'bf.quantity_limit_yn',
  };
  const sortBy = sortByRaw && sortColumnMap[sortByRaw] ? sortColumnMap[sortByRaw] : 'bf.tier_level_value';
  const sortDir = String(sortDirRaw || 'ASC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC';

  // Add pagination placeholders
  params.push(limit, offset);

  const sql = `
    SELECT
      bf.formulary_id,
      bf.rxcui,
      bf.ndc,
      bf.tier_level_value,
      bf.prior_authorization_yn,
      bf.step_therapy_yn,
      bf.quantity_limit_yn
    FROM basic_drugs_formulary bf
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY ${sortBy} ${sortDir}, bf.ndc ASC
    LIMIT $${p} OFFSET $${p + 1}
  `;

  try {
    const { rows } = await pool.query(sql, params);
    const data = rows.map((r) => ({
      formularyId: r.formulary_id,
      rxcui: r.rxcui,
      ndc: r.ndc,
      tierLevel: r.tier_level_value,
      paRequired: r.prior_authorization_yn,
      stepTherapyRequired: r.step_therapy_yn,
      quantityLimit: r.quantity_limit_yn,
    }));
    return successResponse(res, data, 200, { limit, offset, count: data.length });
  } catch (err) {
    return errorResponse(res, 'Database error while searching formulary', 500, String(err));
  }
});

// 3) List Available Years
app.get('/api/years', async (req, res) => {
  const sql = `
    SELECT DISTINCT year
    FROM prescribers_by_geography_drug
    ORDER BY year ASC
  `;
  try {
    const { rows } = await pool.query(sql);
    const years = rows.map((r) => r.year);
    // For this endpoint, return a simple array
    return res.status(200).json(years);
  } catch (err) {
    return errorResponse(res, 'Database error while listing years', 500, String(err));
  }
});

// AMSHU :

app.get('/api/drug_insights', async (req, res) => {
  const { ndc, rxcui } = req.query;

  if (!ndc && !rxcui) {
    return errorResponse(res, "Please provide either 'ndc' or 'rxcui' query parameter", 400);
  }

  try {
    let sql = '';
    let params = [];
    if (rxcui) {
      sql = `
        SELECT bf.formulary_id, bf.formulary_version, bf.contract_year, bf.tier_level_value, bf.quantity_limit_yn,
               bf.quantity_limit_amount, bf.quantity_limit_days, bf.prior_authorization_yn, bf.step_therapy_yn
        FROM basic_drugs_formulary bf
        WHERE bf.rxcui = $1
      `;
      params = [Number(rxcui)];
    } else if (ndc) {
      sql = `
        SELECT bf.formulary_id, bf.formulary_version, bf.contract_year, bf.tier_level_value, bf.quantity_limit_yn,
               bf.quantity_limit_amount, bf.quantity_limit_days, bf.prior_authorization_yn, bf.step_therapy_yn
        FROM basic_drugs_formulary bf
        WHERE bf.ndc = $1
      `;
      params = [ndc];
    }

    const { rows } = await pool.query(sql, params);

    if (rows.length === 0) {
      return errorResponse(res, 'No formulary data found for the provided drug code', 404);
    }

    // Return key formulary details as starting insights
    return successResponse(res, rows);
  } catch (err) {
    return errorResponse(res, 'Database error while fetching drug formulary data', 500, String(err));
  }
});

app.get('/api/drug_full_data', async (req, res) => {
  const { ndc, rxcui } = req.query;

  if (!ndc && !rxcui) {
    return errorResponse(res, "Please provide either 'ndc' or 'rxcui' parameter", 400);
  }

  try {
    // 1. Fetch formulary records for the drug
    let formularySQL = '';
    let formularyParams = [];
    if (rxcui) {
      formularySQL = `
        SELECT *
        FROM basic_drugs_formulary
        WHERE rxcui = $1
      `;
      formularyParams = [Number(rxcui)];
    } else {
      formularySQL = `
        SELECT *
        FROM basic_drugs_formulary
        WHERE ndc = $1
      `;
      formularyParams = [ndc];
    }

    const formularyRes = await pool.query(formularySQL, formularyParams);
    if (formularyRes.rows.length === 0) {
      return errorResponse(res, 'No formulary data found for the provided drug code', 404);
    }

    // Collect contract_id, plan_id, segment_id info for cost and plan queries
    // Assuming formulary rows have contract_year and formulary_id, but to relate to plan_info, need some join logic or user input
    // For simplicity, get distinct plan_info covering this formulary_id and contract_year (if available)

    // Here we fetch plan_info records that reference this formulary_id
    const formularyIdList = formularyRes.rows.map(r => r.formulary_id);

    const planSQL = `
      SELECT DISTINCT p.contract_id, p.plan_id, p.segment_id, p.contract_name, p.plan_name, p.premium, p.deductible, 
                      p.ma_region_code, p.pdp_region_code, p.state, p.county_code, p.snp
      FROM plan_info p
      WHERE p.formulary_id = ANY($1)
      LIMIT 10
    `;

    const planRes = await pool.query(planSQL, [formularyIdList]);

    // For each plan, fetch corresponding beneficiary_cost and insulin_beneficiary_cost details
    const results = [];

    for (const plan of planRes.rows) {
      const { contract_id, plan_id, segment_id } = plan;
      const tierLevels = [...new Set(formularyRes.rows.map(r => r.tier_level_value))];

      // Fetch beneficiary_cost info for this plan_contract/segment and all tiers
      const costSQL = `
        SELECT *
        FROM beneficiary_cost
        WHERE contract_id = $1 AND plan_id = $2 AND segment_id = $3 AND tier = ANY($4)
      `;
      const costRes = await pool.query(costSQL, [contract_id, plan_id, segment_id, tierLevels]);

      // Fetch insulin_beneficiary_cost info similarly if relevant tiers found
      const insulinCostSQL = `
        SELECT *
        FROM insulin_beneficiary_cost
        WHERE contract_id = $1 AND plan_id = $2 AND segment_id = $3 AND tier = ANY($4)
      `;
      const insulinCostRes = await pool.query(insulinCostSQL, [contract_id, plan_id, segment_id, tierLevels]);

      // Fetch excluded drugs for this plan and these tiers if any
      const excludedSQL = `
        SELECT *
        FROM excluded_drugs_formulary
        WHERE contract_id = $1 AND plan_id = $2 AND tier = ANY($3)
        AND rxcui = ANY($4)
      `;
      const rxcuiList = formularyRes.rows.map(r => r.rxcui);
      const excludedRes = await pool.query(excludedSQL, [contract_id, plan_id, tierLevels, rxcuiList]);

      // Fetch indication based coverage info if any
      const indicationSQL = `
        SELECT *
        FROM indication_based_coverage_formulary
        WHERE contract_id = $1 AND plan_id = $2 AND rxcui = ANY($3)
      `;
      const indicationRes = await pool.query(indicationSQL, [contract_id, plan_id, rxcuiList]);

      results.push({
        plan,
        beneficiaryCost: costRes.rows,
        insulinBeneficiaryCost: insulinCostRes.rows,
        excludedDrugs: excludedRes.rows,
        indicationCoverage: indicationRes.rows,
      });
    }

    // Return comprehensive data for analysis
    return successResponse(res, {
      formulary: formularyRes.rows,
      plansData: results,
    });
  } catch (err) {
    return errorResponse(res, 'Error fetching comprehensive drug data', 500, String(err));
  }
});


app.get('/api/drug_profit_analysis', async (req, res) => {
  const { ndc, rxcui } = req.query;

  if (!ndc && !rxcui) {
    return errorResponse(res, "Please provide either 'ndc' or 'rxcui' parameter", 400);
  }

  try {
    // Step 1: Fetch formulary records for the drug
    let formularySQL = '';
    let formularyParams = [];
    if (rxcui) {
      formularySQL = `
        SELECT *
        FROM basic_drugs_formulary
        WHERE rxcui = $1
      `;
      formularyParams = [Number(rxcui)];
    } else {
      formularySQL = `
        SELECT *
        FROM basic_drugs_formulary
        WHERE ndc = $1
      `;
      formularyParams = [ndc];
    }

    const formularyRes = await pool.query(formularySQL, formularyParams);
    if (formularyRes.rows.length === 0) {
      return errorResponse(res, 'No formulary data found for the provided drug code', 404);
    }

    // Collect distinct plan_info linked to these formulary_ids
    const formularyIdList = formularyRes.rows.map(r => r.formulary_id);

    const planSQL = `
      SELECT DISTINCT p.contract_id, p.plan_id, p.segment_id, p.contract_name, p.plan_name, p.premium, p.deductible,
                      p.ma_region_code, p.pdp_region_code, p.state, p.county_code, p.snp
      FROM plan_info p
      WHERE p.formulary_id = ANY($1)
      LIMIT 50
    `;
    const planRes = await pool.query(planSQL, [formularyIdList]);

    // Prepare distinct tier levels from formulary
    const tierLevels = [...new Set(formularyRes.rows.map(r => r.tier_level_value))];

    // Use a Map to deduplicate plans and group tiers
    const analysisMap = new Map();

    for (const plan of planRes.rows) {
      const { contract_id, plan_id, segment_id } = plan;
      const key = `${contract_id}_${plan_id}_${segment_id}`;

      // Skip if already processed
      if (analysisMap.has(key)) {
        continue;
      }

      // Query beneficiary_cost table using correct cost columns
      const costSQL = `
        SELECT tier, days_supply,
               cost_min_amt_pref, cost_max_amt_pref,
               cost_min_amt_nonpref, cost_max_amt_nonpref,
               cost_min_amt_mail_pref, cost_max_amt_mail_pref,
               cost_min_amt_mail_nonpref, cost_max_amt_mail_nonpref
        FROM beneficiary_cost
        WHERE contract_id = $1 AND plan_id = $2 AND segment_id = $3 AND tier = ANY($4)
      `;
      const costRes = await pool.query(costSQL, [contract_id, plan_id, segment_id, tierLevels]);

      // Get formulary requirements for tiers
      const reqsSQL = `
        SELECT tier_level_value, prior_authorization_yn, step_therapy_yn, quantity_limit_yn
        FROM basic_drugs_formulary
        WHERE rxcui = $1 AND tier_level_value = ANY($2)
      `;
      const reqsRes = await pool.query(reqsSQL, [rxcui ? Number(rxcui) : null, tierLevels]);

      // Group tier analysis info
      const tiersAnalysis = tierLevels.map(tier => {
        // Extract costs for the current tier
        const costs = costRes.rows.filter(c => c.tier === tier);
        // Find formulary reqs for tier
        const reqs = reqsRes.rows.find(r => r.tier_level_value === tier);

        // Calculate min and max patient costs across categories
        const minCosts = costs.map(c =>
          Math.min(
            c.cost_min_amt_pref || Infinity,
            c.cost_min_amt_nonpref || Infinity,
            c.cost_min_amt_mail_pref || Infinity,
            c.cost_min_amt_mail_nonpref || Infinity
          )
        ).filter(c => c !== Infinity);

        const maxCosts = costs.map(c =>
          Math.max(
            c.cost_max_amt_pref || 0,
            c.cost_max_amt_nonpref || 0,
            c.cost_max_amt_mail_pref || 0,
            c.cost_max_amt_mail_nonpref || 0
          )
        );

        return {
          tier,
          minPatientCost: minCosts.length > 0 ? Math.min(...minCosts) : null,
          maxPatientCost: maxCosts.length > 0 ? Math.max(...maxCosts) : null,
          priorAuthorizationRequired: reqs ? reqs.prior_authorization_yn === 'Y' : false,
          stepTherapyRequired: reqs ? reqs.step_therapy_yn === 'Y' : false,
          quantityLimit: reqs ? reqs.quantity_limit_yn === 'Y' : false,
        };
      });

      analysisMap.set(key, {
        plan_name: plan.plan_name,
        contract_name: plan.contract_name,
        tiers: tiersAnalysis,
      });
    }

    const analysisResults = Array.from(analysisMap.values());

    // Filter improvement suggestions based on tier barriers
    const improvementSuggestions = analysisResults.filter(plan =>
      plan.tiers.some(tier =>
        (tier.stepTherapyRequired || tier.quantityLimit) 
        // && tier.minPatientCost !== 0 &&
        // tier.minPatientCost !== null
      )
    );

    return successResponse(res, {
      drugInfo: formularyRes.rows,
      planAnalysis: analysisResults,
      improvementSuggestions,
    });
  } catch (err) {
    return errorResponse(res, 'Error fetching profit analysis data', 500, String(err));
  }
});


app.get('/api/plan_ma', async (req, res) => {
  const { plan_id } = req.query;  // plan_id = 001

  try {
    const result = await pool.query(
      `SELECT DISTINCT ma_region_code 
       FROM plan_info 
       WHERE plan_id = $1 AND ma_region_code IS NOT NULL`,
      [plan_id]
    );

    const maCodes = result.rows.map(row => row.ma_region_code);
    return successResponse(res, maCodes);
  } catch (err) {
    return errorResponse(res, 'Failed to fetch MA region codes', 500, err.message);
  }
});


app.get('/api/drug_costs', async (req, res) => {
  const {ma, plan_id} = req.query; // plan_id = 001, ma = 08,13,09
  if (!plan_id || !ma) return errorResponse(res, 'Missing parameters', 400);
  try {
    const result = await pool.query(`
      SELECT pi.plan_id, gl.ma_region, 
        SUM(pbdr.tot_drug_cst) AS total_drug_cost,
        SUM(pbdr.tot_30day_fills) AS total_fills
      FROM prescribers_by_geography_drug pbdr
      JOIN plan_info pi ON pi.plan_id = $1
      JOIN geographic_locator gl ON pi.ma_region_code = gl.ma_region_code
      WHERE gl.ma_region_code = $2
      GROUP BY pi.plan_id, gl.ma_region
    `, [plan_id, ma]);
    return successResponse(res, result.rows);
  } catch(err) {
    return errorResponse(res, 'Failed to fetch drug costs', 500, err.message);
  }
});


// top underperforming drugs by region, ranked by highest average cost per fill and lowest prescription volume.
app.get('/api/underperforming_drugs', async (req, res) => {
  try { 
    const { year, limit } = req.query; // Optional query param for year filtering
    const yearFilter = year ? `WHERE pd.year = $1` : '';
    const limitFilter = limit ? `LIMIT ${limit}` : '';

    const params = year ? [parseInt(year, 10)] : [];

    const query = `
      SELECT 
        pd.prscrbr_geo_desc AS region,
        pd.brnd_name AS drug,
        SUM(pd.tot_30day_fills) AS total_fills,
        SUM(pd.tot_drug_cst) AS total_cost,
        CASE WHEN SUM(pd.tot_30day_fills) > 0 THEN ROUND(SUM(pd.tot_drug_cst) / SUM(pd.tot_30day_fills), 2) ELSE 0 END AS avg_cost_per_fill
      FROM prescribers_by_geography_drug pd
      ${yearFilter}
      GROUP BY region, drug
      HAVING SUM(pd.tot_30day_fills) >= 10
      ORDER BY avg_cost_per_fill DESC, total_fills ASC
      ${limitFilter};
    `;

    const result = await pool.query(query, params);
    return successResponse(res, result.rows);
  } catch (error) {
    return errorResponse(res, 'Error fetching underperforming drugs', 500, error.message);
  }
});

// given a word (brand or generic name), get the list of brand_names and generic_names containing that word
app.get('/api/drugs/search', async (req, res) => {
  try {

    const { name } = req.query;

    const query = `
      SELECT *
      FROM prescribers_by_geography_drug
      WHERE brnd_name ILIKE $1
         OR gnrc_name ILIKE $1
      LIMIT 100;  -- limit to avoid large results, adjust as needed
    `;

    const values = [`%${name}%`];
    const result = await pool.query(query, values);

    return successResponse(res, result.rows);
  } catch (error) {
    return errorResponse(res, 'Error searching drugs', 500, error.message);
  }
});

// given rxcui code, get the list of brand_names and generic_names
app.get('/api/rxnorm', async (req, res) => {
  try {
    const { rxcui } = req.query;
    const apiUrl = `https://rxnav.nlm.nih.gov/REST/rxcui/${rxcui}/properties.json`;

    const response = await fetch(apiUrl);

    console.log('RxNorm API response:', response);

    if (!response.ok) {
      return res.status(500).json({ success: false, error: 'Failed to fetch RxNorm data' });
    }

    const data = await response.json();
    const fullName = data.properties?.name || '';
    const firstWord = fullName.split(' ')[0] || '';

    const query = `
      SELECT DISTINCT brnd_name, gnrc_name
      FROM prescribers_by_geography_drug
      WHERE brnd_name ILIKE $1
        OR gnrc_name ILIKE $1
      LIMIT 100;  -- limit to avoid large results, adjust as needed
    `;

    const values = [`%${firstWord}%`];
    const result = await pool.query(query, values);

    return successResponse(res, result.rows);


  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});


// Global 404
app.use((req, res) => {
  return errorResponse(res, 'Not found', 404);
});

// Global error handler
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  return errorResponse(res, 'Unexpected server error', 500);
});

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, '0.0.0.0', () => {
  console.log(`API server listening on port ${PORT}`);
});