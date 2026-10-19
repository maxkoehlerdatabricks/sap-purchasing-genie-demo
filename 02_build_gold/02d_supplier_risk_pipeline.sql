-- Lakeflow Declarative Pipeline: Workflow D (SAP Ariba Supplier Risk -> SUPPLIER_RISK)
--
-- The one slice of the demo that is ingested from files instead of simulated as a table.
-- SAP Ariba Supplier Risk is exported as JSON (one full snapshot per export day) into the
-- UC volume ${ariba_landing_path}. This pipeline:
--   1. RAW_SUPPLIER_RISK (streaming table): Auto Loader picks up only NEW export files,
--      maps the Ariba fields to the SAP-style columns used by the rest of the demo and
--      keeps lineage columns (_SOURCE_FILE, _INGESTED_AT).
--   2. SUPPLIER_RISK (materialized view, gold): latest export snapshot, referential
--      integrity against the gold SUPPLIER table, data-quality expectations, the primary
--      key (LIFNR, VALID_FROM) and table / column comments.
--
-- Created and run by 02_build_gold/02d_supplier_risk (ensure_supplier_risk_pipeline in
-- 00_config) and by the Lakeflow Job task "gold_supplier_risk". Unqualified table names
-- resolve to the pipeline's catalog and schema (the demo schema).

CREATE OR REFRESH STREAMING TABLE RAW_SUPPLIER_RISK (
  CONSTRAINT has_vendor_id EXPECT (LIFNR IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Raw: SAP Ariba Supplier Risk export records, ingested incrementally by Auto Loader from JSON files in the ariba_landing volume. One row per vendor, risk window and export day. Ariba fields mapped to SAP-style columns; _SOURCE_FILE and _INGESTED_AT give lineage.'
AS SELECT
  smVendorId                                          AS LIFNR,
  riskDomain                                          AS RISK_CATEGORY,
  CASE WHEN deliveryRiskIndicator THEN 'X' ELSE '' END AS DELIVERY_RISK_FLAG,
  CAST(overallRiskScore AS INT)                       AS RISK_SCORE,
  CAST(validity.validFrom AS DATE)                    AS VALID_FROM,
  CAST(validity.validTo AS DATE)                      AS VALID_TO,
  CAST(exportDate AS DATE)                            AS EXPORT_DATE,
  _metadata.file_path                                 AS _SOURCE_FILE,
  current_timestamp()                                 AS _INGESTED_AT
FROM STREAM read_files(
  '${ariba_landing_path}',
  format => 'json',
  schemaHints => 'exportDate STRING, smVendorId STRING, supplierName STRING, riskDomain STRING, deliveryRiskIndicator BOOLEAN, overallRiskScore INT, validity STRUCT<validFrom: STRING, validTo: STRING>'
);

CREATE OR REFRESH MATERIALIZED VIEW SUPPLIER_RISK (
  LIFNR STRING NOT NULL COMMENT 'Vendor the risk assessment applies to (relates to SUPPLIER.LIFNR).',
  RISK_CATEGORY STRING COMMENT 'Risk category: FINANCIAL, DELIVERY, QUALITY, COMPLIANCE, GEOPOLITICAL.',
  DELIVERY_RISK_FLAG STRING COMMENT '''X'' = vendor flagged with delivery risk in this window.',
  RISK_SCORE INT COMMENT 'Risk score 0-100 (higher = riskier).',
  VALID_FROM DATE NOT NULL COMMENT 'Start date of the risk assessment window.',
  VALID_TO DATE COMMENT 'End date of the risk assessment window.',
  CONSTRAINT valid_score EXPECT (RISK_SCORE BETWEEN 0 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT valid_window EXPECT (VALID_FROM <= VALID_TO) ON VIOLATION DROP ROW,
  CONSTRAINT pk_risk PRIMARY KEY (LIFNR, VALID_FROM)
)
COMMENT 'Gold: time-sliced supplier risk classification from SAP Ariba Supplier Risk. One row per vendor (LIFNR) and validity window (VALID_FROM..VALID_TO). DELIVERY_RISK_FLAG=''X'' marks delivery risk. Join at query time; for ''active'' risk, filter VALID_FROM<=date<=VALID_TO. Built by the Lakeflow pipeline from the latest Ariba export, keeping only vendors that exist in SUPPLIER.'
AS SELECT
  r.LIFNR, r.RISK_CATEGORY, r.DELIVERY_RISK_FLAG, r.RISK_SCORE, r.VALID_FROM, r.VALID_TO
FROM RAW_SUPPLIER_RISK r
-- Referential integrity: only vendors known to the SAP vendor master (gold SUPPLIER).
JOIN (SELECT DISTINCT LIFNR FROM SUPPLIER) s ON r.LIFNR = s.LIFNR
-- Each Ariba export is a full snapshot: gold reflects the latest one.
WHERE r.EXPORT_DATE = (SELECT max(EXPORT_DATE) FROM RAW_SUPPLIER_RISK);
