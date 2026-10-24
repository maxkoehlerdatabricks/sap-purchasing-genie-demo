# Databricks notebook source
# MAGIC %md
# MAGIC # 05a — Create the 2 Genie Spaces (SDK + data-rooms API)
# MAGIC
# MAGIC Creates two domain-split Genie spaces and fully configures them with **all**
# MAGIC instruction types, sample questions, trusted SQL functions and value-dictionary
# MAGIC awareness — everything an analyst needs to demo natural-language querying.
# MAGIC
# MAGIC * **Space 1 — Sourcing & Supplier Risk:** MATERIAL, SUPPLIER, SOURCING,
# MAGIC   SUPPLIER_RISK, VALUE_DICTIONARY, RISK_WORKLIST + `mv_sourcing_coverage`,
# MAGIC   `mv_supplier_risk`, plus the live Lakebase case functions from `10a`.
# MAGIC * **Space 2 — Purchase Order Analytics:** PURCHASE_ORDER, MATERIAL, SUPPLIER,
# MAGIC   SUPPLIER_RISK, VALUE_DICTIONARY + `mv_delivery_performance`, `mv_price_analytics`.
# MAGIC
# MAGIC ## Instruction types used (all of them)
# MAGIC The Genie **data-rooms** API supports these instruction types; we use each one:
# MAGIC * `TEXT_INSTRUCTION`   — general/domain notes (**add these FIRST** — highest ROI).
# MAGIC * `SQL_INSTRUCTION`    — example queries anchoring tricky questions.
# MAGIC * `CERTIFIED_ANSWER`   — trusted SQL functions Genie can call directly.
# MAGIC * plus **sample questions** shown in the UI.
# MAGIC
# MAGIC The full human-readable instruction text lives in
# MAGIC `genie_instructions_space1.md` / `genie_instructions_space2.md` (with the "use
# MAGIC first" recommendation). This notebook embeds a distilled version so it is fully
# MAGIC self-contained and runnable on a blank workspace.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
NS = CFG.namespace


# ---- Thin, self-contained client for the Genie data-rooms API ----
# We replicate exactly the endpoints the platform uses so this notebook has no
# dependency on any external helper package.
def _do(method, path, body=None, params=None):
    return w.api_client.do(method, path, body=body, query=params)


def genie_find_by_name(display_name):
    # Use the raw data-rooms REST API (the runtime's bundled SDK may not expose w.genie).
    try:
        resp = _do("GET", "/api/2.0/data-rooms/", params={"page_size": 200})
    except Exception:
        return None
    for s in (resp.get("data_rooms") or resp.get("spaces") or []):
        name = s.get("display_name") or s.get("title") or ""
        if name == display_name:
            return s.get("space_id") or s.get("id")
    return None


def genie_create(display_name, table_identifiers, description):
    body = {
        "display_name": display_name,
        "warehouse_id": CFG.warehouse_id,
        "table_identifiers": table_identifiers,
        "run_as_type": "VIEWER",
        "description": description,
    }
    res = _do("POST", "/api/2.0/data-rooms/", body=body)
    return res.get("space_id") or res.get("id")


def genie_delete(space_id):
    _do("DELETE", f"/api/2.0/data-rooms/{space_id}")


def add_instruction(space_id, title, content, instruction_type):
    return _do("POST", f"/api/2.0/data-rooms/{space_id}/instructions",
               body={"title": title, "content": content, "instruction_type": instruction_type})


def add_text(space_id, title, content):
    return add_instruction(space_id, title, content, "TEXT_INSTRUCTION")


def add_sql(space_id, title, content):
    return add_instruction(space_id, title, content, "SQL_INSTRUCTION")


def add_function(space_id, fq_function_name):
    return add_instruction(space_id, "SQL Function", fq_function_name, "CERTIFIED_ANSWER")


def add_sample_question(space_id, question_text):
    return _do("POST", f"/api/2.0/data-rooms/{space_id}/curated-questions",
               body={"curated_question": {"data_space_id": space_id, "question_text": question_text,
                                          "question_type": "SAMPLE_QUESTION", "is_deprecated": False},
                     "data_space_id": space_id})


def recreate_space(display_name, table_identifiers, description):
    """Idempotent: delete an existing same-named space, then create fresh."""
    existing = genie_find_by_name(display_name)
    if existing:
        print(f"  Deleting existing space '{display_name}' ({existing}) to recreate cleanly.")
        try:
            genie_delete(existing)
        except Exception as e:
            print(f"  (could not delete: {e})")
    sid = genie_create(display_name, table_identifiers, description)
    print(f"  ✓ Created '{display_name}' → space_id={sid}")
    return sid


# COMMAND ----------

# MAGIC %md
# MAGIC ## Space 1 — Sourcing & Supplier Risk

# COMMAND ----------

s1_tables = [
    CFG.fq("MATERIAL"), CFG.fq("SUPPLIER"), CFG.fq("SOURCING"),
    CFG.fq("SUPPLIER_RISK"), CFG.fq("VALUE_DICTIONARY"),
    CFG.fq("mv_sourcing_coverage"), CFG.fq("mv_supplier_risk"),
]
# Lakebase loop (10a/10b): the risk worklist table and the case functions, when present.
HAS_CASES = spark.catalog.functionExists(CFG.fq("single_sourced_risky_without_open_case"))
if spark.catalog.tableExists(CFG.fq("RISK_WORKLIST")):
    s1_tables.append(CFG.fq("RISK_WORKLIST"))
s1_desc = (
    "SAP purchasing — Sourcing & Supplier Risk. Answers sourcing-risk and "
    "supply-continuity questions: multi-sourcing gaps, single-sourced materials, "
    "suppliers with delivery risk, purchasing blocks, and quota concentration. "
    "Grains: MATERIAL=MATNR+WERKS, SUPPLIER=LIFNR+EKORG, SOURCING=MATNR+WERKS+LIFNR, "
    "SUPPLIER_RISK=LIFNR time-sliced. A 'valid, non-blocked vendor' = SOURCING row with "
    "IS_VALID_SOURCE=true. Delivery risk is time-sliced (VALID_FROM..VALID_TO)."
)
s1 = recreate_space(CFG.genie_space_1_name, s1_tables, s1_desc)

# ---- 1) TEXT_INSTRUCTIONS (add FIRST — highest ROI) ----
add_text(s1, "Domain & vocabulary",
    "This space answers sourcing-risk questions for an automotive OEM's SAP purchasing "
    "data. Only query the gold tables, functions and metric views here — never raw SAP "
    "tables. A 'valid, non-blocked vendor' is a SOURCING row with IS_VALID_SOURCE=true "
    "(NOTKZ<>'X'). 'Multi-sourced' = material+plant with >=2 valid vendors; "
    "'single-sourced' = exactly one; 'no sourcing' = zero.")
add_text(s1, "Time-sliced risk rule",
    "Delivery risk is time-sliced. A supplier is at risk only when DELIVERY_RISK_FLAG='X' "
    "AND the reference date is between VALID_FROM and VALID_TO. Use current_date() for "
    "'currently'. SPERM='X' on SUPPLIER means a purchasing block. QUOTA may be NULL in "
    "SOURCING (source-listed but not quota-split) — a meaningful gap, not an error.")
add_text(s1, "Value dictionary",
    "Use the VALUE_DICTIONARY table (DOMAIN, CODE, LABEL) to translate business words to "
    "codes: 'raw material'->MTART='ROH', 'blocked vendor'->SPERM='X', "
    "'delivery risk'->DELIVERY_RISK_FLAG='X'.")

if HAS_CASES:
    add_text(s1, "Mitigation cases (live from Lakebase)",
        "Buyers act on sourcing risks by opening mitigation cases in the App; the cases live in "
        "Lakebase and are read live. A case is ACTIVE when status is OPEN or IN_PROGRESS. "
        "'Unmitigated', 'no action', 'nobody working on it' or 'without a case' = a single-sourced, "
        "currently risky material+plant with no active case: use single_sourced_risky_without_open_case(). "
        "For the cases themselves (who owns what, how long open) use open_mitigation_cases(). "
        "RISK_WORKLIST is the daily snapshot of single-sourced risky materials with descriptions.")

# ---- 2) SQL_INSTRUCTIONS (add SECOND: joins/examples) ----
add_sql(s1, "Join SOURCING to MATERIAL",
    f"SELECT s.*, m.MATKL, m.MTART FROM {NS}.SOURCING s "
    f"JOIN {NS}.MATERIAL m ON s.MATNR=m.MATNR AND s.WERKS=m.WERKS")
add_sql(s1, "Q3 single-sourced & currently risky",
    "WITH single AS (SELECT MATNR,WERKS,MAX(LIFNR) sole_lifnr FROM SOURCING "
    "WHERE IS_VALID_SOURCE=true GROUP BY MATNR,WERKS HAVING COUNT(*)=1) "
    "SELECT s.MATNR,s.WERKS,s.sole_lifnr,r.RISK_SCORE FROM single s "
    "JOIN SUPPLIER_RISK r ON s.sole_lifnr=r.LIFNR WHERE r.DELIVERY_RISK_FLAG='X' "
    "AND current_date() BETWEEN r.VALID_FROM AND r.VALID_TO")
add_sql(s1, "Q6 quota concentration >80%",
    "SELECT MATNR,WERKS,LIFNR,QUOTA FROM SOURCING WHERE QUOTA IS NOT NULL AND QUOTA>80")
add_sql(s1, "Q1 materials without multi-sourcing (threshold 2)",
    f"SELECT * FROM {NS}.materials_without_multisourcing(2)")

# ---- 3) CERTIFIED_ANSWERS — trusted SQL functions (add LAST) ----
# NOTE: Genie certified answers are most reliable with NO-ARG functions, and
# certified-answer functions do NOT support DATE arguments. So for Q1 we register the
# no-arg default wrapper (threshold baked to 2) and for the time-sliced risk questions
# we register the no-arg "_today" variants (which default the date to current_date()).
for fn in ["materials_without_multisourcing_default",    # Q1 no-arg wrapper (threshold=2)
           "materials_linked_to_risky_supplier_today",   # no-arg today variant
           "single_sourced_risky_today",                 # no-arg today variant
           "vendor_count_per_material",                   # no arg
           "blocked_suppliers_with_active_sourcing",      # no arg
           "quota_concentration",                         # INT arg — OK
           "suppliers_multi_material_group",              # no arg
           "materials_without_valid_sourcing"]:           # no arg
    add_function(s1, f"{NS}.{fn}")
if HAS_CASES:
    for fn in ["single_sourced_risky_without_open_case", "open_mitigation_cases"]:
        add_function(s1, f"{NS}.{fn}")

# ---- Sample questions (shown in UI) ----
for q in [
    "Which materials have no multi-sourcing strategy (fewer than 2 valid vendors)?",
    "Which materials are linked to a supplier flagged with delivery risk?",
    "Which materials are single-sourced and their sole supplier has a delivery-risk flag?",
    "What is the vendor count per material at each plant?",
    "Which suppliers hold a purchasing block but still have active sourcing records?",
    "Which materials have quota arrangements where one vendor holds over 80% of quota?",
    "Which suppliers provide materials across more than one material group?",
    "Which materials have no valid sourcing record at all?",
] + (["Which single-sourced risky materials have no open mitigation case?"] if HAS_CASES else []):
    add_sample_question(s1, q)

print(f"✓ Space 1 fully configured: {s1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Space 2 — Purchase Order Analytics

# COMMAND ----------

s2_tables = [
    CFG.fq("PURCHASE_ORDER"), CFG.fq("MATERIAL"), CFG.fq("SUPPLIER"),
    CFG.fq("SUPPLIER_RISK"), CFG.fq("VALUE_DICTIONARY"),
    CFG.fq("mv_delivery_performance"), CFG.fq("mv_price_analytics"),
]
s2_desc = (
    "SAP purchasing — Purchase Order Analytics. Answers PO questions: on-time delivery "
    "(OTD), net-price variance across vendors, and orders placed while a supplier was at "
    "risk. Grain: PURCHASE_ORDER=EBELN+EBELP. ON_TIME_FLAG=true when GR_DATE<=REQUESTED_DATE; "
    "GR_DATE NULL means not yet received (exclude from OTD). Order-under-risk uses ORDER_DATE "
    "against the supplier's active risk window."
)
s2 = recreate_space(CFG.genie_space_2_name, s2_tables, s2_desc)

# ---- 1) TEXT_INSTRUCTIONS (FIRST) ----
add_text(s2, "Domain & OTD definition",
    "This space answers purchase-order questions. On-time = ON_TIME_FLAG=true "
    "(GR_DATE<=REQUESTED_DATE). GR_DATE NULL = not yet received; exclude from OTD rates. "
    "OTD rate = on-time lines / received lines over a window (last 12 months = "
    "GR_DATE>=add_months(current_date(),-12)).")
add_text(s2, "Price & order-under-risk",
    "NETPR is the net price per PO line. 'Price variance across vendors for a material' = "
    "spread/stddev of each vendor's average NETPR for that MATNR. 'Ordered under active "
    "risk' = PO whose ORDER_DATE is inside a supplier's active delivery-risk window "
    "(DELIVERY_RISK_FLAG='X' and ORDER_DATE BETWEEN VALID_FROM AND VALID_TO).")

# ---- 2) SQL_INSTRUCTIONS ----
add_sql(s2, "Q7 OTD rate per supplier (via trusted function)",
    f"SELECT * FROM {NS}.otd_rate_per_supplier(12)")
add_sql(s2, "Q7 OTD rate per supplier (12 months, inline)",
    "SELECT LIFNR, COUNT(*) deliveries, "
    "ROUND(AVG(CASE WHEN ON_TIME_FLAG THEN 1.0 ELSE 0.0 END),3) otd_rate "
    "FROM PURCHASE_ORDER WHERE GR_DATE IS NOT NULL AND "
    "GR_DATE>=add_months(current_date(),-12) GROUP BY LIFNR ORDER BY otd_rate")
add_sql(s2, "Q8 POs under active delivery risk",
    "SELECT po.EBELN,po.EBELP,po.MATNR,po.LIFNR,po.ORDER_DATE,r.RISK_SCORE "
    "FROM PURCHASE_ORDER po JOIN SUPPLIER_RISK r ON po.LIFNR=r.LIFNR "
    "WHERE r.DELIVERY_RISK_FLAG='X' AND po.ORDER_DATE BETWEEN r.VALID_FROM AND r.VALID_TO")
add_sql(s2, "Q11 net price variance per material",
    "WITH pv AS (SELECT MATNR,LIFNR,AVG(NETPR) ap FROM PURCHASE_ORDER GROUP BY MATNR,LIFNR) "
    "SELECT MATNR, ROUND(MAX(ap)-MIN(ap),2) spread, ROUND(STDDEV(ap),2) sd "
    "FROM pv GROUP BY MATNR HAVING COUNT(*)>1 ORDER BY spread DESC")

# ---- 3) CERTIFIED_ANSWERS ----
# Q7 uses the no-arg wrapper (12-month window baked in) for reliable certified answers,
# in place of the INT-arg otd_rate_per_supplier(months) version.
for fn in ["otd_rate_per_supplier_last_12m", "pos_under_active_risk", "net_price_variance_per_material"]:
    add_function(s2, f"{NS}.{fn}")

# ---- Sample questions ----
for q in [
    "What is the on-time delivery rate per supplier over the last 12 months?",
    "Which materials had purchase orders placed with a supplier despite an active delivery-risk flag at order date?",
    "What is the average net price variance across vendors for the same material?",
    "Which suppliers have the worst on-time delivery performance?",
    "What is total spend per supplier?",
]:
    add_sample_question(s2, q)

print(f"✓ Space 2 fully configured: {s2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the space IDs for downstream notebooks (supervisor, App, benchmark)
# MAGIC We write them to a tiny Delta table so 06_agent / 07_benchmark / the App can look
# MAGIC them up without hard-coding.

# COMMAND ----------

from pyspark.sql import Row

spark.createDataFrame([
    Row(space_key="sourcing_supplier_risk", space_id=s1, display_name=CFG.genie_space_1_name),
    Row(space_key="purchase_order_analytics", space_id=s2, display_name=CFG.genie_space_2_name),
]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.fq("_GENIE_SPACES"))

print("Genie space IDs saved to _GENIE_SPACES:")
display(spark.table(CFG.fq("_GENIE_SPACES")))

# COMMAND ----------

host = w.config.host
print(f"Open Space 1: {host}/genie/rooms/{s1}")
print(f"Open Space 2: {host}/genie/rooms/{s2}")
dbutils.notebook.exit(f"05a_create_genie_spaces: SUCCESS — s1={s1} s2={s2}")
