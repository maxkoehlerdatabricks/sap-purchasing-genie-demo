# Databricks notebook source
# MAGIC %md
# MAGIC # _helper/01_simulate_raw_tables — Simulate all 13 RAW_* SAP tables
# MAGIC
# MAGIC Every raw table mirrors a standard SAP table and grain (spec §3) and is
# MAGIC **simulated independently before any joining happens**. We use a deterministic
# MAGIC seed so the dataset is fully reproducible on a blank workspace.
# MAGIC
# MAGIC ## Design goals — the data must *guarantee* demoable answers
# MAGIC The distributions are engineered so every one of the 11 business questions
# MAGIC returns a meaningful, non-empty result:
# MAGIC * **Single-sourced materials** (only 1 valid vendor) → Q1, Q3, Q10.
# MAGIC * **Suppliers with a delivery-risk flag** → Q2, Q3, Q8.
# MAGIC * **Suppliers with a purchasing block (SPERM) yet active sourcing** → Q5.
# MAGIC * **Quota arrangements where one vendor holds > 80%** → Q6.
# MAGIC * **NULL quotas** on some source-listed vendors (a deliberate, meaningful gap) → Q6.
# MAGIC * **POs placed while a delivery-risk flag was active at order date** → Q8.
# MAGIC * **Suppliers spanning multiple material groups (MATKL)** → Q9.
# MAGIC * **Materials with no valid sourcing record at all** → Q10.
# MAGIC * **Net-price variance across vendors for the same material** → Q11.
# MAGIC * **On-time vs late goods receipts** for a realistic OTD rate → Q7.
# MAGIC
# MAGIC The 13 raw tables (SAP origin / grain):
# MAGIC MARA, MAKT, MARC, LFA1, LFB1, LFM1, EORD, EQUK, EQUP, SUPPLIER_RISK, EKKO, EKPO, EKBE.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and deterministic seed
# MAGIC We generate the data in pure Python (so the distributions are precisely
# MAGIC controllable and reproducible) and then create Spark DataFrames + Delta tables.

# COMMAND ----------

import random
import datetime as dt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType,
)

SEED = 42
random.seed(SEED)

# Reference "today" for the demo. We anchor AS_OF to the *actual current date* so that
# time-based questions (OTD over the last 12 months, currently-active risk windows,
# POs placed under active risk) always resolve against `current_date()` at query time —
# the demo stays "fresh" no matter when it is run. The seed still makes the *shape* of
# the data fully reproducible; only the absolute dates shift with today's date.
AS_OF = spark.sql("SELECT current_date() AS d").first()["d"]

print(f"Simulating raw tables into {CFG.namespace} (seed={SEED}, as_of={AS_OF})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions & controlled distributions
# MAGIC We define a fixed universe of materials, plants, vendors and purchasing orgs,
# MAGIC then deliberately assign sourcing/risk/quota patterns to hit every question.

# COMMAND ----------

# ---- Universe sizes ----
N_MATERIALS = 60          # distinct MATNR
PLANTS = ["1010", "1020", "1030"]          # WERKS (Germany/EU-style plant codes)
N_VENDORS = 25            # distinct LIFNR
PURCH_ORGS = ["1000", "2000"]              # EKORG
COMPANY_CODES = ["1000", "2000"]           # BUKRS

# ---- SAP-style code domains ----
MTART_VALUES = ["ROH", "HALB", "FERT"]     # raw / semi-finished / finished
MATKL_VALUES = ["ELEC", "MECH", "CHEM", "ELAST", "FAST"]  # material groups
LAND1_VALUES = ["DE", "FR", "IT", "CN", "US", "PL"]       # country of vendor
DISPO_VALUES = ["101", "102", "103"]       # MRP controller
SOBSL_VALUES = ["", "30", "40"]            # special procurement key

MATNRS = [f"MAT{100000 + i}" for i in range(N_MATERIALS)]
LIFNRS = [f"V{200000 + i}" for i in range(N_VENDORS)]

# Assign each vendor a home country & name.
VENDOR_META = {}
for i, v in enumerate(LIFNRS):
    VENDOR_META[v] = {
        "NAME1": f"Vendor {i:02d} GmbH" if i % 2 == 0 else f"Supplier {i:02d} Ltd",
        "LAND1": random.choice(LAND1_VALUES),
    }

# ---- Deliberately design vendor risk & block flags ----
# ~30% of vendors carry an active delivery-risk flag; a handful also hold a purchasing
# block (SPERM) while still appearing in active source lists (drives Q5).
RISKY_VENDORS = set(random.sample(LIFNRS, k=int(N_VENDORS * 0.30)))          # delivery risk
BLOCKED_VENDORS = set(random.sample(sorted(RISKY_VENDORS | set(random.sample(LIFNRS, 4))), k=4))
print(f"Risky vendors ({len(RISKY_VENDORS)}): {sorted(RISKY_VENDORS)}")
print(f"Blocked vendors ({len(BLOCKED_VENDORS)}): {sorted(BLOCKED_VENDORS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sourcing strategy per (material, plant)
# MAGIC We pre-compute, for each material+plant, the set of valid vendors. This is the
# MAGIC single most important design step — it drives single-sourcing, multi-sourcing,
# MAGIC no-sourcing, quota concentration, and blocked-but-active patterns.

# COMMAND ----------

# For each material we pick which plants it is produced/procured in.
MATERIAL_PLANTS = {m: random.sample(PLANTS, k=random.choice([1, 2, 3])) for m in MATNRS}

# Categorize materials into sourcing archetypes (guarantees answers to Q1/Q3/Q10):
#   - "none"   : no valid sourcing record at all (Q10)
#   - "single" : exactly 1 valid vendor (Q1 candidate; some single + risky → Q3)
#   - "multi"  : 2+ valid vendors (healthy multi-sourcing)
archetypes = (["none"] * 6) + (["single"] * 20) + (["multi"] * (N_MATERIALS - 26))
random.shuffle(archetypes)
MATERIAL_ARCHETYPE = dict(zip(MATNRS, archetypes))

# Build the concrete sourcing map: {(matnr, werks): [lifnr, ...]}
SOURCING_MAP = {}
for m in MATNRS:
    arch = MATERIAL_ARCHETYPE[m]
    for w in MATERIAL_PLANTS[m]:
        if arch == "none":
            SOURCING_MAP[(m, w)] = []            # no vendors → no sourcing rows
        elif arch == "single":
            SOURCING_MAP[(m, w)] = random.sample(LIFNRS, k=1)
        else:  # multi
            SOURCING_MAP[(m, w)] = random.sample(LIFNRS, k=random.choice([2, 3, 4]))

# Guarantee at least a few "single + risky sole supplier" combos for Q3.
single_combos = [k for k, v in SOURCING_MAP.items() if len(v) == 1]
forced_single_risky = random.sample(single_combos, k=min(4, len(single_combos)))
for (m, w) in forced_single_risky:
    SOURCING_MAP[(m, w)] = [random.choice(sorted(RISKY_VENDORS))]
print(f"Sourcing combos: {len(SOURCING_MAP)} | single: {len(single_combos)} | "
      f"none-materials: {sum(1 for a in MATERIAL_ARCHETYPE.values() if a=='none')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Small helper to write a Delta table with overwrite semantics

# COMMAND ----------


def write_raw(rows, schema, table_name):
    """Create/overwrite a raw Delta table from a list of Row-dicts."""
    fq = CFG.fq(table_name)
    if len(rows) == 0:
        df = spark.createDataFrame([], schema)
    else:
        df = spark.createDataFrame(rows, schema)
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(fq))
    cnt = spark.table(fq).count()
    print(f"  ✓ {table_name:<20} {cnt:>6,} rows  → {fq}")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_MARA — Material master, general data (1 row per MATNR)

# COMMAND ----------

mara_rows = []
for m in MATNRS:
    mara_rows.append({
        "MATNR": m,
        "MTART": random.choice(MTART_VALUES),
        "MATKL": random.choice(MATKL_VALUES),
    })
mara_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("MTART", StringType()),
    StructField("MATKL", StringType()),
])
write_raw(mara_rows, mara_schema, "RAW_MARA")
MATERIAL_MATKL = {r["MATNR"]: r["MATKL"] for r in mara_rows}

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_MAKT — Material descriptions (1 row per MATNR + SPRAS)
# MAGIC We emit multiple languages on purpose so Workflow A must filter to EN.

# COMMAND ----------

makt_rows = []
for i, m in enumerate(MATNRS):
    # English description (always present).
    makt_rows.append({"MATNR": m, "SPRAS": "EN", "MAKTX": f"Component {i:03d} assembly"})
    # German description for ~half of materials (to prove the language filter works).
    if i % 2 == 0:
        makt_rows.append({"MATNR": m, "SPRAS": "DE", "MAKTX": f"Bauteil {i:03d} Baugruppe"})
makt_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("SPRAS", StringType()),
    StructField("MAKTX", StringType()),
])
write_raw(makt_rows, makt_schema, "RAW_MAKT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_MARC — Material master, plant data (1 row per MATNR + WERKS)

# COMMAND ----------

marc_rows = []
for m in MATNRS:
    for w in MATERIAL_PLANTS[m]:
        marc_rows.append({
            "MATNR": m, "WERKS": w,
            "DISPO": random.choice(DISPO_VALUES),
            "SOBSL": random.choice(SOBSL_VALUES),
        })
marc_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("DISPO", StringType()),
    StructField("SOBSL", StringType()),
])
write_raw(marc_rows, marc_schema, "RAW_MARC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_LFA1 — Vendor master, general data (1 row per LIFNR)

# COMMAND ----------

lfa1_rows = [{"LIFNR": v, "NAME1": VENDOR_META[v]["NAME1"], "LAND1": VENDOR_META[v]["LAND1"]}
             for v in LIFNRS]
lfa1_schema = StructType([
    StructField("LIFNR", StringType()),
    StructField("NAME1", StringType()),
    StructField("LAND1", StringType()),
])
write_raw(lfa1_rows, lfa1_schema, "RAW_LFA1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_LFB1 — Vendor master, company code data (1 row per LIFNR + BUKRS)
# MAGIC Simulated for completeness (spec §3) but intentionally **dropped** in Workflow B.

# COMMAND ----------

lfb1_rows = []
for v in LIFNRS:
    for b in random.sample(COMPANY_CODES, k=random.choice([1, 2])):
        lfb1_rows.append({"LIFNR": v, "BUKRS": b})
lfb1_schema = StructType([
    StructField("LIFNR", StringType()),
    StructField("BUKRS", StringType()),
])
write_raw(lfb1_rows, lfb1_schema, "RAW_LFB1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_LFM1 — Vendor master, purchasing org data (1 row per LIFNR + EKORG)
# MAGIC `SPERM` = purchasing block flag. Blocked vendors get SPERM='X' in at least one org.

# COMMAND ----------

lfm1_rows = []
for v in LIFNRS:
    orgs = random.sample(PURCH_ORGS, k=random.choice([1, 2]))
    for eo in orgs:
        sperm = "X" if (v in BLOCKED_VENDORS) else ""
        lfm1_rows.append({"LIFNR": v, "EKORG": eo, "SPERM": sperm})
lfm1_schema = StructType([
    StructField("LIFNR", StringType()),
    StructField("EKORG", StringType()),
    StructField("SPERM", StringType()),
])
write_raw(lfm1_rows, lfm1_schema, "RAW_LFM1")
# Track which (vendor) is in which purch org for downstream PO generation.
VENDOR_ORGS = {}
for r in lfm1_rows:
    VENDOR_ORGS.setdefault(r["LIFNR"], []).append(r["EKORG"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_EORD — Source list (1 row per MATNR + WERKS + LIFNR)
# MAGIC `NOTKZ` = blocked-source indicator; `FLIFN` = fixed-vendor flag; VDATU/BDATU =
# MAGIC validity window. A "valid, non-blocked" vendor means NOTKZ != 'X' and within window.

# COMMAND ----------

eord_rows = []
for (m, w), vendors in SOURCING_MAP.items():
    for v in vendors:
        # Most source-list entries are valid; a few are blocked (NOTKZ='X') to make the
        # "valid, non-blocked" filter meaningful (a material could look multi-sourced
        # but actually be single-sourced once blocked entries are excluded).
        notkz = "X" if random.random() < 0.12 else ""
        eord_rows.append({
            "MATNR": m, "WERKS": w, "LIFNR": v,
            "VDATU": AS_OF - dt.timedelta(days=random.randint(200, 900)),  # valid from
            "BDATU": AS_OF + dt.timedelta(days=random.randint(60, 600)),   # valid to
            "NOTKZ": notkz,
            "FLIFN": "X" if random.random() < 0.25 else "",
        })
eord_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("LIFNR", StringType()),
    StructField("VDATU", DateType()),
    StructField("BDATU", DateType()),
    StructField("NOTKZ", StringType()),
    StructField("FLIFN", StringType()),
])
write_raw(eord_rows, eord_schema, "RAW_EORD")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_EQUK / RAW_EQUP — Quota arrangement header & item
# MAGIC EQUK is the header (1 per MATNR+WERKS); EQUP holds the per-vendor quota split.
# MAGIC We only create quota arrangements for a subset of multi-sourced combos, and for
# MAGIC some of those we force one vendor to hold **> 80%** of the quota (drives Q6).
# MAGIC Vendors that are source-listed but not in a quota arrangement will have NULL
# MAGIC quota after Workflow C — the deliberate, meaningful gap.

# COMMAND ----------

equk_rows = []
equp_rows = []
equk_counter = 0
multi_combos = [(k, v) for k, v in SOURCING_MAP.items() if len(v) >= 2]
# ~60% of multi-sourced combos get a quota arrangement.
quota_combos = random.sample(multi_combos, k=int(len(multi_combos) * 0.6))
# Force ~40% of quota arrangements to be concentrated (>80% to one vendor).
concentrated = set(random.sample(range(len(quota_combos)), k=max(2, int(len(quota_combos) * 0.4))))

for idx, ((m, w), vendors) in enumerate(quota_combos):
    equk_counter += 1
    equk_id = f"QA{equk_counter:05d}"
    equk_rows.append({"MATNR": m, "WERKS": w, "EQUK_ID": equk_id})

    if idx in concentrated and len(vendors) >= 2:
        # One vendor holds 85%, the rest split the remaining 15%.
        lead = vendors[0]
        equp_rows.append({"EQUK_ID": equk_id, "LIFNR": lead, "QUOTA": 85})
        rest = vendors[1:]
        share = int(15 / len(rest))
        for v in rest:
            equp_rows.append({"EQUK_ID": equk_id, "LIFNR": v, "QUOTA": share})
    else:
        # Even-ish split summing ~100.
        share = int(100 / len(vendors))
        acc = 0
        for j, v in enumerate(vendors):
            q = share if j < len(vendors) - 1 else (100 - acc)
            acc += q
            equp_rows.append({"EQUK_ID": equk_id, "LIFNR": v, "QUOTA": q})

equk_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("EQUK_ID", StringType()),
])
equp_schema = StructType([
    StructField("EQUK_ID", StringType()),
    StructField("LIFNR", StringType()),
    StructField("QUOTA", IntegerType()),
])
write_raw(equk_rows, equk_schema, "RAW_EQUK")
write_raw(equp_rows, equp_schema, "RAW_EQUP")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_SUPPLIER_RISK — Supplier risk classification (Ariba-styled), time-sliced
# MAGIC 1 row per LIFNR + VALID_FROM. Risky vendors get an active window covering AS_OF
# MAGIC with `DELIVERY_RISK_FLAG='X'`. We also add historical (expired) windows so the
# MAGIC time-slicing is real (drives Q8: risk active *at order date*).

# COMMAND ----------

RISK_CATEGORIES = ["FINANCIAL", "DELIVERY", "QUALITY", "COMPLIANCE", "GEOPOLITICAL"]
risk_rows = []
for v in LIFNRS:
    if v in RISKY_VENDORS:
        # Historical window (expired) — flag was on, then off.
        risk_rows.append({
            "LIFNR": v, "RISK_CATEGORY": "DELIVERY",
            "DELIVERY_RISK_FLAG": "X", "RISK_SCORE": random.randint(70, 95),
            "VALID_FROM": AS_OF - dt.timedelta(days=random.randint(500, 800)),
            "VALID_TO": AS_OF - dt.timedelta(days=random.randint(200, 400)),
        })
        # Current active window covering AS_OF.
        risk_rows.append({
            "LIFNR": v, "RISK_CATEGORY": "DELIVERY",
            "DELIVERY_RISK_FLAG": "X", "RISK_SCORE": random.randint(75, 99),
            "VALID_FROM": AS_OF - dt.timedelta(days=random.randint(150, 350)),
            "VALID_TO": AS_OF + dt.timedelta(days=random.randint(90, 400)),
        })
    else:
        # Low-risk vendors: a single current window, no delivery-risk flag.
        risk_rows.append({
            "LIFNR": v, "RISK_CATEGORY": random.choice(["FINANCIAL", "QUALITY", "COMPLIANCE"]),
            "DELIVERY_RISK_FLAG": "", "RISK_SCORE": random.randint(5, 45),
            "VALID_FROM": AS_OF - dt.timedelta(days=random.randint(150, 350)),
            "VALID_TO": AS_OF + dt.timedelta(days=random.randint(90, 400)),
        })
risk_schema = StructType([
    StructField("LIFNR", StringType()),
    StructField("RISK_CATEGORY", StringType()),
    StructField("DELIVERY_RISK_FLAG", StringType()),
    StructField("RISK_SCORE", IntegerType()),
    StructField("VALID_FROM", DateType()),
    StructField("VALID_TO", DateType()),
])
write_raw(risk_rows, risk_schema, "RAW_SUPPLIER_RISK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAW_EKKO / RAW_EKPO / RAW_EKBE — Purchase order header, item, history
# MAGIC We generate POs mostly against **valid source-listed vendors** so the PO domain
# MAGIC joins cleanly. Crucially, some POs are placed **while the supplier's delivery-risk
# MAGIC flag was active at the order date** (drives Q8). Goods-receipt events in EKBE
# MAGIC produce a realistic on-time-delivery mix (drives Q7). NETPR varies across vendors
# MAGIC for the same material (drives Q11).

# COMMAND ----------

# Base price per material; each vendor deviates ±, creating cross-vendor price variance.
BASE_PRICE = {m: round(random.uniform(5.0, 500.0), 2) for m in MATNRS}

VGABE_GR = "1"        # goods receipt event type
VGABE_INVOICE = "2"   # invoice receipt event type
BEWTP_GR = "E"        # GR movement
BEWTP_INV = "Q"       # invoice movement

ekko_rows, ekpo_rows, ekbe_rows = [], [], []
ebeln_counter = 4500000000

# Build a pool of (matnr, werks, lifnr) that are actually source-listed & valid-ish.
po_candidates = []
for (m, w), vendors in SOURCING_MAP.items():
    for v in vendors:
        po_candidates.append((m, w, v))

N_POS = 400
for _ in range(N_POS):
    m, w, v = random.choice(po_candidates)
    ebeln_counter += 1
    ebeln = str(ebeln_counter)
    ekorg = random.choice(VENDOR_ORGS.get(v, PURCH_ORGS))
    order_date = AS_OF - dt.timedelta(days=random.randint(1, 360))  # within last ~12 months
    ekko_rows.append({"EBELN": ebeln, "LIFNR": v, "EKORG": ekorg, "AEDAT": order_date})

    n_items = random.choice([1, 1, 2, 3])
    # Pick items: mostly the header material, sometimes other materials from same plant.
    item_materials = [m] + random.sample(MATNRS, k=n_items - 1) if n_items > 1 else [m]
    for li, im in enumerate(item_materials, start=1):
        ebelp = str(li * 10)
        # Price = base ± vendor-specific deviation → cross-vendor variance for Q11.
        vendor_factor = 1.0 + ((hash(v) % 21) - 10) / 100.0  # ±10%
        netpr = round(BASE_PRICE.get(im, 100.0) * vendor_factor, 2)
        # Requested delivery date: order_date + lead time.
        req_date = order_date + dt.timedelta(days=random.randint(14, 60))
        ekpo_rows.append({
            "EBELN": ebeln, "EBELP": ebelp, "MATNR": im, "NETPR": netpr,
            "EINDT": req_date,   # requested delivery date (used to derive ON_TIME_FLAG)
        })
        # Goods receipt: ~70% on-time, ~30% late (delivery-risk vendors skew later).
        late_bias = 12 if v in RISKY_VENDORS else 3
        on_time = random.random() < (0.55 if v in RISKY_VENDORS else 0.80)
        if on_time:
            gr_date = req_date - dt.timedelta(days=random.randint(0, 5))
        else:
            gr_date = req_date + dt.timedelta(days=random.randint(1, late_bias + 20))
        # GR event(s): sometimes a partial + final; we keep the earliest as GR_DATE.
        ekbe_rows.append({
            "EBELN": ebeln, "EBELP": ebelp, "VGABE": VGABE_GR, "BEWTP": BEWTP_GR,
            "EVENT_DATE": gr_date,
        })
        # Invoice event (not a GR — Workflow E filters VGABE to GR only).
        ekbe_rows.append({
            "EBELN": ebeln, "EBELP": ebelp, "VGABE": VGABE_INVOICE, "BEWTP": BEWTP_INV,
            "EVENT_DATE": gr_date + dt.timedelta(days=random.randint(5, 30)),
        })

ekko_schema = StructType([
    StructField("EBELN", StringType()),
    StructField("LIFNR", StringType()),
    StructField("EKORG", StringType()),
    StructField("AEDAT", DateType()),      # PO creation/order date
])
ekpo_schema = StructType([
    StructField("EBELN", StringType()),
    StructField("EBELP", StringType()),
    StructField("MATNR", StringType()),
    StructField("NETPR", DoubleType()),
    StructField("EINDT", DateType()),      # requested delivery date
])
ekbe_schema = StructType([
    StructField("EBELN", StringType()),
    StructField("EBELP", StringType()),
    StructField("VGABE", StringType()),
    StructField("BEWTP", StringType()),
    StructField("EVENT_DATE", DateType()),
])
write_raw(ekko_rows, ekko_schema, "RAW_EKKO")
write_raw(ekpo_rows, ekpo_schema, "RAW_EKPO")
write_raw(ekbe_rows, ekbe_schema, "RAW_EKBE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & sanity checks
# MAGIC Print a quick report so a demo operator can confirm the data hits every scenario.

# COMMAND ----------

for t in RAW_TABLES:
    c = spark.table(CFG.fq(t)).count()
    print(f"{t:<20} {c:>7,} rows")

print("\nDesign guarantees:")
print(f"  risky vendors (delivery risk): {len(RISKY_VENDORS)}")
print(f"  blocked vendors (SPERM):        {len(BLOCKED_VENDORS)}")
print(f"  materials with NO sourcing:     {sum(1 for a in MATERIAL_ARCHETYPE.values() if a=='none')}")
print(f"  single-sourced combos:          {sum(1 for v in SOURCING_MAP.values() if len(v)==1)}")
print(f"  concentrated quota arrangements:{len(concentrated)}")

# COMMAND ----------

dbutils.notebook.exit("01_simulate_raw_tables: SUCCESS — 13 RAW_* tables written.")
