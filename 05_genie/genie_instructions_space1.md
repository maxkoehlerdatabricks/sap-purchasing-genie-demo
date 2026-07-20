<!--
================================================================================
Genie Space 1 — "SAP Purchasing — Sourcing & Supplier Risk"
================================================================================
This file is the SINGLE SOURCE OF TRUTH for the instructions applied to Genie
Space 1. `05a_create_genie_spaces` reads it and pushes the content into the space.

Genie supports several *types* of instruction. This file groups them by type and,
per the spec, includes a RECOMMENDATION on which to add FIRST.

RECOMMENDED ORDER (add in this sequence for the best accuracy-per-effort):
  1) GENERAL / TEXT INSTRUCTIONS   ← add first. Biggest bang for the buck: they
       teach Genie the domain vocabulary, the "valid vendor" definition, and the
       time-slicing rule. Most wrong answers come from missing this context.
  2) JOIN / RELATIONSHIP HINTS     ← add second. Prevent fan-out / wrong joins,
       especially SOURCING↔MATERIAL and *_RISK time-sliced joins.
  3) SQL EXAMPLE QUERIES           ← add third. Anchor the trickiest questions
       (single-sourced-risky, quota concentration) to known-good SQL.
  4) CERTIFIED ANSWERS (FUNCTIONS) ← add last. Point Genie at the trusted SQL
       functions so nuanced questions call a governed asset instead of guessing.
================================================================================
-->

# Space 1 — Sourcing & Supplier Risk

Domain: which vendors may supply which materials, whether materials are multi-sourced,
and which suppliers carry delivery risk. Tables: `MATERIAL`, `SUPPLIER`, `SOURCING`,
`SUPPLIER_RISK`, plus the trusted SQL functions and `mv_sourcing_coverage` /
`mv_supplier_risk` metric views and the `VALUE_DICTIONARY`.

---

## 1) GENERAL / TEXT INSTRUCTIONS  *(add FIRST)*

- This space answers **sourcing-risk** and **supply-continuity** questions for an
  automotive OEM's SAP purchasing data. Never query raw SAP tables — only the gold
  tables, functions, and metric views in this schema.
- **"Valid, non-blocked vendor"** means a row in `SOURCING` where `IS_VALID_SOURCE = true`
  (equivalently `NOTKZ <> 'X'`). Always apply this filter when counting vendors for
  multi-sourcing questions unless the user explicitly asks about blocked sources too.
- **"Multi-sourced"** = a material+plant (`MATNR`+`WERKS`) with **2 or more** valid,
  non-blocked vendors. **"Single-sourced"** = exactly one. **"No sourcing"** = zero.
- **Delivery risk is time-sliced.** A supplier is "at risk" only when
  `DELIVERY_RISK_FLAG = 'X'` AND the question's reference date falls inside
  `VALID_FROM .. VALID_TO`. For "currently", use `current_date()`. For "at order date",
  compare against the PO's order date.
- **QUOTA may be NULL** in `SOURCING` — that means the vendor is source-listed but not
  quota-split. This is a meaningful business gap, not missing data.
- **SPERM = 'X'** on `SUPPLIER` means the vendor holds a **purchasing block**.
- Grain reminders: `MATERIAL` = MATNR+WERKS; `SUPPLIER` = LIFNR+EKORG;
  `SOURCING` = MATNR+WERKS+LIFNR; `SUPPLIER_RISK` = LIFNR + validity window.
- Use the `VALUE_DICTIONARY` table to translate business words to codes, e.g.
  "raw material" → `MTART='ROH'`, "blocked vendor" → `SPERM='X'`,
  "delivery risk" → `DELIVERY_RISK_FLAG='X'`.

---

## 2) JOIN / RELATIONSHIP INSTRUCTIONS  *(add SECOND)*

- Join `SOURCING` to `MATERIAL` on **both** `MATNR` AND `WERKS` (they share that PK).
- Join `SOURCING`/`PURCHASE_ORDER` to `SUPPLIER_RISK` on `LIFNR` only, and **always**
  add the time-slice predicate `<date> BETWEEN VALID_FROM AND VALID_TO`.
- Join to `SUPPLIER` on `LIFNR`; note `SUPPLIER` is at LIFNR+EKORG grain, so if you do
  not need org-specific info, `SELECT DISTINCT LIFNR` first to avoid row fan-out.
- To get a vendor's material group, join `SOURCING` → `MATERIAL` and read `MATKL`.

---

## 3) SQL EXAMPLE QUERIES  *(add THIRD)*

**Q1 — materials without multi-sourcing (fewer than 2 valid vendors):**
```sql
SELECT MATNR, WERKS, COUNT(*) AS valid_vendors
FROM SOURCING
WHERE IS_VALID_SOURCE = true
GROUP BY MATNR, WERKS
HAVING COUNT(*) < 2;
```

**Q3 — single-sourced materials whose sole supplier is currently at delivery risk:**
```sql
WITH single AS (
  SELECT MATNR, WERKS, MAX(LIFNR) AS sole_lifnr
  FROM SOURCING WHERE IS_VALID_SOURCE = true
  GROUP BY MATNR, WERKS HAVING COUNT(*) = 1
)
SELECT s.MATNR, s.WERKS, s.sole_lifnr, r.RISK_SCORE
FROM single s
JOIN SUPPLIER_RISK r ON s.sole_lifnr = r.LIFNR
WHERE r.DELIVERY_RISK_FLAG = 'X'
  AND current_date() BETWEEN r.VALID_FROM AND r.VALID_TO;
```

**Q6 — quota concentration (one vendor > 80%):**
```sql
SELECT MATNR, WERKS, LIFNR, QUOTA
FROM SOURCING
WHERE QUOTA IS NOT NULL AND QUOTA > 80;
```

**Q9 — suppliers spanning more than one material group:**
```sql
SELECT s.LIFNR, COUNT(DISTINCT m.MATKL) AS groups
FROM SOURCING s JOIN MATERIAL m ON s.MATNR=m.MATNR AND s.WERKS=m.WERKS
GROUP BY s.LIFNR HAVING COUNT(DISTINCT m.MATKL) > 1;
```

---

## 4) CERTIFIED ANSWERS — TRUSTED SQL FUNCTIONS  *(add LAST)*

Prefer these governed functions over ad-hoc SQL for the questions they cover:

- `materials_without_multisourcing(min_vendors INT)` → Q1
- `materials_linked_to_risky_supplier(as_of_date DATE)` → Q2
- `single_sourced_risky(as_of_date DATE)` → Q3
- `vendor_count_per_material()` → Q4
- `blocked_suppliers_with_active_sourcing()` → Q5
- `quota_concentration(threshold_pct INT)` → Q6
- `suppliers_multi_material_group()` → Q9
- `materials_without_valid_sourcing()` → Q10

Example: *"Which materials have no multi-sourcing strategy?"* →
`SELECT * FROM materials_without_multisourcing(2)`.
