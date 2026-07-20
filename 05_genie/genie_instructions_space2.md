<!--
================================================================================
Genie Space 2 — "SAP Purchasing — Purchase Order Analytics"
================================================================================
Single source of truth for Space 2 instructions. Same instruction TYPES as Space 1
with the same RECOMMENDED ORDER:
  1) GENERAL / TEXT INSTRUCTIONS   ← add first (vocabulary, OTD & price definitions).
  2) JOIN / RELATIONSHIP HINTS     ← add second (PO↔SUPPLIER/MATERIAL, time-sliced risk).
  3) SQL EXAMPLE QUERIES           ← add third (OTD rate, price variance, PO-under-risk).
  4) CERTIFIED ANSWERS (FUNCTIONS) ← add last (otd_rate_per_supplier, pos_under_active_risk...).
================================================================================
-->

# Space 2 — Purchase Order Analytics

Domain: what was actually ordered, from whom, at what price, and whether it arrived on
time. Tables: `PURCHASE_ORDER` (+ `MATERIAL`, `SUPPLIER` for enrichment,
`SUPPLIER_RISK` for order-time risk), the `mv_delivery_performance` /
`mv_price_analytics` metric views, the trusted functions, and `VALUE_DICTIONARY`.

---

## 1) GENERAL / TEXT INSTRUCTIONS  *(add FIRST)*

- This space answers **purchase-order** questions: on-time delivery (OTD), net-price
  variance across vendors, and orders placed while a supplier was at risk. Query only
  the gold tables, functions, and metric views — never raw SAP tables.
- **On-time delivery**: a PO line is on time when `ON_TIME_FLAG = true` (goods-receipt
  date `GR_DATE` ≤ requested delivery date `REQUESTED_DATE`). `GR_DATE IS NULL` means the
  line has not been received yet — exclude those from OTD rates.
- **OTD rate** = on-time lines ÷ received lines, over a stated window (e.g. last 12
  months uses `GR_DATE >= add_months(current_date(), -12)`).
- **Net price** is `NETPR` per PO line. "Price variance across vendors for the same
  material" = spread/stddev of each vendor's average `NETPR` for that `MATNR`.
- **Order placed under active risk**: a PO whose `ORDER_DATE` falls inside a supplier's
  active delivery-risk window (`DELIVERY_RISK_FLAG='X'` and
  `ORDER_DATE BETWEEN VALID_FROM AND VALID_TO`). Use the ORDER date, not today.
- Grain: `PURCHASE_ORDER` = EBELN (PO) + EBELP (line). A PO can have several lines.
- Use `VALUE_DICTIONARY` for code translation (e.g. country codes, risk categories).

---

## 2) JOIN / RELATIONSHIP INSTRUCTIONS  *(add SECOND)*

- Join `PURCHASE_ORDER` to `SUPPLIER` on `LIFNR` (dedupe to distinct `LIFNR` if org is
  irrelevant, since `SUPPLIER` is LIFNR+EKORG grain).
- Join `PURCHASE_ORDER` to `MATERIAL` on `MATNR` (a PO line references a material; if a
  plant-specific attribute is needed, also constrain `WERKS`).
- Join `PURCHASE_ORDER` to `SUPPLIER_RISK` on `LIFNR` **with the order-date time slice**
  `ORDER_DATE BETWEEN VALID_FROM AND VALID_TO` for "ordered under active risk" questions.

---

## 3) SQL EXAMPLE QUERIES  *(add THIRD)*

**Q7 — on-time delivery rate per supplier over the last 12 months:**
```sql
SELECT LIFNR,
       COUNT(*) AS deliveries,
       ROUND(AVG(CASE WHEN ON_TIME_FLAG THEN 1.0 ELSE 0.0 END), 3) AS otd_rate
FROM PURCHASE_ORDER
WHERE GR_DATE IS NOT NULL
  AND GR_DATE >= add_months(current_date(), -12)
GROUP BY LIFNR
ORDER BY otd_rate;
```

**Q8 — POs placed while the supplier had an active delivery-risk flag:**
```sql
SELECT po.EBELN, po.EBELP, po.MATNR, po.LIFNR, po.ORDER_DATE, r.RISK_SCORE
FROM PURCHASE_ORDER po
JOIN SUPPLIER_RISK r ON po.LIFNR = r.LIFNR
WHERE r.DELIVERY_RISK_FLAG = 'X'
  AND po.ORDER_DATE BETWEEN r.VALID_FROM AND r.VALID_TO;
```

**Q11 — average net-price variance across vendors for the same material:**
```sql
WITH per_vendor AS (
  SELECT MATNR, LIFNR, AVG(NETPR) AS avg_price
  FROM PURCHASE_ORDER GROUP BY MATNR, LIFNR
)
SELECT MATNR,
       ROUND(MAX(avg_price) - MIN(avg_price), 2) AS price_spread,
       ROUND(STDDEV(avg_price), 2) AS price_stddev
FROM per_vendor GROUP BY MATNR HAVING COUNT(*) > 1
ORDER BY price_spread DESC;
```

---

## 4) CERTIFIED ANSWERS — TRUSTED SQL FUNCTIONS  *(add LAST)*

- `otd_rate_per_supplier(months INT)` → Q7
- `pos_under_active_risk()` → Q8
- `net_price_variance_per_material()` → Q11

Example: *"What is the on-time delivery rate per supplier over the last 12 months?"* →
`SELECT * FROM otd_rate_per_supplier(12)`.
