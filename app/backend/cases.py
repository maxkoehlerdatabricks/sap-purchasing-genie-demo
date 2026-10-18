"""Mitigation cases — the operational part of the App, backed by Lakebase (Postgres).

Reads the risk worklist (a synced table: Delta RISK_WORKLIST -> Lakebase) and reads and
writes mitigation cases with plain SQL over a Postgres connection. Every write runs in
one transaction together with its audit row in case_event.

Auth: the App's service principal connects as its own Postgres role with a short-lived
OAuth token from /api/2.0/postgres/credentials (cached, refreshed before expiry).
Connection coordinates are injected by app/deploy_app as environment variables.
"""

import logging
import os
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import psycopg
from fastapi import APIRouter, HTTPException, Request
from psycopg import sql
from psycopg.rows import dict_row
from pydantic import BaseModel

log = logging.getLogger("sap-app.cases")

PG_HOST = os.environ.get("LAKEBASE_HOST") or os.environ.get("PGHOST", "")
PG_DATABASE = os.environ.get("LAKEBASE_DATABASE") or os.environ.get("PGDATABASE", "purchasing_ops")
PG_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "")
OPS = os.environ.get("LAKEBASE_SCHEMA", "ops")
SYNC = os.environ.get("LAKEBASE_SYNC_SCHEMA", "purchasing_demo_sync")

LAKEBASE_INFO = {"host": PG_HOST, "database": PG_DATABASE, "ops_schema": OPS, "sync_schema": SYNC}

router = APIRouter(prefix="/api/cases")

_token_lock = threading.Lock()
_token = {"value": None, "expires": 0.0}


def _ws():
    from backend.main import _ws as main_ws
    return main_ws()


def _pg_token() -> str:
    """Return a cached Postgres OAuth token, minting a new one every 40 minutes."""
    with _token_lock:
        if _token["value"] and time.time() < _token["expires"]:
            return _token["value"]
        res = _ws().api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": PG_ENDPOINT})
        _token["value"], _token["expires"] = res["token"], time.time() + 40 * 60
        return _token["value"]


def _pg_user() -> str:
    # In the App runtime this is the service principal's client id (its Postgres role).
    return os.environ.get("DATABRICKS_CLIENT_ID") or _ws().current_user.me().user_name


def _connect():
    return psycopg.connect(host=PG_HOST, dbname=PG_DATABASE, user=_pg_user(), password=_pg_token(),
                           sslmode="require", row_factory=dict_row, connect_timeout=15)


def _q(text: str) -> sql.Composed:
    """Format a query with the ops / sync schema identifiers."""
    return sql.SQL(text).format(ops=sql.Identifier(OPS), sync=sql.Identifier(SYNC))


def _safe(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def _rows(cur):
    return [{k: _safe(v) for k, v in r.items()} for r in cur.fetchall()]


def _actor(request: Request) -> str:
    # Databricks Apps forwards the signed-in user's identity in these headers.
    return (request.headers.get("X-Forwarded-Email")
            or request.headers.get("X-Forwarded-Preferred-Username") or "app-user")


def _timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return out, round((time.perf_counter() - t0) * 1000, 1)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
@router.get("/meta")
def meta():
    """Buyers and the action catalog (reference data for the case form)."""
    with _connect() as c, c.cursor() as cur:
        cur.execute(_q("SELECT buyer_id, full_name, material_group, role FROM {ops}.buyer ORDER BY buyer_id"))
        buyers = _rows(cur)
        cur.execute(_q("SELECT action_code, label, description, typical_lead_time_days FROM {ops}.action_type ORDER BY label"))
        actions = _rows(cur)
    return {"buyers": buyers, "actions": actions}


@router.get("/worklist")
def worklist():
    """The synced risk worklist with each row's active case (if any). One Postgres query."""
    def run():
        with _connect() as c, c.cursor() as cur:
            cur.execute(_q("""
                SELECT w."MATNR" AS matnr, w."WERKS" AS werks, w."LIFNR" AS lifnr, w."MAKTX" AS maktx,
                       w."MATKL" AS matkl, w."SUPPLIER_NAME" AS supplier_name, w."SUPPLIER_COUNTRY" AS country,
                       w."RISK_CATEGORY" AS risk_category, w."RISK_SCORE" AS risk_score,
                       w."OPEN_PO_LINES" AS open_po_lines, w."REFRESHED_AT" AS refreshed_at,
                       c.case_id, c.status, c.action_code, c.buyer_id, c.due_date
                FROM {sync}.risk_worklist w
                LEFT JOIN {ops}.mitigation_case c
                       ON c.matnr = w."MATNR" AND c.werks = w."WERKS" AND c.status IN ('OPEN','IN_PROGRESS')
                ORDER BY (c.case_id IS NOT NULL), w."RISK_SCORE" DESC, w."MATNR", w."WERKS"
            """))
            return _rows(cur)
    rows, ms = _timed(run)
    return {"rows": rows, "elapsed_ms": ms}


@router.get("/summary")
def summary():
    """Headline numbers: worklist size, unmitigated risks, cases by status."""
    with _connect() as c, c.cursor() as cur:
        cur.execute(_q("""
            SELECT (SELECT count(*) FROM {sync}.risk_worklist) AS worklist,
                   (SELECT count(*) FROM {sync}.risk_worklist w WHERE NOT EXISTS (
                        SELECT 1 FROM {ops}.mitigation_case c WHERE c.matnr = w."MATNR" AND c.werks = w."WERKS"
                          AND c.status IN ('OPEN','IN_PROGRESS'))) AS unmitigated,
                   count(*) FILTER (WHERE status = 'OPEN') AS open,
                   count(*) FILTER (WHERE status = 'IN_PROGRESS') AS in_progress,
                   count(*) FILTER (WHERE status = 'DONE') AS done
            FROM {ops}.mitigation_case
        """))
        return _rows(cur)[0]


@router.get("")
def list_cases(buyer_id: Optional[str] = None, status: Optional[str] = None):
    """All cases (optionally for one buyer / status), newest first."""
    def run():
        with _connect() as c, c.cursor() as cur:
            cur.execute(_q("""
                SELECT c.case_id, c.matnr, c.werks, c.lifnr, c.risk_score, c.status, c.priority, c.due_date,
                       c.note, c.created_by, c.created_at, c.updated_at, c.buyer_id, b.full_name AS buyer_name,
                       c.action_code, a.label AS action_label, w."MAKTX" AS maktx, w."SUPPLIER_NAME" AS supplier_name
                FROM {ops}.mitigation_case c
                JOIN {ops}.buyer b ON b.buyer_id = c.buyer_id
                JOIN {ops}.action_type a ON a.action_code = c.action_code
                LEFT JOIN {sync}.risk_worklist w ON w."MATNR" = c.matnr AND w."WERKS" = c.werks
                WHERE (%(buyer)s::text IS NULL OR c.buyer_id = %(buyer)s)
                  AND (%(status)s::text IS NULL OR c.status = %(status)s)
                ORDER BY c.updated_at DESC
            """), {"buyer": buyer_id or None, "status": status or None})
            return _rows(cur)
    rows, ms = _timed(run)
    return {"rows": rows, "elapsed_ms": ms}


@router.get("/{case_id}/events")
def case_events(case_id: int):
    with _connect() as c, c.cursor() as cur:
        cur.execute(_q("""SELECT event_id, event_type, from_status, to_status, actor, note, created_at
                          FROM {ops}.case_event WHERE case_id = %s ORDER BY created_at, event_id"""), (case_id,))
        return _rows(cur)


# ---------------------------------------------------------------------------
# Writes (each in one transaction with its audit event)
# ---------------------------------------------------------------------------
class NewCase(BaseModel):
    matnr: str
    werks: str
    action_code: str
    buyer_id: str
    priority: str = "MEDIUM"
    due_date: Optional[date] = None
    note: Optional[str] = None


class CaseUpdate(BaseModel):
    status: Optional[str] = None
    buyer_id: Optional[str] = None
    note: Optional[str] = None


@router.post("")
def create_case(body: NewCase, request: Request):
    actor = _actor(request)

    def run():
        with _connect() as c:
            with c.transaction(), c.cursor() as cur:
                cur.execute(_q("""SELECT "LIFNR" AS lifnr, "RISK_SCORE" AS risk_score FROM {sync}.risk_worklist
                                  WHERE "MATNR" = %s AND "WERKS" = %s"""), (body.matnr, body.werks))
                wl = cur.fetchone()
                if not wl:
                    raise HTTPException(404, f"{body.matnr}/{body.werks} is not on the risk worklist.")
                cur.execute(_q("""
                    INSERT INTO {ops}.mitigation_case
                      (matnr, werks, lifnr, risk_score, action_code, buyer_id, priority, due_date, note, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            COALESCE(%s, current_date + (SELECT typical_lead_time_days FROM {ops}.action_type WHERE action_code = %s)),
                            %s, %s)
                    RETURNING case_id"""),
                    (body.matnr, body.werks, wl["lifnr"], wl["risk_score"], body.action_code, body.buyer_id,
                     body.priority, body.due_date, body.action_code, body.note, actor))
                case_id = cur.fetchone()["case_id"]
                cur.execute(_q("""INSERT INTO {ops}.case_event (case_id, event_type, to_status, actor, note)
                                  VALUES (%s, 'CREATED', 'OPEN', %s, %s)"""), (case_id, actor, body.note))
                return case_id

    try:
        case_id, ms = _timed(run)
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, f"{body.matnr}/{body.werks} already has an active case.")
    except (psycopg.errors.ForeignKeyViolation, psycopg.errors.CheckViolation) as e:
        raise HTTPException(400, str(e).splitlines()[0])
    log.info("case %s created by %s in %s ms", case_id, actor, ms)
    return {"case_id": case_id, "elapsed_ms": ms}


@router.patch("/{case_id}")
def update_case(case_id: int, body: CaseUpdate, request: Request):
    actor = _actor(request)

    def run():
        with _connect() as c:
            with c.transaction(), c.cursor() as cur:
                cur.execute(_q("SELECT status, buyer_id FROM {ops}.mitigation_case WHERE case_id = %s FOR UPDATE"), (case_id,))
                cur_row = cur.fetchone()
                if not cur_row:
                    raise HTTPException(404, f"Case {case_id} not found.")
                cur.execute(_q("""UPDATE {ops}.mitigation_case
                                  SET status = COALESCE(%s, status), buyer_id = COALESCE(%s, buyer_id),
                                      note = COALESCE(%s, note), updated_at = now()
                                  WHERE case_id = %s"""), (body.status, body.buyer_id, body.note, case_id))
                if body.status and body.status != cur_row["status"]:
                    event = ("STATUS_CHANGE", cur_row["status"], body.status)
                elif body.buyer_id and body.buyer_id != cur_row["buyer_id"]:
                    event = ("REASSIGNED", None, None)
                else:
                    event = ("NOTE", None, None)
                cur.execute(_q("""INSERT INTO {ops}.case_event (case_id, event_type, from_status, to_status, actor, note)
                                  VALUES (%s, %s, %s, %s, %s, %s)"""), (case_id, *event, actor, body.note))

    try:
        _, ms = _timed(run)
    except psycopg.errors.CheckViolation as e:
        raise HTTPException(400, str(e).splitlines()[0])
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, "This material already has another active case.")
    return {"case_id": case_id, "elapsed_ms": ms}
