"""
MDC HR Dashboard — Employee Detail API
GET /employee/{pernr}  →  employee details, positions, and management chain
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent
PA_FILE = DATA_DIR / "mdc_pa_active_only_ext.json"
OM_FILE = DATA_DIR / "mdc_om_relationships_fixed.json"
MAX_MANAGER_LEVELS = 10

GENDER_MAP = {"1": "Male", "2": "Female", "": "Unknown"}
MARITAL_MAP = {"0": "Single", "1": "Married", "2": "Separated", "3": "Divorced", "6": "Other", "": "Unknown"}
STAT2_MAP = {"1": "Withdrawn", "2": "Inactive", "3": "Active", "0": "Not Yet Started"}
MASSN_MAP = {
    "01": "Hiring", "02": "Transfer", "04": "Re-hire",
    "16": "Pay Change", "21": "Org Change",
}

# ---------------------------------------------------------------------------
# In-memory indexes (populated at startup)
# ---------------------------------------------------------------------------
class Store:
    # pernr (int) -> raw employee dict from PA file
    pa_by_pernr: dict[int, dict] = {}
    # zero-padded pernr str -> int pernr  (e.g. "00001023" -> 1023)
    pernr_by_padded: dict[str, int] = {}
    # SAP user ID (upper) -> int pernr
    pernr_by_userid: dict[str, int] = {}

    # OM indexes  (all keyed on str IDs)
    # position_id -> A002 record (reports-to supervisor position)
    reports_to: dict[str, str] = {}            # pos_id -> supervisor_pos_id
    # position_id -> A003 record (org unit)
    pos_to_org: dict[str, dict] = {}           # pos_id -> {id, name}
    # position_id -> holder info  (P pernr or US userid)
    pos_to_holder: dict[str, dict] = {}        # pos_id -> {type, id}
    # position_id -> position text
    pos_text: dict[str, str] = {}

store = Store()


def _build_indexes(pa_data: dict, om_data: list[dict]) -> None:
    # --- PA indexes ---
    for emp in pa_data["employees"]:
        pernr: int = emp["pernr"]
        store.pa_by_pernr[pernr] = emp
        store.pernr_by_padded[str(pernr).zfill(8)] = pernr
        for comm in emp.get("pa0105", []):
            if comm["subty"] == "0001" and comm["usrid"]:
                store.pernr_by_userid[comm["usrid"].upper()] = pernr

    # --- OM indexes ---
    for rel in om_data:
        src_type = rel["sourceType"]
        src_id   = rel["sourceId"]
        tgt_type = rel["targetType"]
        tgt_id   = rel["targetId"]
        relat    = rel["relat"]

        # Collect position text from any side
        if src_type == "S" and rel["sourceText"]:
            store.pos_text[src_id] = rel["sourceText"]
        if tgt_type == "S" and rel["targetText"]:
            store.pos_text[tgt_id] = rel["targetText"]

        # A002: position reports-to position (subordinate → supervisor)
        if relat == "A002" and src_type == "S" and tgt_type == "S" and tgt_id:
            store.reports_to[src_id] = tgt_id

        # A003: position belongs to org unit
        if relat == "A003" and src_type == "S" and tgt_type == "O":
            store.pos_to_org[src_id] = {"id": tgt_id, "name": rel["targetText"]}

        # A008: position is held by person (P) or user (US)
        if relat == "A008" and src_type == "S" and tgt_id:
            store.pos_to_holder[src_id] = {"type": tgt_type, "id": tgt_id}


@asynccontextmanager
async def lifespan(app: FastAPI):
    pa_data = json.loads(PA_FILE.read_text(encoding="utf-8"))
    om_data = json.loads(OM_FILE.read_text(encoding="utf-8"))
    _build_indexes(pa_data, om_data)
    print(f"[startup] Loaded {len(store.pa_by_pernr)} active employees, {len(store.reports_to)} reporting relationships")
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="MDC HR Dashboard API",
    description="Employee detail, positions, and management chain from SAP HR data.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class AddressModel(BaseModel):
    type: str
    street: str
    city: str
    state: str
    postal_code: str
    country: str


class PositionModel(BaseModel):
    position_id: str
    position_title: str
    org_unit_id: Optional[str]
    org_unit_name: Optional[str]
    job_id: Optional[str]
    cost_center: Optional[str]
    valid_from: str
    valid_to: str


class ManagerModel(BaseModel):
    level: int
    pernr: Optional[int]
    first_name: Optional[str]
    last_name: Optional[str]
    full_name: Optional[str]
    email: Optional[str]
    sap_user_id: Optional[str]
    position_id: str
    position_title: Optional[str]
    org_unit_id: Optional[str]
    org_unit_name: Optional[str]


class EmployeeModel(BaseModel):
    pernr: int
    first_name: str
    last_name: str
    full_name: str
    gender: str
    date_of_birth: Optional[str]
    marital_status: str
    email: Optional[str]
    sap_user_id: Optional[str]
    phone: Optional[str]
    addresses: list[AddressModel]
    company_code: str
    personnel_area: str
    personnel_subarea: str
    employee_group: str
    employee_subgroup: str
    org_unit_id: Optional[str]
    org_unit_name: Optional[str]
    cost_center: Optional[str]
    pay_scale_type: Optional[str]
    pay_scale_group: Optional[str]
    pay_scale_step: Optional[str]
    annual_salary: Optional[float]
    currency: Optional[str]
    weekly_hours: Optional[float]
    fte_percent: Optional[float]
    work_schedule: Optional[str]
    employment_status: str
    last_action: str
    action_date: Optional[str]


class EmployeeDetailResponse(BaseModel):
    employee: EmployeeModel
    positions: list[PositionModel]
    managers: list[ManagerModel]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ADDRESS_SUBTYPE = {"1": "Permanent", "7": "Mailing", "3": "Emergency", "4": "Other", "R1": "Special"}

def _build_employee(emp: dict) -> EmployeeModel:
    p2 = emp["pa0002"][0] if emp.get("pa0002") else {}
    p1 = emp["pa0001"][0] if emp.get("pa0001") else {}
    p0 = emp["pa0000"][0] if emp.get("pa0000") else {}
    p7 = emp["pa0007"][0] if emp.get("pa0007") else {}
    p8 = emp["pa0008"][0] if emp.get("pa0008") else {}

    email = sap_user = phone = None
    for comm in emp.get("pa0105", []):
        if comm["subty"] == "0001" and comm["usrid"]:
            sap_user = comm["usrid"]
        elif comm["subty"] == "0010" and comm.get("usridLong"):
            email = comm["usridLong"].strip()
        elif comm["subty"] in ("0020", "CELL") and comm.get("usridLong"):
            phone = comm["usridLong"].strip()

    addresses = []
    for addr in emp.get("pa0006", []):
        addresses.append(AddressModel(
            type=ADDRESS_SUBTYPE.get(addr.get("subty", ""), addr.get("subty", "")),
            street=addr.get("stras", ""),
            city=addr.get("ort01", ""),
            state=addr.get("state", ""),
            postal_code=addr.get("pstlz", ""),
            country=addr.get("land1", ""),
        ))

    org_info = store.pos_to_org.get(str(p1.get("plans", "")), {})

    return EmployeeModel(
        pernr=emp["pernr"],
        first_name=p2.get("vorna", ""),
        last_name=p2.get("nachn", ""),
        full_name=f"{p2.get('vorna', '')} {p2.get('nachn', '')}".strip(),
        gender=GENDER_MAP.get(p2.get("gesch", ""), "Unknown"),
        date_of_birth=p2.get("gbdat") or None,
        marital_status=MARITAL_MAP.get(p2.get("famst", ""), "Unknown"),
        email=email,
        sap_user_id=sap_user,
        phone=phone,
        addresses=addresses,
        company_code=p1.get("bukrs", ""),
        personnel_area=p1.get("werks", ""),
        personnel_subarea=p1.get("btrtl", ""),
        employee_group=p1.get("persg", ""),
        employee_subgroup=p1.get("persk", ""),
        org_unit_id=str(p1["orgeh"]) if p1.get("orgeh") else None,
        org_unit_name=org_info.get("name"),
        cost_center=p1.get("kostl") or None,
        pay_scale_type=p8.get("trfar") or None,
        pay_scale_group=p8.get("trfgr") or None,
        pay_scale_step=p8.get("trfst") or None,
        annual_salary=p8.get("ansal") if p8.get("ansal") else None,
        currency=p8.get("waers") or None,
        weekly_hours=p7.get("wostd"),
        fte_percent=p7.get("empct"),
        work_schedule=p7.get("schkz") or None,
        employment_status=STAT2_MAP.get(p0.get("stat2", ""), "Unknown"),
        last_action=MASSN_MAP.get(p0.get("massn", ""), p0.get("massn", "")),
        action_date=p0.get("begda") or None,
    )


def _build_position(emp: dict) -> list[PositionModel]:
    p1 = emp["pa0001"][0] if emp.get("pa0001") else {}
    pos_id = str(p1.get("plans", "")) if p1.get("plans") else None
    if not pos_id:
        return []
    org = store.pos_to_org.get(pos_id, {})
    return [PositionModel(
        position_id=pos_id,
        position_title=store.pos_text.get(pos_id, ""),
        org_unit_id=org.get("id"),
        org_unit_name=org.get("name"),
        job_id=str(p1["stell"]) if p1.get("stell") else None,
        cost_center=p1.get("kostl") or None,
        valid_from=p1.get("begda", ""),
        valid_to=p1.get("endda", ""),
    )]


def _resolve_holder_pernr(holder: dict) -> Optional[int]:
    """Given an A008 holder {type, id}, return the pernr (int) or None."""
    if holder["type"] == "P":
        return store.pernr_by_padded.get(holder["id"])
    if holder["type"] == "US":
        return store.pernr_by_userid.get(holder["id"].upper())
    return None


def _build_managers(emp: dict) -> list[ManagerModel]:
    p1 = emp["pa0001"][0] if emp.get("pa0001") else {}
    current_pos = str(p1.get("plans", "")) if p1.get("plans") else None
    if not current_pos:
        return []

    managers: list[ManagerModel] = []
    visited: set[str] = {current_pos}

    for level in range(1, MAX_MANAGER_LEVELS + 1):
        supervisor_pos = store.reports_to.get(current_pos)
        if not supervisor_pos or supervisor_pos in visited:
            break
        visited.add(supervisor_pos)

        org = store.pos_to_org.get(supervisor_pos, {})
        mgr = ManagerModel(
            level=level,
            pernr=None,
            first_name=None,
            last_name=None,
            full_name=None,
            email=None,
            sap_user_id=None,
            position_id=supervisor_pos,
            position_title=store.pos_text.get(supervisor_pos),
            org_unit_id=org.get("id"),
            org_unit_name=org.get("name"),
        )

        holder = store.pos_to_holder.get(supervisor_pos)
        if holder:
            mgr_pernr = _resolve_holder_pernr(holder)
            if mgr_pernr and mgr_pernr in store.pa_by_pernr:
                mgr_emp = store.pa_by_pernr[mgr_pernr]
                p2 = mgr_emp["pa0002"][0] if mgr_emp.get("pa0002") else {}
                fn = p2.get("vorna", "")
                ln = p2.get("nachn", "")
                email = sap_user = None
                for comm in mgr_emp.get("pa0105", []):
                    if comm["subty"] == "0001" and comm["usrid"]:
                        sap_user = comm["usrid"]
                    elif comm["subty"] == "0010" and comm.get("usridLong"):
                        email = comm["usridLong"].strip()
                mgr.pernr = mgr_pernr
                mgr.first_name = fn
                mgr.last_name = ln
                mgr.full_name = f"{fn} {ln}".strip()
                mgr.email = email
                mgr.sap_user_id = sap_user

        managers.append(mgr)
        current_pos = supervisor_pos

    return managers


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------
@app.get(
    "/employee/{pernr}",
    response_model=EmployeeDetailResponse,
    summary="Get employee details, positions, and management chain",
    tags=["Employee"],
)
def get_employee(pernr: int):
    """
    Returns full employee profile for the given PERNR:
    - **employee** – personal data, org assignment, pay, contact info
    - **positions** – current position with org unit
    - **managers** – reporting chain (level 1 = direct manager, up to 10 levels)
    """
    emp = store.pa_by_pernr.get(pernr)
    if emp is None:
        raise HTTPException(status_code=404, detail=f"Employee PERNR {pernr} not found or not active.")

    return EmployeeDetailResponse(
        employee=_build_employee(emp),
        positions=_build_position(emp),
        managers=_build_managers(emp),
    )


@app.get(
    "/employees",
    summary="List all active employee PERNRs with basic info",
    tags=["Employee"],
)
def list_employees():
    """
    Returns all active employee PERNRs with name and org unit.
    Useful for populating dropdowns and map analytics.
    """
    result = []
    for pernr, emp in sorted(store.pa_by_pernr.items()):
        p2 = emp["pa0002"][0] if emp.get("pa0002") else {}
        p1 = emp["pa0001"][0] if emp.get("pa0001") else {}
        org = store.pos_to_org.get(str(p1.get("plans", "")), {})
        result.append({
            "pernr": pernr,
            "full_name": f"{p2.get('vorna', '')} {p2.get('nachn', '')}".strip(),
            "org_unit_id": str(p1["orgeh"]) if p1.get("orgeh") else None,
            "org_unit_name": org.get("name"),
        })
    return {"total": len(result), "employees": result}


@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "active_employees": len(store.pa_by_pernr)}
