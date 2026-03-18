"""
MDC HR Dashboard — Employee Detail API
GET /employee/{pernr}  →  selectedEmployee, managerChain, directReports (level 1 only)
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
PA_FILE  = DATA_DIR / "mdc_pa_active_only_ext.json"
OM_FILE  = DATA_DIR / "mdc_om_relationships_fixed.json"

MAX_MANAGER_LEVELS = 10   # upward chain depth

GENDER_MAP  = {"1": "Male", "2": "Female", "": "Unknown"}
MARITAL_MAP = {"0": "Single", "1": "Married", "2": "Separated",
               "3": "Divorced", "6": "Other", "": "Unknown"}
STAT2_MAP   = {"1": "Withdrawn", "2": "Inactive", "3": "Active",
               "0": "Not Yet Started"}
MASSN_MAP   = {"01": "Hiring", "02": "Transfer", "04": "Re-hire",
               "16": "Pay Change", "21": "Org Change"}

ADDRESS_SUBTYPE = {"1": "Permanent", "7": "Mailing", "3": "Emergency",
                   "4": "Other", "R1": "Special"}


# ---------------------------------------------------------------------------
# In-memory indexes
# ---------------------------------------------------------------------------
class Store:
    # PA data
    pa_by_pernr:     dict[int, dict]    = {}   # pernr (int) → raw employee record
    pernr_by_padded: dict[str, int]     = {}   # "00001023"  → 1023
    pernr_by_userid: dict[str, int]     = {}   # "MCURLEY"   → 1023  (from PA0105)

    # OM upward: A002  sub_pos → supervisor_pos
    reports_to:      dict[str, str]     = {}

    # OM downward: reverse of A002  supervisor_pos → [sub_pos, …]
    # Built simultaneously with reports_to so it is always consistent.
    direct_reports:  dict[str, list[str]] = {}

    # OM org-unit assignment: A003  pos_id → {id, name}
    pos_to_org:      dict[str, dict]    = {}

    # OM position occupancy: A008  pos_id → {type: "P"|"US", id: "…"}
    pos_to_holder:   dict[str, dict]    = {}

    # Position titles collected from both sides of every OM relationship
    pos_text:        dict[str, str]     = {}


store = Store()


# ---------------------------------------------------------------------------
# Index builder
# ---------------------------------------------------------------------------
def _build_indexes(pa_data: dict, om_data: list[dict]) -> None:
    # ── PA indexes (active employees only — source file is pre-filtered STAT2=3) ──
    for emp in pa_data["employees"]:
        pernr: int = emp["pernr"]
        store.pa_by_pernr[pernr] = emp
        store.pernr_by_padded[str(pernr).zfill(8)] = pernr
        for comm in emp.get("pa0105", []):
            if comm["subty"] == "0001" and comm["usrid"]:
                store.pernr_by_userid[comm["usrid"].upper()] = pernr

    # ── OM indexes ──
    for rel in om_data:
        src_type = rel["sourceType"]
        src_id   = rel["sourceId"]
        tgt_type = rel["targetType"]
        tgt_id   = rel["targetId"]
        relat    = rel["relat"]

        # Collect position titles from both sides of every record
        if src_type == "S" and rel["sourceText"]:
            store.pos_text[src_id] = rel["sourceText"]
        if tgt_type == "S" and rel["targetText"]:
            store.pos_text[tgt_id] = rel["targetText"]

        # A002: subordinate position → supervisor position  (PA0001.plans based)
        if relat == "A002" and src_type == "S" and tgt_type == "S" and tgt_id:
            store.reports_to[src_id] = tgt_id                   # upward
            store.direct_reports.setdefault(tgt_id, [])
            if src_id not in store.direct_reports[tgt_id]:      # avoid duplicates
                store.direct_reports[tgt_id].append(src_id)     # downward

        # A003: position belongs to org unit
        if relat == "A003" and src_type == "S" and tgt_type == "O":
            store.pos_to_org[src_id] = {"id": tgt_id, "name": rel["targetText"]}

        # A008: position is held by person (P) or SAP user (US)
        if relat == "A008" and src_type == "S" and tgt_id:
            store.pos_to_holder[src_id] = {"type": tgt_type, "id": tgt_id}


@asynccontextmanager
async def lifespan(app: FastAPI):
    pa_data = json.loads(PA_FILE.read_text(encoding="utf-8"))
    om_data = json.loads(OM_FILE.read_text(encoding="utf-8"))
    _build_indexes(pa_data, om_data)
    total_dr = sum(len(v) for v in store.direct_reports.values())
    print(
        f"[startup] {len(store.pa_by_pernr)} active employees · "
        f"{len(store.reports_to)} upward links · "
        f"{total_dr} downward links"
    )
    yield


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="MDC HR Dashboard API",
    description="Employee detail, managers (upward) and full reporting structure (downward).",
    version="2.0.0",
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


# ── Upward: manager chain ─────────────────────────────────────────────────────
class ManagerModel(BaseModel):
    level: int                    # 1 = direct manager, N = top of chain (CEO)
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


# ── Downward: directReports — camelCase shape as specified ───────────────────
# A002 direction used:
#   OBJID=childPosition, RSIGN="A", RELAT="002", SOBID=selectedEmployeePosition
#   i.e. JSON: sourceId=childPos  --A002-->  targetId=selectedPos
#   Direct reports = all sourceIds where targetId == selectedEmployee's position.
class DirectReportItem(BaseModel):
    positionId: str
    positionTitle: Optional[str]
    pernr: Optional[int]          # None = position vacant or held by inactive person
    fullName: Optional[str]
    email: Optional[str]          # PA0105 subtype 0010 only; None if not available
    orgUnit: Optional[str]        # org unit name from A003
    level: int = 1                # always 1 — these are immediate direct reports


# ── Employee personal data ────────────────────────────────────────────────────
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


# ── Full response ─────────────────────────────────────────────────────────────
class EmployeeDetailResponse(BaseModel):
    selectedEmployee: EmployeeModel
    positions: list[PositionModel]
    managerChain: list[ManagerModel]       # level 1 = direct manager, level N = CEO
    directReports: list[DirectReportItem]  # level 1 only — camelCase fields


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _resolve_holder_pernr(holder: dict) -> Optional[int]:
    """Resolve A008 holder ({type, id}) → active pernr, or None."""
    if holder["type"] == "P":
        return store.pernr_by_padded.get(holder["id"])
    if holder["type"] == "US":
        return store.pernr_by_userid.get(holder["id"].upper())
    return None


def _enrich_person(pos_id: str) -> dict:
    """
    Return person fields for the active employee currently holding pos_id.
    All fields are None when the position is vacant or held by an inactive person.
    Only active employees (STAT2=3) are resolved — inactive holders are excluded
    because pa_by_pernr is pre-filtered to active-only records.
    """
    null = {"pernr": None, "full_name": None, "email": None, "sap_user_id": None}
    holder = store.pos_to_holder.get(pos_id)
    if not holder:
        return null
    pernr = _resolve_holder_pernr(holder)
    if not pernr or pernr not in store.pa_by_pernr:
        return null
    emp  = store.pa_by_pernr[pernr]
    p2   = emp["pa0002"][0] if emp.get("pa0002") else {}
    email = sap_user = None
    for comm in emp.get("pa0105", []):
        if comm["subty"] == "0001" and comm["usrid"]:
            sap_user = comm["usrid"]
        elif comm["subty"] == "0010" and comm.get("usridLong"):
            email = comm["usridLong"].strip()
    return {
        "pernr":      pernr,
        "full_name":  f"{p2.get('vorna', '')} {p2.get('nachn', '')}".strip(),
        "email":      email,
        "sap_user_id": sap_user,
    }


# ---------------------------------------------------------------------------
# Builder: upward manager chain  (UNCHANGED from original)
# ---------------------------------------------------------------------------
def _build_managers(emp: dict) -> list[ManagerModel]:
    """
    Walk A002 upward from the employee's position (PA0001.plans).
    level 1 = direct manager, level N = CEO / top of chain.
    """
    p1 = emp["pa0001"][0] if emp.get("pa0001") else {}
    current_pos = str(p1.get("plans", "")) if p1.get("plans") else None
    if not current_pos:
        return []

    managers: list[ManagerModel] = []
    visited:  set[str]           = {current_pos}

    for level in range(1, MAX_MANAGER_LEVELS + 1):
        supervisor_pos = store.reports_to.get(current_pos)
        if not supervisor_pos or supervisor_pos in visited:
            break
        visited.add(supervisor_pos)

        org    = store.pos_to_org.get(supervisor_pos, {})
        person = _enrich_person(supervisor_pos)

        managers.append(ManagerModel(
            level=level,
            pernr=person["pernr"],
            first_name=None,   # kept for model compat; full_name is sufficient
            last_name=None,
            full_name=person["full_name"],
            email=person["email"],
            sap_user_id=person["sap_user_id"],
            position_id=supervisor_pos,
            position_title=store.pos_text.get(supervisor_pos),
            org_unit_id=org.get("id"),
            org_unit_name=org.get("name"),
        ))
        current_pos = supervisor_pos

    return managers


# ---------------------------------------------------------------------------
# Builders: downward reporting structure
# ---------------------------------------------------------------------------
def _build_direct_reports(root_pos: str) -> list[DirectReportItem]:
    """
    Return immediate direct reports of root_pos using A002 as primary source.

    A002 direction (HRP1001 terms):
        OTYPE=S, OBJID=childPosition, RSIGN="A", RELAT="002",
        RSOBJT=S, SOBID=selectedEmployeePosition

    In the JSON index this means:
        relat="A002", sourceId=childPosition, targetId=selectedEmployeePosition

    So direct_reports[root_pos] = all sourceIds where A002.targetId == root_pos.

    Only active employees (STAT2=3) are resolved; vacant/inactive → pernr=None.
    PA0002 used for name; PA0105 subtype 0010 for email.
    """
    result: list[DirectReportItem] = []
    for pos_id in store.direct_reports.get(root_pos, []):
        org    = store.pos_to_org.get(pos_id, {})
        person = _enrich_person(pos_id)
        result.append(DirectReportItem(
            positionId=pos_id,
            positionTitle=store.pos_text.get(pos_id),
            pernr=person["pernr"],
            fullName=person["full_name"],
            email=person["email"],
            orgUnit=org.get("name"),
            level=1,
        ))
    return result


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get(
    "/employee/{pernr}",
    response_model=EmployeeDetailResponse,
    summary="Full employee profile with upward manager chain and downward reporting structure",
    tags=["Employee"],
)
def get_employee(pernr: int):
    """
    **Upward** (`managers`):
    - Walks A002 from `PA0001.plans` to the top of the hierarchy.
    - `level 1` = direct manager … `level N` = CEO / root.

    **Downward** (three representations of the same data):
    - `directReports` — immediate reports only (level 1).
    - `allReports` — every descendant, flat list, BFS order, with level numbers
      relative to the selected employee.
    - `reportingTree` — same data as a fully nested recursive structure.

    All downward results resolve only **active** employees (STAT2 = 3) via A008.
    Vacant positions are included with `pernr = null`.
    """
    emp = store.pa_by_pernr.get(pernr)
    if emp is None:
        raise HTTPException(
            status_code=404,
            detail=f"Employee PERNR {pernr} not found or not active (STAT2 ≠ 3).",
        )

    p1      = emp["pa0001"][0] if emp.get("pa0001") else {}
    root_pos = str(p1.get("plans", "")) if p1.get("plans") else None

    return EmployeeDetailResponse(
        selectedEmployee=_build_employee(emp),
        positions=_build_position(emp),
        managerChain=_build_managers(emp),
        directReports=_build_direct_reports(root_pos) if root_pos else [],
    )


@app.get(
    "/employees",
    summary="List all active employee PERNRs with basic info",
    tags=["Employee"],
)
def list_employees():
    result = []
    for pernr, emp in sorted(store.pa_by_pernr.items()):
        p2  = emp["pa0002"][0] if emp.get("pa0002") else {}
        p1  = emp["pa0001"][0] if emp.get("pa0001") else {}
        org = store.pos_to_org.get(str(p1.get("plans", "")), {})
        result.append({
            "pernr":        pernr,
            "full_name":    f"{p2.get('vorna', '')} {p2.get('nachn', '')}".strip(),
            "org_unit_id":  str(p1["orgeh"]) if p1.get("orgeh") else None,
            "org_unit_name": org.get("name"),
        })
    return {"total": len(result), "employees": result}


@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "active_employees": len(store.pa_by_pernr)}


# ---------------------------------------------------------------------------
# Private helpers (used by both upward and downward builders)
# ---------------------------------------------------------------------------
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
