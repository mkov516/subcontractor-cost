import os
import re
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Any
 
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
 
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
 
 
def env_or_default(key: str, default: str) -> str:
    v = os.getenv(key)
    return v.strip() if v and v.strip() else default
 
 
# ---------------------- Config ----------------------
SAP_BASE_URL    = env_or_default("SAP_BASE_URL", "https://my438923.businessbydesign.cloud.sap").rstrip("/")
SAP_ODATA_PATH  = env_or_default("SAP_ODATA_PATH", "/sap/byd/odata/ana_businessanalytics_analytics.svc").strip("/")
 
# Codes query: returns the distinct project IDs to loop over
SAP_CODES_QUERY = env_or_default("SAP_CODES_QUERY", "RPZF595F0F3D4FC6D5380A2C7QueryResults").strip("/")
CODES_FILTER_FIELD = env_or_default("CODES_FILTER_FIELD", "CPROJECT")
 
# Main query: uses each project ID in its mandatory filter
SAP_MAIN_QUERY  = env_or_default("SAP_MAIN_QUERY",  "RPFINCACU02_Q0001QueryResults").strip("/")
MAIN_FILTER_FIELD = env_or_default("MAIN_FILTER_FIELD", "PARA_PROJECT")
MAIN_SETOFBKS = env_or_default("MAIN_SETOFBKS", "6000")
 
# --- Accounting Period range ---
# Overrides the report's "Current Fiscal Period" default. ByD uses PYYYY
# format (e.g. 82025 = period 8 of 2025) and only accepts an enumerated
# list of `eq` values for PARA_ACCYEARPER, not ge/le ranges. We expand
# the configured window into one eq clause per period and OR them.
# Defaults cover SAP go-live (Feb 2025) through end of 2026; bump
# PERIOD_TO_YEAR each year as needed.
PERIOD_FROM_YEAR   = int(env_or_default("PERIOD_FROM_YEAR",   "2025"))
PERIOD_FROM_PERIOD = int(env_or_default("PERIOD_FROM_PERIOD", "2"))
PERIOD_TO_YEAR     = int(env_or_default("PERIOD_TO_YEAR",     "2026"))
PERIOD_TO_PERIOD   = int(env_or_default("PERIOD_TO_PERIOD",   "12"))
PERIODS_PER_YEAR   = int(env_or_default("PERIODS_PER_YEAR",   "12"))
USE_PERIOD_FILTER  = env_or_default("USE_PERIOD_FILTER", "1").lower() not in ("0", "false", "no", "")
 
# --- Concurrency ---
# Number of parallel requests to ByD. 8 is a safe default; bump to 16 if
# stable, lower if you start seeing 503s or "too many requests".
MAX_WORKERS = int(env_or_default("MAX_WORKERS", "8"))
 
OUTPUT_CSV = env_or_default("OUTPUT_CSV", "data/subcontractor-cost.csv")
 
SAP_USERNAME = os.getenv("SAP_USERNAME")
SAP_PASSWORD = os.getenv("SAP_PASSWORD")
 
REQUEST_PAUSE = float(env_or_default("REQUEST_PAUSE", "0.0"))
 
# Shared session -- requests.Session is thread-safe for .get() calls and
# enables HTTP keep-alive across the worker pool. Bump pool size so
# concurrent threads don't queue on the adapter.
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=max(16, MAX_WORKERS * 2),
    pool_maxsize=max(16, MAX_WORKERS * 2),
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)
 
# Fields to pull from the main query. Not renamed in the CSV.
MAIN_SELECT_FIELDS = [
    "T1ACCDOITUIDsCREDITOR_BP",
    "CGLACCT",
    "TGLACCT",
    "CPROJECT",
    "TPROJECT",
    "KCAMTCOMP",
    "CACCPSTDAT",
]
 
# ByD returns Edm.DateTime as "/Date(<ms-since-epoch>)/"; convert to real
# datetimes in the output CSV.
_BYD_DATE_RE = re.compile(r"/Date\((-?\d+)")
DATE_COLUMNS = ["CACCPSTDAT", "CACCTRANDT", "CCREATDT"]
 
 
def _build_period_list() -> List[int]:
    """
    Enumerate every accounting period in the configured window using
    ByD's PYYYY format (e.g. 82025 = period 8 of 2025).
    """
    periods: List[int] = []
    for year in range(PERIOD_FROM_YEAR, PERIOD_TO_YEAR + 1):
        start = PERIOD_FROM_PERIOD if year == PERIOD_FROM_YEAR else 1
        end = PERIOD_TO_PERIOD if year == PERIOD_TO_YEAR else PERIODS_PER_YEAR
        for p in range(start, end + 1):
            periods.append(int(f"{p}{year}"))
    return periods
 
 
PERIOD_LIST: List[int] = _build_period_list() if USE_PERIOD_FILTER else []
 
 
# ---------------------- URL helpers ----------------------
def _auth() -> Optional[HTTPBasicAuth]:
    if SAP_USERNAME and SAP_PASSWORD:
        return HTTPBasicAuth(SAP_USERNAME, SAP_PASSWORD)
    return None
 
 
def _root_url() -> str:
    return f"{SAP_BASE_URL.rstrip('/')}/{SAP_ODATA_PATH.strip('/')}".rstrip("/")
 
 
def _entity_url(entity: str) -> str:
    return f"{_root_url()}/{entity.strip('/')}".rstrip("/")
 
 
def _get_raw(url: str, params: Dict[str, str]) -> requests.Response:
    return SESSION.get(url, params=params, auth=_auth(), timeout=90)
 
 
def _get_json_or_raise(url: str, params: Dict[str, str]) -> Dict:
    resp = _get_raw(url, params)
    if not resp.ok:
        logging.error("HTTP %s for %s params=%s\nBody: %s",
                      resp.status_code, url, params, resp.text[:2000])
        resp.raise_for_status()
    return resp.json()
 
 
def _extract_results_and_next(data: Dict) -> Tuple[List[Dict], Optional[str]]:
    if "d" in data:
        d = data["d"]
        return d.get("results", []), d.get("__next")
    return data.get("value", []), data.get("@odata.nextLink") or data.get("odata.nextLink")
 
 
# ---------------------- Core ETL ----------------------
def fetch_distinct_projects() -> List[str]:
    """Fetch distinct project IDs from the codes query."""
    url = _entity_url(SAP_CODES_QUERY)
    params = {"$select": CODES_FILTER_FIELD, "$top": "1000000", "$format": "json"}
    data = _get_json_or_raise(url, params)
    results, _ = _extract_results_and_next(data)
    vals = [r.get(CODES_FILTER_FIELD) for r in results if r.get(CODES_FILTER_FIELD)]
    distinct = sorted(set(vals))
    logging.info("Fetched %d distinct %s values", len(distinct), CODES_FILTER_FIELD)
    return distinct
 
 
def _build_filter(project_value: str) -> str:
    """
    Build the $filter clause. Constrains Set of Books, Project, and (if
    enabled) Accounting Period/Year via an enumerated list of values
    that overrides the report's "Current Fiscal Period" default.
    """
    filter_value = project_value.replace("'", "''")
    setofbks_value = MAIN_SETOFBKS.replace("'", "''")
 
    clauses = [
        f"PARA_SETOFBKS eq '{setofbks_value}'",
        f"{MAIN_FILTER_FIELD} eq '{filter_value}'",
    ]
 
    if USE_PERIOD_FILTER and PERIOD_LIST:
        period_clause = " or ".join(f"PARA_ACCYEARPER eq {p}" for p in PERIOD_LIST)
        clauses.append(f"({period_clause})")
 
    return " and ".join(clauses)
 
 
def fetch_rows_for_project(project_value: str, top_per_page: int = 1000000) -> List[Dict]:
    """
    Pull rows from the main query, filtered by PARA_SETOFBKS, PARA_PROJECT,
    and the enumerated PARA_ACCYEARPER list.
    """
    base_url = _entity_url(SAP_MAIN_QUERY)
    select = ",".join(MAIN_SELECT_FIELDS)
    params = {
        "$select": select,
        "$top": str(top_per_page),
        "$format": "json",
        "$filter": _build_filter(project_value),
    }
 
    resp = _get_raw(base_url, params)
    if not resp.ok:
        logging.error("HTTP %s for %s params=%s\nBody: %s", resp.status_code, base_url, params, resp.text[:2000])
        resp.raise_for_status()
 
    data = resp.json()
    rows, next_link = _extract_results_and_next(data)
    all_rows = list(rows)
 
    while next_link:
        data2 = _get_json_or_raise(next_link, {})
        rows2, next_link = _extract_results_and_next(data2)
        all_rows.extend(rows2)
 
    return all_rows
 
 
def _parse_byd_date(v: Any) -> Any:
    """
    Convert ByD's '/Date(ms)/' wire format (or a bare epoch int) into a
    pandas Timestamp. Pass anything else through untouched.
    """
    if v is None or v == "":
        return v
    if isinstance(v, str):
        m = _BYD_DATE_RE.search(v)
        if m:
            try:
                return pd.to_datetime(int(m.group(1)), unit="ms")
            except Exception:
                return v
        return v
    if isinstance(v, (int, float)):
        try:
            ms = int(v)
            return pd.to_datetime(ms, unit="ms")
        except Exception:
            return v
    return v
 
 
def _stringify_unhashables(x: Any) -> Any:
    if isinstance(x, (dict, list, set)):
        return str(x)
    return x
 
 
def run_etl() -> pd.DataFrame:
    projects = fetch_distinct_projects()
    total = len(projects)
    all_records: List[Dict] = []
    done = 0
    start = time.time()
 
    logging.info("Fetching %d projects with %d parallel workers...", total, MAX_WORKERS)
 
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_project = {
            executor.submit(fetch_rows_for_project, p): p for p in projects
        }
        for future in as_completed(future_to_project):
            p = future_to_project[future]
            done += 1
            try:
                rows = future.result()
                all_records.extend(rows)
                logging.info("(%d/%d) %s -> %d rows", done, total, p, len(rows))
            except Exception as e:
                logging.exception("(%d/%d) Failed for %s=%s: %s", done, total, MAIN_FILTER_FIELD, p, e)
 
    elapsed = time.time() - start
    logging.info("Fetched %d projects in %.1f sec (%.2f sec/project avg)",
                 total, elapsed, elapsed / max(total, 1))
 
    if not all_records:
        logging.warning("No records fetched.")
        return pd.DataFrame()
 
    df = pd.DataFrame.from_records(all_records)
 
    first = [c for c in MAIN_SELECT_FIELDS if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]
 
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(_parse_byd_date)
 
    df = df.map(_stringify_unhashables).drop_duplicates()
    return df
 
 
def main():
    logging.info("Starting SAP OData ETL...")
    logging.info("Root URL: %s", _root_url())
    logging.info("Codes entity: %s (field=%s)", SAP_CODES_QUERY, CODES_FILTER_FIELD)
    logging.info("Main entity:  %s (filter=%s, set of books=%s)", SAP_MAIN_QUERY, MAIN_FILTER_FIELD, MAIN_SETOFBKS)
    if USE_PERIOD_FILTER and PERIOD_LIST:
        logging.info("Accounting Period range: %d/%d .. %d/%d (%d periods enumerated)",
                     PERIOD_FROM_PERIOD, PERIOD_FROM_YEAR,
                     PERIOD_TO_PERIOD, PERIOD_TO_YEAR, len(PERIOD_LIST))
    else:
        logging.info("Accounting Period filter: disabled")
    logging.info("Parallel workers: %d", MAX_WORKERS)
    logging.info("Output CSV: %s", OUTPUT_CSV)
 
    df = run_etl()
    out_path = OUTPUT_CSV
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logging.info("Wrote %d rows to %s", len(df), out_path)
 
 
if __name__ == "__main__":
    main()
