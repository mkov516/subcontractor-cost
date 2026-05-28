import os
import sys
import time
import logging
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
 
OUTPUT_CSV      = env_or_default("OUTPUT_CSV", "data/subcontractor-cost.csv")
 
SAP_USERNAME    = os.getenv("SAP_USERNAME")
SAP_PASSWORD    = os.getenv("SAP_PASSWORD")
 
REQUEST_PAUSE = float(env_or_default("REQUEST_PAUSE", "0.2"))
 
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})
 
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
    """
    Fetch distinct project IDs from the codes query.
    """
    url = _entity_url(SAP_CODES_QUERY)
    params = {"$select": CODES_FILTER_FIELD, "$top": "1000000", "$format": "json"}
    data = _get_json_or_raise(url, params)
    results, _ = _extract_results_and_next(data)
 
    vals = [r.get(CODES_FILTER_FIELD) for r in results if r.get(CODES_FILTER_FIELD)]
    distinct = sorted(set(vals))
    logging.info("Fetched %d distinct %s values", len(distinct), CODES_FILTER_FIELD)
    return distinct
 
def fetch_rows_for_project(project_value: str, top_per_page: int = 1000000) -> List[Dict]:
    """
    Pull rows from the main query, filtered by PARA_SETOFBKS and PARA_PROJECT.
    """
    base_url = _entity_url(SAP_MAIN_QUERY)
 
    filter_value = project_value.replace("'", "''")
    setofbks_value = MAIN_SETOFBKS.replace("'", "''")
 
    select = ",".join(MAIN_SELECT_FIELDS)
    params = {
        "$select": select,
        "$top": str(top_per_page),
        "$format": "json",
        "$filter": f"PARA_SETOFBKS eq '{setofbks_value}' and {MAIN_FILTER_FIELD} eq '{filter_value}'",
    }
 
    resp = _get_raw(base_url, params)
    if not resp.ok:
        logging.error("HTTP %s for %s params=%s\nBody: %s", resp.status_code, base_url, params, resp.text[:2000])
        resp.raise_for_status()
 
    data = resp.json()
    rows, next_link = _extract_results_and_next(data)
    all_rows = list(rows)
 
    while next_link:
        time.sleep(REQUEST_PAUSE)
        data2 = _get_json_or_raise(next_link, {})
        rows2, next_link = _extract_results_and_next(data2)
        all_rows.extend(rows2)
 
    logging.info("  %s=%s -> %d rows", MAIN_FILTER_FIELD, project_value, len(all_rows))
    return all_rows
 
def _stringify_unhashables(x: Any) -> Any:
    if isinstance(x, (dict, list, set)):
        return str(x)
    return x
 
def run_etl() -> pd.DataFrame:
    projects = fetch_distinct_projects()
    all_records: List[Dict] = []
 
    for i, p in enumerate(projects, start=1):
        logging.info("(%d/%d) Fetching project: %s", i, len(projects), p)
        try:
            all_records.extend(fetch_rows_for_project(p))
        except Exception as e:
            logging.exception("Failed for %s=%s: %s", MAIN_FILTER_FIELD, p, e)
        time.sleep(REQUEST_PAUSE)
 
    if not all_records:
        logging.warning("No records fetched.")
        return pd.DataFrame()
 
    df = pd.DataFrame.from_records(all_records)
 
    # Put selected columns first if present, in the requested order
    first = [c for c in MAIN_SELECT_FIELDS if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]
 
    df = df.map(_stringify_unhashables).drop_duplicates()
    return df
 
def main():
    logging.info("Starting SAP OData ETL...")
    logging.info("Root URL: %s", _root_url())
    logging.info("Codes entity: %s (field=%s)", SAP_CODES_QUERY, CODES_FILTER_FIELD)
    logging.info("Main entity:  %s (filter=%s, set of books=%s)", SAP_MAIN_QUERY, MAIN_FILTER_FIELD, MAIN_SETOFBKS)
    logging.info("Output CSV: %s", OUTPUT_CSV)
 
    df = run_etl()
 
    out_path = OUTPUT_CSV
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logging.info("Wrote %d rows to %s", len(df), out_path)
 
if __name__ == "__main__":
    main()
