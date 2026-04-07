#!/usr/bin/env python3
"""
ETL pipeline: load CMS hospital data CSVs into PostgreSQL.

Handles:
  - "Not Available" / "" / blank → NULL
  - Boolean coercion for emergency_services
  - Numeric coercion for lat/lon and score
  - Derivation of national comparison labels from CMS measure counts
  - Deduplication on provider_id
  - Upsert (ON CONFLICT DO UPDATE) so re-runs are safe
  - --reload flag truncates and reloads from scratch
  - --simulated flag generates 600+ synthetic hospitals (no CSVs needed)

Usage:
    python etl/load_data.py               # incremental upsert
    python etl/load_data.py --reload      # truncate and reload
    python etl/load_data.py --simulated   # generate synthetic data
"""

import argparse
import logging
import os
import random
import string
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"

# CMS API uses "" or "Not Available" for missing values
CMS_NULL_VALUES = {"Not Available", "N/A", "NA", "", " ", "–", "-", "None", "Not Applicable"}

# ── Column mapping: CMS API snake_case → our schema ──────────────────────────
#
# The CMS DKAN API (data.cms.gov) uses snake_case field names.
# National comparison fields were removed from the API payload in 2024;
# we derive them in _derive_national_comparisons() from the measure count
# columns that are still present.

HOSPITAL_COL_MAP = {
    "facility_id":                      "provider_id",
    "facility_name":                    "name",
    "address":                          "address",
    "citytown":                         "city",
    "state":                            "state",
    "zip_code":                         "zip",
    "countyparish":                     "county",
    "telephone_number":                 "phone",
    "hospital_type":                    "hospital_type",
    "hospital_ownership":               "ownership",
    "emergency_services":               "emergency_services",
    "hospital_overall_rating":          "overall_rating",
    "hospital_overall_rating_footnote": "rating_footnote",
    # Count columns used to DERIVE national comparison labels
    "count_of_mort_measures_better":    "_mort_better",
    "count_of_mort_measures_worse":     "_mort_worse",
    "count_of_facility_mort_measures":  "_mort_total",
    "count_of_safety_measures_better":  "_safety_better",
    "count_of_safety_measures_worse":   "_safety_worse",
    "count_of_facility_safety_measures":"_safety_total",
    "count_of_readm_measures_better":   "_readm_better",
    "count_of_readm_measures_worse":    "_readm_worse",
    "count_of_facility_readm_measures": "_readm_total",
    "count_of_facility_pt_exp_measures":"_pt_exp_total",
    "count_of_facility_te_measures":    "_te_total",
    # Lat/lon: not in current API payload but map if present
    "lat":  "lat",
    "lon":  "lon",
    "latitude":  "lat",
    "longitude": "lon",
}

TIMELY_COL_MAP = {
    "facility_id":  "provider_id",
    "measure_id":   "measure_id",
    "measure_name": "measure_name",
    "score":        "score",
    "footnote":     "footnote",
    "start_date":   "start_date",
    "end_date":     "end_date",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set. Copy .env.example to .env.")
        sys.exit(1)
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(db_url, echo=False, pool_pre_ping=True)


def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace CMS null-sentinel strings with actual NaN."""
    return df.replace(list(CMS_NULL_VALUES), pd.NA)


def coerce_bool(series: pd.Series) -> pd.Series:
    mapping = {"yes": True, "true": True, "no": False, "false": False, "y": True, "n": False}
    return series.str.strip().str.lower().map(mapping)


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _derive_comparison(better: pd.Series, worse: pd.Series, total: pd.Series) -> pd.Series:
    """
    Derive CMS national comparison label from measure count columns.

    Logic (mirrors CMS's own methodology):
      - If no facility measures scored: NULL (insufficient data)
      - If better > worse: "Above the national average"
      - If worse > better: "Below the national average"
      - Otherwise: "Same as the national average"
    """
    result = pd.Series(index=better.index, dtype="object")
    result[:] = pd.NA

    has_data = coerce_numeric(total) > 0
    b = coerce_numeric(better).fillna(0)
    w = coerce_numeric(worse).fillna(0)

    result[has_data & (b > w)] = "Above the national average"
    result[has_data & (w > b)] = "Below the national average"
    result[has_data & (b == w)] = "Same as the national average"

    return result


def _derive_national_comparisons(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a NEW DataFrame with derived national comparison columns added.

    The CMS DKAN API no longer exposes pre-computed comparison labels.
    We reconstruct them from the measure count columns:
      count_of_*_measures_better / worse / total
    """
    def safe(name: str) -> pd.Series:
        return df.get(name, pd.Series(pd.NA, index=df.index, dtype="object"))

    pt_exp = coerce_numeric(safe("_pt_exp_total"))
    te     = coerce_numeric(safe("_te_total"))

    def _same_when_measured(total_series: pd.Series) -> list:
        return [
            "Same as the national average" if bool(v) else pd.NA
            for v in (total_series > 0)
        ]

    new_cols: dict = {
        "mortality_national_comparison": _derive_comparison(
            safe("_mort_better"), safe("_mort_worse"), safe("_mort_total")
        ),
        "safety_national_comparison": _derive_comparison(
            safe("_safety_better"), safe("_safety_worse"), safe("_safety_total")
        ),
        "readmission_national_comparison": _derive_comparison(
            safe("_readm_better"), safe("_readm_worse"), safe("_readm_total")
        ),
        # Patient experience: only total count available, no better/worse breakdown
        "patient_experience_national_comparison": pd.Series(
            _same_when_measured(pt_exp), index=df.index, dtype="object"
        ),
        # Effectiveness (timely/effective care group)
        "effectiveness_national_comparison": pd.Series(
            _same_when_measured(te), index=df.index, dtype="object"
        ),
        # Timeliness / efficient use — not available in current API payload
        "timeliness_national_comparison":  pd.Series(pd.NA, index=df.index, dtype="object"),
        "efficient_use_national_comparison": pd.Series(pd.NA, index=df.index, dtype="object"),
    }

    # Drop temp helper columns, add derived columns — pd.DataFrame.assign returns a copy
    tmp_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(columns=tmp_cols).assign(**new_cols).copy()


# ── Hospital loading ──────────────────────────────────────────────────────────

def load_hospitals(engine, reload: bool = False) -> int:
    csv_path = DATA_DIR / "Hospital_General_Information.csv"
    if not csv_path.exists():
        logger.error(f"File not found: {csv_path}\nRun: python data/download_data.py")
        return 0

    logger.info(f"Reading {csv_path.name} ...")
    raw = pd.read_csv(csv_path, dtype=str, low_memory=False)
    logger.info(f"  Raw rows: {len(raw):,}")

    # Map columns we care about (ignore extras gracefully)
    available = {k: v for k, v in HOSPITAL_COL_MAP.items() if k in raw.columns}
    missing = set(HOSPITAL_COL_MAP) - set(available)
    if missing:
        logger.debug(f"  Columns not in CSV (will be NULL/derived): {missing}")

    df = raw[list(available.keys())].rename(columns=available).copy()
    df = normalize_nulls(df)

    # Derive national comparison fields from measure count columns
    df = _derive_national_comparisons(df)

    # Type coercions — use .assign() to avoid pandas CoW chained-assignment warnings
    df = df.assign(
        emergency_services=coerce_bool(df["emergency_services"]),
        overall_rating=coerce_numeric(df.get("overall_rating", pd.Series(dtype=str))),
        lat=coerce_numeric(df["lat"] if "lat" in df.columns else pd.Series(pd.NA, index=df.index)),
        lon=coerce_numeric(df["lon"] if "lon" in df.columns else pd.Series(pd.NA, index=df.index)),
    )

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["provider_id"])
    df = df[df["provider_id"].notna()]
    dupes = before - len(df)
    if dupes:
        logger.info(f"  Dropped {dupes:,} duplicate/null provider_ids")

    # Log comparison derivation quality
    for field in ["mortality_national_comparison", "readmission_national_comparison"]:
        n_derived = df[field].notna().sum()
        logger.info(f"  Derived {field}: {n_derived:,}/{len(df):,} rows have values")

    loaded = _upsert_hospitals(engine, df, reload)
    logger.info(f"  Hospitals loaded/updated: {loaded:,}")
    return loaded


def _upsert_hospitals(engine, df: pd.DataFrame, reload: bool) -> int:
    # Ensure all schema columns are present
    schema_cols = [
        "provider_id", "name", "address", "city", "state", "zip", "county", "phone",
        "hospital_type", "ownership", "emergency_services", "overall_rating",
        "rating_footnote",
        "mortality_national_comparison", "safety_national_comparison",
        "readmission_national_comparison", "patient_experience_national_comparison",
        "effectiveness_national_comparison", "timeliness_national_comparison",
        "efficient_use_national_comparison", "lat", "lon",
    ]
    for col in schema_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[schema_cols].copy()

    with engine.begin() as conn:
        if reload:
            conn.execute(text("TRUNCATE timely_care, hospitals RESTART IDENTITY CASCADE"))
            logger.info("  Truncated hospitals + timely_care tables")

        df.to_sql("__tmp_hospitals", engine, if_exists="replace", index=False, method="multi")

        result = conn.execute(text("""
            INSERT INTO hospitals
            SELECT
                provider_id, name, address, city, state, zip, county, phone,
                hospital_type, ownership, emergency_services,
                overall_rating::SMALLINT, rating_footnote,
                mortality_national_comparison, safety_national_comparison,
                readmission_national_comparison, patient_experience_national_comparison,
                effectiveness_national_comparison, timeliness_national_comparison,
                efficient_use_national_comparison,
                lat, lon, NOW()
            FROM __tmp_hospitals
            ON CONFLICT (provider_id) DO UPDATE SET
                name                                    = EXCLUDED.name,
                address                                 = EXCLUDED.address,
                city                                    = EXCLUDED.city,
                state                                   = EXCLUDED.state,
                zip                                     = EXCLUDED.zip,
                county                                  = EXCLUDED.county,
                phone                                   = EXCLUDED.phone,
                hospital_type                           = EXCLUDED.hospital_type,
                ownership                               = EXCLUDED.ownership,
                emergency_services                      = EXCLUDED.emergency_services,
                overall_rating                          = EXCLUDED.overall_rating,
                rating_footnote                         = EXCLUDED.rating_footnote,
                mortality_national_comparison           = EXCLUDED.mortality_national_comparison,
                safety_national_comparison              = EXCLUDED.safety_national_comparison,
                readmission_national_comparison         = EXCLUDED.readmission_national_comparison,
                patient_experience_national_comparison  = EXCLUDED.patient_experience_national_comparison,
                effectiveness_national_comparison       = EXCLUDED.effectiveness_national_comparison,
                timeliness_national_comparison          = EXCLUDED.timeliness_national_comparison,
                efficient_use_national_comparison       = EXCLUDED.efficient_use_national_comparison,
                lat                                     = EXCLUDED.lat,
                lon                                     = EXCLUDED.lon
        """))
        conn.execute(text("DROP TABLE IF EXISTS __tmp_hospitals"))
        return result.rowcount


# ── Timely care loading ───────────────────────────────────────────────────────

def load_timely_care(engine, reload: bool = False) -> int:
    csv_path = DATA_DIR / "Timely_and_Effective_Care-Hospital.csv"
    if not csv_path.exists():
        logger.warning(f"File not found: {csv_path} — skipping timely_care load.")
        return 0

    logger.info(f"Reading {csv_path.name} ...")
    raw = pd.read_csv(csv_path, dtype=str, low_memory=False)
    logger.info(f"  Raw rows: {len(raw):,}")

    available = {k: v for k, v in TIMELY_COL_MAP.items() if k in raw.columns}
    df = raw[list(available.keys())].rename(columns=available).copy()
    df = normalize_nulls(df)

    df["score"] = coerce_numeric(df.get("score", pd.Series(dtype=str)))
    df["start_date"] = coerce_date(df.get("start_date", pd.Series(dtype=str)))
    df["end_date"] = coerce_date(df.get("end_date", pd.Series(dtype=str)))

    # Filter to provider_ids that exist in hospitals (FK constraint)
    with engine.connect() as conn:
        existing_ids = set(
            r[0] for r in conn.execute(text("SELECT provider_id FROM hospitals"))
        )

    before = len(df)
    df = df[df["provider_id"].isin(existing_ids)]
    skipped = before - len(df)
    if skipped:
        logger.info(f"  Skipped {skipped:,} rows — provider_id not in hospitals")

    if reload:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE timely_care RESTART IDENTITY"))

    total = 0
    chunk_size = 5000
    for i in range(0, len(df), chunk_size):
        df.iloc[i: i + chunk_size].to_sql(
            "timely_care", engine, if_exists="append", index=False, method="multi"
        )
        total += min(chunk_size, len(df) - i)

    logger.info(f"  Timely care rows loaded: {total:,}")
    return total


# ── Synthetic data fallback ───────────────────────────────────────────────────

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY",
]
HOSPITAL_TYPES = [
    "Acute Care Hospitals", "Critical Access Hospitals",
    "Childrens", "Psychiatric", "Long-Term Care",
]
OWNERSHIP_TYPES = [
    "Voluntary non-profit - Private", "Voluntary non-profit - Church",
    "Government - State", "Government - Local",
    "Proprietary", "Government - Federal",
]
COMPARISON_VALUES = [
    "Above the national average",
    "Same as the national average",
    "Below the national average",
]
MEASURE_IDS = [
    ("ED_1b",              "Emergency dept median time from ED arrival to departure (admitted)"),
    ("ED_2b",              "Emergency dept admit decision to departure time"),
    ("OP_18b",             "Median time from ED arrival to departure for discharged patients"),
    ("OP_22",              "Patient left without being seen"),
    ("READM_30_HOSP_WIDE", "Hospital-wide all-cause unplanned readmission rate"),
    ("HF_READM",           "Heart failure readmission rate"),
    ("AMI_READM",          "Acute myocardial infarction readmission rate"),
    ("PN_READM",           "Pneumonia readmission rate"),
    ("COMP_HIP_KNEE",      "Complication rate for hip/knee replacement"),
    ("OP_23",              "Head CT results"),
]


def generate_simulated_data(engine, n_hospitals: int = 600) -> tuple[int, int]:
    """Generate realistic synthetic hospital data and load it."""
    logger.info(f"Generating {n_hospitals} simulated hospitals ...")
    random.seed(42)

    state_weights = [1] * 50
    high_pop = {i: w for i, (s, w) in enumerate(zip(STATES, [1]*50)) if s in
                {"CA":8,"TX":7,"FL":6,"NY":6,"PA":4,"OH":4,"IL":4}.keys()}
    for i, s in enumerate(STATES):
        if s in {"CA","TX","FL","NY","PA","OH","IL"}:
            state_weights[i] = {"CA":8,"TX":7,"FL":6,"NY":6,"PA":4,"OH":4,"IL":4}[s]
    total_w = sum(state_weights)
    state_weights = [w / total_w for w in state_weights]

    type_weights  = [0.60, 0.25, 0.05, 0.05, 0.05]
    owner_weights = [0.35, 0.15, 0.10, 0.15, 0.20, 0.05]

    def cmp(r):
        if r >= 4:
            return random.choices(COMPARISON_VALUES, weights=[0.50, 0.35, 0.15])[0]
        elif r == 3:
            return random.choices(COMPARISON_VALUES, weights=[0.20, 0.60, 0.20])[0]
        else:
            return random.choices(COMPARISON_VALUES, weights=[0.10, 0.35, 0.55])[0]

    rows, seen = [], set()
    for idx in range(n_hospitals):
        pid = "".join(random.choices(string.digits, k=6))
        while pid in seen:
            pid = "".join(random.choices(string.digits, k=6))
        seen.add(pid)

        state    = random.choices(STATES, weights=state_weights)[0]
        h_type   = random.choices(HOSPITAL_TYPES, weights=type_weights)[0]
        ownership = random.choices(OWNERSHIP_TYPES, weights=owner_weights)[0]
        rating    = random.choices([1,2,3,4,5], weights=[0.10,0.20,0.35,0.25,0.10])[0]

        rows.append({
            "provider_id": pid,
            "name": f"{state} Regional Medical Center #{idx+1}",
            "address": f"{random.randint(100,9999)} Healthcare Blvd",
            "city": f"City_{state}_{random.randint(1,20)}",
            "state": state,
            "zip": f"{random.randint(10000,99999)}",
            "county": f"County_{random.randint(1,50)}",
            "phone": "".join(random.choices(string.digits, k=10)),
            "hospital_type": h_type,
            "ownership": ownership,
            "emergency_services": random.random() > 0.15,
            "overall_rating": rating,
            "rating_footnote": None,
            "mortality_national_comparison":             cmp(rating),
            "safety_national_comparison":               cmp(rating),
            "readmission_national_comparison":          cmp(rating),
            "patient_experience_national_comparison":   cmp(rating),
            "effectiveness_national_comparison":        cmp(rating),
            "timeliness_national_comparison":           cmp(rating),
            "efficient_use_national_comparison":        cmp(rating),
            "lat": round(random.uniform(25.0, 48.0), 6),
            "lon": round(random.uniform(-124.0, -67.0), 6),
        })

    h_df = pd.DataFrame(rows)
    h_loaded = _upsert_hospitals(engine, h_df, reload=True)
    logger.info(f"  Simulated hospitals inserted: {h_loaded:,}")

    # Timely care
    logger.info("Generating simulated timely care records ...")
    tc_rows = []
    start, end = date(2022, 10, 1), date(2023, 6, 30)
    for pid in h_df["provider_id"]:
        for mid, mname in MEASURE_IDS:
            score = round(random.uniform(5, 350) if "time" in mname.lower()
                          else random.uniform(0.5, 25.0), 2)
            tc_rows.append({
                "provider_id": pid, "measure_id": mid, "measure_name": mname,
                "score": score, "footnote": None,
                "start_date": start, "end_date": end,
            })

    tc_df = pd.DataFrame(tc_rows)
    tc_total = 0
    for i in range(0, len(tc_df), 5000):
        tc_df.iloc[i:i+5000].to_sql(
            "timely_care", engine, if_exists="append", index=False, method="multi"
        )
        tc_total += min(5000, len(tc_df) - i)

    logger.info(f"  Simulated timely care rows: {tc_total:,}")
    return h_loaded, tc_total


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Load CMS hospital data into PostgreSQL")
    p.add_argument("--reload", action="store_true",
                   help="Truncate tables and reload from scratch")
    p.add_argument("--simulated", action="store_true",
                   help="Generate 600 synthetic hospitals (no CSVs needed)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    engine = get_engine()

    if args.simulated:
        logger.info("=== Simulated mode — generating synthetic dataset ===")
        generate_simulated_data(engine)
        logger.info("Done. Note in any public presentation: data is synthetic.")
        return 0

    logger.info("=== Loading CMS Hospital General Information ===")
    h_count = load_hospitals(engine, reload=args.reload)

    logger.info("=== Loading CMS Timely and Effective Care ===")
    tc_count = load_timely_care(engine, reload=args.reload)

    logger.info(f"\n{'='*50}")
    logger.info(f"  Hospitals loaded:       {h_count:>8,}")
    logger.info(f"  Timely care rows:       {tc_count:>8,}")
    logger.info(f"{'='*50}")

    if h_count == 0:
        logger.error(
            "No hospitals loaded. Run  python data/download_data.py  first,\n"
            "or use  --simulated  to generate synthetic data."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
