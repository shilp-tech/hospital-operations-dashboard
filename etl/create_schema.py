#!/usr/bin/env python3
"""
Idempotent schema creation for the hospital-operations-dashboard.

Run this before load_data.py. Safe to re-run: uses CREATE TABLE IF NOT EXISTS
and CREATE INDEX IF NOT EXISTS so it never destroys existing data.

Usage:
    python etl/create_schema.py
"""

import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL — keep in one place so it's easy to read and version-control
# ---------------------------------------------------------------------------

DDL = """
-- ============================================================
-- hospitals
-- Core hospital reference data from CMS Hospital General Info
-- ============================================================
CREATE TABLE IF NOT EXISTS hospitals (
    provider_id                         VARCHAR(10)  PRIMARY KEY,
    name                                TEXT         NOT NULL,
    address                             TEXT,
    city                                TEXT,
    state                               CHAR(2),
    zip                                 VARCHAR(10),
    county                              TEXT,
    phone                               VARCHAR(20),
    hospital_type                       TEXT,
    ownership                           TEXT,
    emergency_services                  BOOLEAN,
    overall_rating                      SMALLINT,       -- 1-5 stars, NULL if not rated
    rating_footnote                     TEXT,

    -- National comparison categorical fields (CMS values:
    --   "Above the national average" / "Same as the national average" /
    --   "Below the national average" / "Not Available")
    mortality_national_comparison               TEXT,
    safety_national_comparison                  TEXT,
    readmission_national_comparison             TEXT,
    patient_experience_national_comparison      TEXT,
    effectiveness_national_comparison           TEXT,
    timeliness_national_comparison              TEXT,
    efficient_use_national_comparison           TEXT,

    lat                                 NUMERIC(9, 6),
    lon                                 NUMERIC(9, 6),

    created_at                          TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- timely_care
-- Measure-level performance data from CMS Timely & Effective Care
-- ============================================================
CREATE TABLE IF NOT EXISTS timely_care (
    id                  SERIAL       PRIMARY KEY,
    provider_id         VARCHAR(10)  NOT NULL
                            REFERENCES hospitals(provider_id)
                            ON DELETE CASCADE,
    measure_id          VARCHAR(50),
    measure_name        TEXT,
    score               NUMERIC(10, 2),
    footnote            TEXT,
    start_date          DATE,
    end_date            DATE
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_hospitals_state
    ON hospitals(state);

CREATE INDEX IF NOT EXISTS idx_hospitals_rating
    ON hospitals(overall_rating);

CREATE INDEX IF NOT EXISTS idx_hospitals_type
    ON hospitals(hospital_type);

CREATE INDEX IF NOT EXISTS idx_hospitals_ownership
    ON hospitals(ownership);

CREATE INDEX IF NOT EXISTS idx_timely_care_provider
    ON timely_care(provider_id);

CREATE INDEX IF NOT EXISTS idx_timely_care_measure
    ON timely_care(measure_id);
"""


def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error(
            "DATABASE_URL not set.\n"
            "Copy .env.example to .env and fill in your PostgreSQL credentials."
        )
        sys.exit(1)
    # SQLAlchemy 2.x requires postgresql+psycopg2:// scheme
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(db_url, echo=False)


def main() -> int:
    engine = get_engine()
    logger.info(f"Connected to database.")

    with engine.begin() as conn:
        # Execute each statement individually for clearer error messages
        statements = [s.strip() for s in DDL.split(";") if s.strip()]
        for stmt in statements:
            conn.execute(text(stmt))
            # Log the object name from the statement
            first_line = stmt.split("\n")[0].strip()
            if first_line and not first_line.startswith("--"):
                logger.info(f"  OK  {first_line[:80]}")

    logger.info("Schema creation complete (idempotent — safe to re-run).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
