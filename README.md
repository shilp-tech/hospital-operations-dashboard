# Hospital Operations Dashboard

> A production-quality healthcare data engineering and reporting project demonstrating ETL pipelines, analytical SQL, and an interactive Plotly Dash dashboard — built on real CMS Hospital Compare data.

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Plotly Dash](https://img.shields.io/badge/Plotly_Dash-2.17-3D9BE9?logo=plotly&logoColor=white)](https://dash.plotly.com)
[![Deploy on Railway](https://img.shields.io/badge/Deploy-Railway-8B5CF6?logo=railway)](https://railway.app)

---

## Screenshot

> *Replace this section with a screenshot of your live dashboard after deployment.*

```
┌─────────────────────────────────────────────────────────────────────┐
│  🏥  Hospital Operations Dashboard                    LIVE DATA      │
│  CMS Hospital Compare · Quality & Performance Analytics              │
├──────────┬──────────────────────────────────────────────────────────┤
│ Filters  │  [Total]  [Avg ★]  [High Perf %]  [ER %]  [Rated %]     │
│ ──────── │                                                           │
│ State    │  ┌─ Top 10 States ──────┐  ┌─ Rating Distribution ─┐    │
│ Type     │  │  Bar chart           │  │  Donut chart           │    │
│ Rating   │  └──────────────────────┘  └───────────────────────┘    │
│ Ownership│  ┌─ Hospital Type ──────┐  ┌─ Ownership Type ──────┐    │
│          │  │  Bar chart           │  │  Horizontal bar        │    │
│          │  └──────────────────────┘  └───────────────────────┘    │
│          │  ┌─ Patient Exp vs Readmission Scatter ─────────────┐    │
│          │  │  Scatter plot colored by star rating             │    │
│          │  └──────────────────────────────────────────────────┘    │
│          │  ┌─ Sortable Hospital Table ──────────────────────── ┐   │
└──────────┴──────────────────────────────────────────────────────────┘
```

---

## Project Overview

This project demonstrates the full data lifecycle that a **Healthcare Informaticist / Report Writer** performs daily:

| Stage | What it demonstrates |
|-------|---------------------|
| **Data Acquisition** | Automated download of real CMS public datasets |
| **ETL Pipeline** | Cleaning CMS's notoriously messy data ("Not Available" → NULL, dedup, type coercion) |
| **Schema Design** | Normalized PostgreSQL schema with FK constraints and indexes |
| **Analytical SQL** | 6 production-grade report queries using window functions, CTEs, CASE WHEN, HAVING |
| **Visualization** | Dark-themed Plotly Dash dashboard with live filters and callbacks |
| **Deployment** | Docker + Railway with environment-variable-driven configuration |

---

## Data Source

**CMS Hospital Compare** — U.S. Centers for Medicare & Medicaid Services

| Dataset | Description | Records |
|---------|-------------|---------|
| [Hospital General Information](https://data.cms.gov/provider-data/topics/hospitals) | Star ratings, ownership, location, national comparisons | ~5,000 hospitals |
| [Timely and Effective Care](https://data.cms.gov/provider-data/topics/hospitals) | Measure-level performance scores (ED throughput, readmissions, etc.) | ~200,000 rows |

> **Data License:** CMS public datasets are open to the public under [data.cms.gov terms](https://data.cms.gov/about).
>
> If the automated download fails (URLs change quarterly), run `python etl/load_data.py --simulated` to generate 600+ synthetic hospitals with realistic distributions. This is documented clearly in the app footer.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Language | Python 3.11 | ETL, dashboard backend |
| Database | PostgreSQL 15 | OLAP-style relational store |
| ORM / Connection | SQLAlchemy 2.0 | DB abstraction, connection pooling |
| Data Processing | pandas 2.2, numpy | Cleaning, transformation |
| Dashboard | Plotly Dash 2.17 | Interactive web app |
| UI Components | Dash Bootstrap Components | Responsive dark-theme layout |
| Charts | Plotly 5.22 | Bar, donut, scatter visualizations |
| Web Server | Gunicorn 22 | Production WSGI server |
| Containerization | Docker (python:3.11-slim) | Reproducible deployment |
| Hosting | Railway | Cloud PaaS with managed PostgreSQL |
| Config | python-dotenv | Secret management |

---

## Project Structure

```
hospital-operations-dashboard/
│
├── data/
│   └── download_data.py          # Downloads CMS CSVs (with fallback URLs)
│
├── etl/
│   ├── create_schema.py          # Idempotent DDL — CREATE TABLE IF NOT EXISTS
│   └── load_data.py              # Cleans + upserts CSVs → PostgreSQL
│
├── sql/                          # Analytical report queries
│   ├── 01_revenue_by_state.sql   # Hospital distribution & avg rating by state
│   ├── 02_readmission_analysis.sql  # Readmission outlier detection
│   ├── 03_rating_distribution.sql   # Rating frequency + cumulative %
│   ├── 04_top_hospitals_by_volume.sql  # Window-function ranking per state
│   ├── 05_ownership_performance.sql    # Non-profit vs. for-profit analysis
│   └── 06_quality_scorecard.sql        # Composite weighted quality score
│
├── app/
│   ├── app.py                    # Dash app init, custom CSS, WSGI server
│   ├── layouts.py                # Declarative UI layout (KPI cards, charts, table)
│   └── callbacks.py              # All interactivity: filters → chart updates
│
├── .env.example                  # Template for environment variables
├── .gitignore                    # Excludes .env, CSVs, __pycache__
├── requirements.txt              # Pinned Python dependencies
├── Dockerfile                    # python:3.11-slim, Gunicorn CMD
├── railway.toml                  # Build + deploy config for Railway
└── README.md
```

---

## Database Schema

```
┌───────────────────────────────────────┐
│              hospitals                │
├──────────────────────┬────────────────┤
│ provider_id  VARCHAR  PK              │
│ name         TEXT                     │
│ address      TEXT                     │
│ city         TEXT                     │
│ state        CHAR(2)                  │
│ zip          VARCHAR(10)              │
│ county       TEXT                     │
│ phone        VARCHAR(20)              │
│ hospital_type TEXT                    │
│ ownership    TEXT                     │
│ emergency_services  BOOLEAN           │
│ overall_rating      SMALLINT  (1-5)   │
│ rating_footnote     TEXT               │
│ mortality_national_comparison   TEXT  │
│ safety_national_comparison      TEXT  │
│ readmission_national_comparison TEXT  │
│ patient_experience_nat_comp     TEXT  │
│ effectiveness_national_comp     TEXT  │
│ timeliness_national_comparison  TEXT  │
│ efficient_use_national_comp     TEXT  │
│ lat          NUMERIC(9,6)             │
│ lon          NUMERIC(9,6)             │
│ created_at   TIMESTAMPTZ              │
└──────────────────┬───────────────────┘
                   │ 1:N
                   ▼
┌───────────────────────────────────────┐
│             timely_care               │
├──────────────────────┬────────────────┤
│ id           SERIAL   PK              │
│ provider_id  VARCHAR  FK→hospitals    │
│ measure_id   VARCHAR(50)              │
│ measure_name TEXT                     │
│ score        NUMERIC(10,2)            │
│ footnote     TEXT                     │
│ start_date   DATE                     │
│ end_date     DATE                     │
└───────────────────────────────────────┘

Indexes: state, overall_rating, hospital_type, ownership, provider_id (timely_care)
```

**CMS Comparison Field Values:**
- `"Above the national average"` — hospital performs better than peers
- `"Same as the national average"` — within expected range
- `"Below the national average"` — flagged for quality improvement
- `NULL` — insufficient data to compare

---

## SQL Report Queries

Each query is written the way a production report writer would — with CTEs, window functions, comments explaining business intent, and handling for CMS's missing-data patterns.

| File | Business Question | Key Techniques |
|------|------------------|----------------|
| `01_revenue_by_state.sql` | Which states have the most hospitals and highest quality? | `GROUP BY`, `RANK()`, `SUM() OVER()`, high-performer rate |
| `02_readmission_analysis.sql` | Which hospitals have worse-than-average readmissions? | `LEFT JOIN`, `CROSS JOIN`, `PERCENT_RANK()`, deviation flagging |
| `03_rating_distribution.sql` | What does the national rating distribution look like? | `GENERATE_SERIES`, `FILTER`, cumulative `SUM() OVER()`, `MODE()` |
| `04_top_hospitals_by_volume.sql` | Who are the top 3 hospitals in each state? | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE()`, `LAG()` |
| `05_ownership_performance.sql` | Do non-profits outperform for-profits? | `GROUP BY` + `HAVING`, `STDDEV()`, composite weighted score |
| `06_quality_scorecard.sql` | What is each hospital's overall quality grade? | `CASE WHEN` scoring, normalized weighted avg, letter grades, `PERCENT_RANK()` |

---

## Local Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 15+ running locally
- `git`

### Step 1 — Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/hospital-operations-dashboard.git
cd hospital-operations-dashboard

# Set up environment
cp .env.example .env
# Edit .env: set DATABASE_URL to your local PostgreSQL connection string
```

**`.env` example:**
```
DATABASE_URL=postgresql://postgres:mypassword@localhost:5432/hospital_dashboard
DASH_DEBUG=true
```

### Step 2 — Create the database

```bash
# In psql (or pgAdmin), create the database:
psql -U postgres -c "CREATE DATABASE hospital_dashboard;"
```

### Step 3 — Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Step 4 — Create the schema

```bash
python etl/create_schema.py
# Output: "Schema creation complete (idempotent — safe to re-run)."
```

### Step 5 — Download and load data

```bash
# Option A: Real CMS data (recommended)
python data/download_data.py     # ~100MB download
python etl/load_data.py          # loads ~5,000 hospitals + ~200,000 timely care rows

# Option B: Synthetic data (if CMS URLs are unavailable or for quick testing)
python etl/load_data.py --simulated    # generates 600 realistic hospitals
```

### Step 6 — Run the dashboard

```bash
python app/app.py
# Open: http://localhost:8050
```

**Full reload (truncate + reload from scratch):**
```bash
python etl/load_data.py --reload
```

---

## Railway Deployment

Railway provides managed PostgreSQL and automatic deployments from GitHub.

### Step 1 — Create Railway project

1. Go to [railway.app](https://railway.app) and create a new project
2. Click **Add Service → GitHub Repo** and connect this repository
3. Railway will detect the `Dockerfile` automatically

### Step 2 — Add PostgreSQL

1. In your Railway project, click **New → Database → PostgreSQL**
2. Railway creates a managed PostgreSQL instance
3. Copy the **`DATABASE_URL`** from the PostgreSQL service's Variables tab

### Step 3 — Set environment variables

In your Railway app service → **Variables**, add:

```
DATABASE_URL=postgresql://...  (paste from PostgreSQL service)
```

Railway sets `PORT` automatically — do not set it manually.

### Step 4 — Run the ETL (one-time)

Use Railway's **Shell** (or a Railway one-off command) to initialize the database:

```bash
# In Railway Shell for your app service:
python etl/create_schema.py
python data/download_data.py
python etl/load_data.py

# Or with synthetic data:
python etl/load_data.py --simulated
```

Alternatively, trigger it via a temporary `CMD` override in Railway settings, then revert.

### Step 5 — Deploy

Railway auto-deploys on every push to `main`. Force a manual deploy from the Railway dashboard if needed.

**Your dashboard will be live at:** `https://your-project.railway.app`

---

## Running with Docker (optional local test)

```bash
# Build
docker build -t hospital-dashboard .

# Run (pass your DATABASE_URL)
docker run -p 8050:8050 \
  -e DATABASE_URL="postgresql://postgres:pass@host.docker.internal:5432/hospital_dashboard" \
  hospital-dashboard

# Open: http://localhost:8050
```

---

## Dashboard Features

### KPI Cards
| Card | Metric |
|------|--------|
| Total Hospitals | Count of hospitals matching current filters |
| Avg CMS Star Rating | Mean overall_rating of rated hospitals |
| High Performers (4-5★) | % of rated hospitals with 4+ stars |
| With Emergency Services | % of hospitals offering emergency care |
| Hospitals Rated | % with a CMS rating (vs. unrated) |
| States Covered | Distinct state count in filtered set |

### Charts
| Chart | Type | Description |
|-------|------|-------------|
| Top 10 States | Horizontal bar | Hospital count by state, color-scaled |
| Rating Distribution | Donut | 1-5 star breakdown with percentages |
| Hospital Type | Vertical bar | Type breakdown colored by avg rating |
| Ownership Distribution | Horizontal bar | Non-profit vs. government vs. for-profit |
| Patient Exp vs. Readmission | Scatter | Two CMS metrics plotted, colored by star rating |
| Hospital Detail Table | Sortable/filterable | Top 200 hospitals with all key columns |

### Filters
- **State** — multi-select dropdown
- **Hospital Type** — multi-select dropdown
- **Star Rating** — range slider (1-5)
- **Ownership Type** — multi-select dropdown
- **Reset** — clears all filters in one click

All charts update in real-time as filters change via Dash callbacks.

---

## Author

**Shilp Patel**
M.S. Advanced Data Analytics — University of North Texas

> Built as a portfolio project demonstrating healthcare data engineering, SQL report writing, and interactive dashboard development — the core skills for a Healthcare Informaticist / Report Writer role.

---

## License

MIT — free to use, adapt, and share with attribution.
