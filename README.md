# Hospital Operations Dashboard

> A production-quality healthcare data engineering and reporting project demonstrating ETL pipelines, analytical SQL, and an interactive Plotly Dash dashboard — built on real CMS Hospital Compare data.

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Plotly Dash](https://img.shields.io/badge/Plotly_Dash-2.17-3D9BE9?logo=plotly&logoColor=white)](https://dash.plotly.com)
[![Supabase](https://img.shields.io/badge/Database-Supabase-3ECF8E?logo=supabase&logoColor=white)](https://supabase.com)
[![Deploy on Railway](https://img.shields.io/badge/Deploy-Railway-8B5CF6?logo=railway)](https://railway.app)

**🔴 Live Demo:** [https://hospital-operations-dashboard-production.up.railway.app](https://hospital-operations-dashboard-production.up.railway.app)

**GitHub:** [github.com/shilp-tech/hospital-operations-dashboard](https://github.com/shilp-tech/hospital-operations-dashboard)

---

## Screenshot

> *Add a screenshot of the live dashboard here.*

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
| **Data Acquisition** | Automated paginated download of real CMS public datasets via API |
| **ETL Pipeline** | Cleaning CMS's notoriously messy data ("Not Available" → NULL, dedup, type coercion, derived fields) |
| **Schema Design** | Normalized PostgreSQL schema with FK constraints and indexes |
| **Analytical SQL** | 6 production-grade report queries using window functions, CTEs, CASE WHEN, HAVING |
| **Visualization** | Dark-themed Plotly Dash dashboard with live filters and callbacks |
| **Deployment** | Docker + Railway (app) + Supabase (managed PostgreSQL), fully cloud-hosted |

---

## Data Source

**CMS Hospital Compare** — U.S. Centers for Medicare & Medicaid Services

| Dataset | Description | Records |
|---------|-------------|---------|
| [Hospital General Information](https://data.cms.gov/provider-data/topics/hospitals) | Star ratings, ownership, location, national comparisons | 5,426 hospitals |
| [Timely and Effective Care](https://data.cms.gov/provider-data/topics/hospitals) | Measure-level performance scores (ED throughput, readmissions, etc.) | 138,129 rows |

> **Data License:** CMS public datasets are open to the public under [data.cms.gov terms](https://data.cms.gov/about).
>
> **API note:** The CMS DKAN API enforces a 1,500-row page limit. The downloader handles full pagination automatically. National comparison labels were removed from the API payload in 2024 and are re-derived from measure count fields (better/same/worse counts) during ETL.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Language | Python 3.11 | ETL, dashboard backend |
| Database | PostgreSQL 15 | OLAP-style relational store |
| Database hosting | **Supabase** (free tier, us-east-1) | Managed PostgreSQL, no infrastructure to run |
| ORM / Connection | SQLAlchemy 2.0 | DB abstraction, connection pooling |
| DB connector | psycopg2-binary | PostgreSQL driver |
| Data Processing | pandas 2.2, numpy | Cleaning, transformation |
| Dashboard | Plotly Dash 2.17 | Interactive web app |
| UI Components | Dash Bootstrap Components | Responsive dark-theme layout |
| Charts | Plotly 5.22 | Bar, donut, scatter visualizations |
| Web Server | Gunicorn 22 | Production WSGI server |
| Containerization | Docker (python:3.11-slim) | Reproducible build |
| App hosting | **Railway** | Docker-based cloud deployment, auto-deploys from GitHub |
| Config | python-dotenv | Secret management via `.env` |

---

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Production Setup                              │
│                                                                      │
│   GitHub Repo                                                        │
│   (main branch)                                                      │
│        │                                                             │
│        │  push → auto-deploy                                         │
│        ▼                                                             │
│   ┌─────────────┐         DATABASE_URL          ┌────────────────┐  │
│   │   Railway   │ ────────────────────────────► │   Supabase     │  │
│   │  (Docker)   │         (env variable)        │  PostgreSQL    │  │
│   │  Gunicorn   │                               │  us-east-1     │  │
│   │  port $PORT │                               │                │  │
│   └─────────────┘                               │  hospitals     │  │
│         │                                       │  (5,426 rows)  │  │
│         │ public URL                            │  timely_care   │  │
│         ▼                                       │  (138,129 rows)│  │
│   https://hospital-operations-                  └────────────────┘  │
│   dashboard-production.up.railway.app                                │
│                                                                      │
│   Connection: Supabase Session Pooler                                │
│   Host: aws-1-us-east-1.pooler.supabase.com:5432                    │
│   (session pooler used instead of direct connection for             │
│    compatibility with Railway's network and DNS)                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Why Supabase + Railway instead of Railway-managed PostgreSQL:**
- Supabase free tier provides 500MB PostgreSQL with a web SQL editor — useful for running schema DDL directly
- Railway provides zero-config Docker deployment with automatic public URLs and GitHub integration
- The session pooler URL (`aws-1-us-east-1.pooler.supabase.com`) is more reliable across different network environments than the direct connection URL (`db.*.supabase.co`), which can have DNS resolution issues on some networks

---

## Project Structure

```
hospital-operations-dashboard/
│
├── data/
│   └── download_data.py          # Paginated CMS API downloader (1,500 rows/page)
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
├── requirements.txt              # Python dependencies
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

**Live database stats (Supabase, as loaded):**
- `hospitals`: 5,426 rows
- `timely_care`: 138,129 rows

**CMS Comparison Field Values:**
- `"Above the national average"` — hospital performs better than peers
- `"Same as the national average"` — within expected range
- `"Below the national average"` — flagged for quality improvement
- `NULL` — insufficient data to compare

> **Note on derivation:** The CMS API no longer returns pre-computed comparison labels. The ETL derives them from measure count fields: if `count_of_*_measures_better > worse` → Above; `worse > better` → Below; otherwise → Same.

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
- PostgreSQL 15+ (local install **or** a free Supabase project)
- `git`

### Step 1 — Clone and configure

```bash
git clone https://github.com/shilp-tech/hospital-operations-dashboard.git
cd hospital-operations-dashboard

cp .env.example .env
# Edit .env and set DATABASE_URL (see formats below)
```

**Local PostgreSQL:**
```
DATABASE_URL=postgresql://postgres:mypassword@localhost:5432/hospital_dashboard
DASH_DEBUG=true
```

**Supabase (Session Pooler — recommended for cloud use):**
```
DATABASE_URL=postgresql://postgres.YOURREF:PASSWORD@aws-1-us-east-1.pooler.supabase.com:5432/postgres
DASH_DEBUG=true
```

> **Password encoding:** If your password contains special characters (e.g. `@`, `#`, `!`), URL-encode them before embedding in the connection string. For example, `p@ss` becomes `p%40ss`. You can encode a password with Python: `python -c "import urllib.parse; print(urllib.parse.quote('your_password'))"`.
>
> **Direct vs. Session Pooler:** The direct Supabase host (`db.YOURREF.supabase.co`) can have DNS resolution issues on some networks and ISPs. If you see `could not translate host name` errors, switch to the Session Pooler URL (`aws-1-us-east-1.pooler.supabase.com`) — it resolves reliably from all environments.

### Step 2 — Create the database

**Local PostgreSQL:**
```bash
psql -U postgres -c "CREATE DATABASE hospital_dashboard;"
```

**Supabase:** No action needed — the `postgres` database already exists. Skip to Step 3.

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

**Alternative (Supabase):** Paste the contents of `etl/create_schema.py` DDL block directly into the Supabase **SQL Editor** and run it there. This avoids any local network connectivity requirements.

### Step 5 — Download and load data

```bash
# Download all CMS data (handles pagination automatically — ~4 minutes)
python data/download_data.py

# Load into PostgreSQL (~2-3 minutes for 138,000+ rows)
python etl/load_data.py

# Or, if you want to start fresh:
python etl/load_data.py --reload

# No internet / quick test — generate 600 synthetic hospitals:
python etl/load_data.py --simulated
```

### Step 6 — Run the dashboard

```bash
python app/app.py
# Open: http://localhost:8050
```

**Full reload from scratch:**
```bash
python etl/load_data.py --reload
```

---

## Deployment — Supabase + Railway (actual production setup)

This is the exact setup used for the live demo.

### Step 1 — Set up Supabase database

1. Go to [supabase.com](https://supabase.com) and create a free project (select **us-east-1** region)
2. Once provisioned, open **SQL Editor** and run the full DDL from `etl/create_schema.py` (the `DDL` string, everything between the triple-quotes)
3. Confirm both tables exist: **Table Editor** should show `hospitals` and `timely_care`
4. Copy your connection string from **Project Settings → Database → Connection String → Session Pooler** — it looks like:
   ```
   postgresql://postgres.YOURREF:PASSWORD@aws-1-us-east-1.pooler.supabase.com:5432/postgres
   ```

### Step 2 — Load data from your local machine

Point your local `.env` at the Supabase Session Pooler URL and run the ETL:

```bash
# In .env:
DATABASE_URL=postgresql://postgres.YOURREF:ENCODED_PASSWORD@aws-1-us-east-1.pooler.supabase.com:5432/postgres

# Then run:
python data/download_data.py
python etl/load_data.py
```

This loads 5,426 hospitals and 138,129 timely care rows into Supabase over your internet connection. Takes 3–5 minutes.

### Step 3 — Deploy app to Railway

1. Push this repo to GitHub (the `.gitignore` already excludes `.env` and CSVs)
2. Go to [railway.app](https://railway.app) → **New Project → Deploy from GitHub repo**
3. Select your repository — Railway detects the `Dockerfile` automatically
4. In your service → **Variables**, add one variable:
   ```
   DATABASE_URL=postgresql://postgres.YOURREF:ENCODED_PASSWORD@aws-1-us-east-1.pooler.supabase.com:5432/postgres
   ```
   Do **not** set `PORT` — Railway injects it automatically and the `Dockerfile` reads it via `${PORT:-8050}`.
5. Trigger a deploy (or push a commit). Railway builds the Docker image and starts Gunicorn.
6. Your app is live at the Railway-generated URL (e.g. `https://hospital-operations-dashboard-production.up.railway.app`)

**Auto-deploy:** Every push to `main` triggers a new Railway build and deploy — no manual steps needed after initial setup.

---

## Running with Docker (optional local test)

```bash
# Build
docker build -t hospital-dashboard .

# Run (substitute your DATABASE_URL)
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
