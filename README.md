# Data Lineage

A single-page web application for visualising end-to-end data lineage across Snowflake databases. Built with **Django** (backend API) and **React** (frontend), it renders an interactive graph that lets analysts trace how data flows from source tables to downstream datasets.

---
https://github.com/user-attachments/assets/5a8895a8-7920-4b76-8a05-968712f97a80

## Features

- Interactive node-edge lineage graph powered by **vis.js**
- Collapsible DB → Schema → Table navigation tree with live search
- Click a node to view table profile (owner, row count, DDL history, etc.) and privileges
- Click an edge to view column-level source → target mapping
- **Expand Front / Expand Back** — progressively explore downstream and upstream lineage
- Column search — highlight edges that carry a specific column
- Dark-themed UI built in React (no build step — CDN-delivered)

---

## Tech Stack

| Layer      | Technology                              |
|------------|-----------------------------------------|
| Backend    | Python 3.9, Django 4.2                  |
| Frontend   | React 18, vis.js, FontAwesome           |
| Database   | Snowflake-managed PostgreSQL (psycopg2) |
| Lineage    | `consumption.data_lineage_master_data`  |

---

## Project Structure

```
apps/
  data_observability/
    functions.py   # data loading + graph/node/edge builders
    views.py       # Django views (all CSRF-exempt, no auth)
    urls.py        # URL routing
config/
  settings.py
  urls.py
templates/
  layouts/base-all-module.html
  data_observability/data-lineage.html   # React SPA
static/
.env                                     # DB credentials (gitignored)
master_data.sql                          # DDL for the lineage table
snowflake_setup.sql                      # Snowflake setup scripts
```

---

## Getting Started

### 1. Clone and set up the environment

```bash
git clone <repo-url>
cd Data_lineage
python -m venv .venv
source .venv/bin/activate
pip install django psycopg2-binary pandas python-dotenv
```

### 2. Configure credentials

Create a `.env` file in the project root:

```env
PGHOST=<your-postgres-host>
PGPORT=5432
PGUSER=<user>
PGPASSWORD=<password>
```

The app connects to the `data_observability` database and reads from `consumption.data_lineage_master_data`.



### 3. Run the application

```bash
python manage.py migrate
python manage.py runserver
```

---

## URL Reference

| URL                    | Description                       |
|------------------------|-----------------------------------|
| `/`                    | Data lineage landing page         |
| `/create-lineage/`     | POST — initial graph for a table  |
| `/expand-lineage/`     | POST — expand downstream nodes    |
| `/expandback-lineage/` | POST — expand upstream nodes      |
| `/display-edge/`       | POST — column mapping for an edge |
