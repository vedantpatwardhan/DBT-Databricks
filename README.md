# dbt Sales Transformation Pipeline · Databricks

> A production-pattern data engineering pipeline built on **dbt Core** and **Databricks**, transforming raw retail data into analytics-ready Gold layer tables using the **Medallion Architecture** (Bronze → Silver → Gold).

---

## The Business Problem

Retail analytics teams struggle with a fundamental data reliability issue: raw transactional data from sales systems is messy, denormalised, and inconsistent — making it impossible to answer business questions like:

- Which stores generate the highest gross revenue after returns?
- How does customer behaviour change across date dimensions (weekly, monthly, seasonal)?
- What does net sales performance look like once returns are factored in?

Without a structured transformation layer, every analyst writes their own version of the same JOIN. One team's "total sales" doesn't match another's. Decisions get made on inconsistent numbers.

**This pipeline solves that.** It establishes a single, tested, version-controlled transformation layer that every downstream consumer — dashboards, ad-hoc queries, ML features — can trust.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                               │
│              Databricks Catalog  ·  sources.yml                     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt source()
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BRONZE  (Staging layer)                         │
│         Cast types · Rename fields · Filter nulls · No joins        │
│                                                                     │
│  bronze_customer.sql    bronze_date.sql    bronze_returns.sql       │
│  bronze_sales.sql       bronze_store.sql   properties.yml           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt ref()
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SILVER  (Intermediate layer)                    │
│          Join sources · Apply business rules · Enrich & flatten     │
│                                                                     │
│                      silver_salesinfo.sql                           │
│         (joins bronze_sales + bronze_customer + bronze_store        │
│                  + bronze_date + bronze_returns)                    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt ref()
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      GOLD  (Mart layer)                             │
│           Aggregate · Summarise · Business-ready output             │
│                                                                     │
│                     source_gold_items.sql                           │
│          (gross_amount by category, store, date dimension)          │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
               Dashboards  ·  Ad-hoc SQL  ·  Reporting
```

Each layer has a strict contract:
- **Bronze** never contains joins or business logic — only type casts and field renames
- **Silver** never references raw sources directly — only Bronze models via `ref()`
- **Gold** is the only layer consumed by downstream users and dashboards

---

## Project Structure

```
dbt_project/
│
├── analyses/                        ← Exploratory SQL (not part of the DAG)
│   ├── 1_explore.sql                   Initial source exploration
│   ├── jinja1.sql                      Jinja templating experiments
│   ├── jinja2.sql
│   ├── jinja3.sql
│   ├── macro-1.sql                     Macro usage examples
│   ├── query_macro.sql
│   └── target_variables.sql            dbt target/env variable tests
│
├── macros/                          ← Reusable Jinja logic
│   ├── generate_schema.sql             Custom schema name generation
│   └── multiply.sql                    Gross amount calculation macro
│
├── models/
│   ├── source/
│   │   └── sources.yml              ← Source declarations (Bronze entry point)
│   │
│   ├── bronze/                      ← Staging models (materialised as views)
│   │   ├── bronze_customer.sql         Cast + rename customer fields
│   │   ├── bronze_date.sql             Parse and standardise date dimensions
│   │   ├── bronze_returns.sql          Clean returns data
│   │   ├── bronze_sales.sql            Cast sales transactions
│   │   ├── bronze_store.sql            Standardise store reference data
│   │   └── properties.yml              Schema tests for Bronze models
│   │
│   ├── silver/                      ← Intermediate models (materialised as tables)
│   │   └── silver_salesinfo.sql        Join all Bronze sources into enriched flat table
│   │
│   └── gold/                        ← Mart models (business-ready aggregations)
│       └── source_gold_items.sql       Aggregate gross_amount by category / store / date
│
├── snapshots/                       ← SCD Type 2 historical tracking
│   └── gold_items_snapshot.sql         Tracks price & category changes over time
│
├── logs/
│   └── dbt.log                      ← Runtime logs (gitignored)
│
└── dbt_project.yml                  ← Project config, materialisation defaults
```

---

## Key Engineering Decisions

### Materialisation strategy per layer

| Layer | Model | Materialisation | Rationale |
|---|---|---|---|
| Bronze | `bronze_*.sql` | `view` | Zero storage cost; always reflects latest source |
| Silver | `silver_salesinfo.sql` | `table` | 5-table join computed once, reused by Gold |
| Gold | `source_gold_items.sql` | `table` | Pre-aggregated for fast BI query performance |

Views in Bronze mean source schema changes propagate instantly with no rebuild. Tables in Silver mean the expensive multi-source join runs once per pipeline execution, not once per dashboard query.

### Two macros, one place to change

```sql
-- macros/multiply.sql
-- Gross amount formula lives here, referenced across all models
{% macro multiply(column_a, column_b) %}
    ({{ column_a }} * {{ column_b }})
{% endmacro %}

-- macros/generate_schema.sql
-- Overrides dbt's default schema naming to keep layer schemas clean
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

The `generate_schema` macro ensures Bronze models land in the `bronze` schema, Silver in `silver`, and Gold in `gold` — regardless of the dbt target environment. Schema isolation is enforced by code, not by convention.

### Silver as the single source of truth for joins

`silver_salesinfo.sql` is the backbone of the pipeline. It joins five Bronze models:

```
bronze_sales
    + bronze_customer   (on customer_id)
    + bronze_store      (on store_id)
    + bronze_date       (on date_id)
    + bronze_returns    (on sale_id, to calculate net amounts)
= silver_salesinfo      (one enriched, flat, denormalised table)
```

Every Gold model reads from `silver_salesinfo` via `ref()`. No Gold model ever reaches back to Bronze. This means if the join logic changes, it changes in exactly one place.

### SCD Type 2 snapshot on Gold items

```sql
{% snapshot gold_items_snapshot %}
{{ config(
    target_schema  = 'snapshots',
    unique_key     = 'item_id',
    strategy       = 'check',
    check_cols     = ['unit_price', 'category']
) }}
select * from {{ ref('source_gold_items') }}
{% endsnapshot %}
```

dbt adds `dbt_valid_from` and `dbt_valid_to` automatically. This enables: *"What was the price of this item when this order was placed?"* — a question that is impossible to answer without historical tracking.

---

## Data Quality Gates

Tests run on every `dbt build`. Bad data cannot reach Gold.

```yaml
# models/bronze/properties.yml
models:
  - name: bronze_sales
    columns:
      - name: sale_id
        tests: [unique, not_null]
      - name: quantity
        tests: [not_null]
      - name: unit_price
        tests: [not_null]

  - name: bronze_customer
    columns:
      - name: customer_id
        tests: [unique, not_null]

  - name: bronze_store
    columns:
      - name: store_id
        tests: [unique, not_null]
```

Custom singular test enforcing a business rule:

```sql
-- tests/assert_gross_amount_non_negative.sql
-- Gross amount must never be negative after returns are applied
select *
from {{ ref('source_gold_items') }}
where gross_amount < 0
```

---

## Model Lineage (DAG)

```
sources.yml
    │
    ├── bronze_customer ──────┐
    ├── bronze_date ──────────┤
    ├── bronze_returns ───────┼──► silver_salesinfo ──► source_gold_items
    ├── bronze_sales ─────────┤                               │
    └── bronze_store ─────────┘                               ▼
                                                    gold_items_snapshot
```

All five Bronze models feed a single Silver model. Gold reads only from Silver. The DAG has no shortcuts or cross-layer references — every hop is intentional and auditable via `dbt docs`.

---

## Tech Stack

| Component | Technology |
|---|---|
| Data warehouse | Databricks SQL Warehouse |
| Transformation tool | dbt Core v1.11.8 |
| Query language | SQL + Jinja2 |
| Reusable logic | Custom macros — `multiply`, `generate_schema` |
| Historical tracking | dbt Snapshots — SCD Type 2 |
| Data quality | dbt generic + singular tests via `properties.yml` |
| Source config | `sources.yml` with source declarations |
| Exploratory analysis | `analyses/` folder with Jinja + macro experiments |
| Version control | Git + GitHub |

---

## How to Run

### Prerequisites

```bash
pip install dbt-databricks
```

### Profile configuration

Create `~/.dbt/profiles.yml` (never committed — excluded via `.gitignore`):

```yaml
dbt_project:
  outputs:
    dev:
      type: databricks
      host: <your-databricks-host>
      http_path: <your-sql-warehouse-http-path>
      token: <your-personal-access-token>
      catalog: dbt_tutorial_dev
      schema: default
      threads: 4
  target: dev
```

### Full pipeline execution

```bash
dbt seed        # Load static reference data
dbt run         # Execute all transformation models
dbt test        # Run all data quality tests
dbt snapshot    # Capture SCD Type 2 history

# Recommended: run everything in one command
dbt build
```

### Run a specific layer

```bash
dbt run --select bronze                       # Bronze only
dbt run --select silver                       # Silver only
dbt run --select gold                         # Gold only
dbt run --select +source_gold_items           # Gold model + all upstream dependencies
dbt test --select bronze                      # Test Bronze models only
```

---

## Analyses Folder

The `analyses/` folder documents the learning progression and is not part of the production DAG:

| File | Purpose |
|---|---|
| `1_explore.sql` | Initial source table exploration queries |
| `jinja1.sql` → `jinja3.sql` | Progressively complex Jinja templating experiments |
| `macro-1.sql` | First macro invocation and testing |
| `query_macro.sql` | Macro usage patterns across different query types |
| `target_variables.sql` | dbt `target` and environment variable experiments |

These are retained in the repo as documentation of the engineering process — showing how the Jinja and macro patterns in `analyses/` were developed and then promoted into production `macros/`.

---

## Roadmap

- [ ] Add GitHub Actions CI/CD — `dbt build --select state:modified+` on pull requests
- [ ] Convert `source_gold_items` to incremental materialisation for large-volume data
- [ ] Add `source freshness` thresholds in `sources.yml` to detect stale ingestion
- [ ] Add `dbt docs generate` step to auto-publish lineage graph on every merge
- [ ] Extend to a real-world problem domain — return fraud detection or customer churn prediction pipeline

---

## What I Learned

- **Medallion architecture** — why strict layer separation prevents logic duplication and makes pipelines debuggable at scale
- **Materialisation strategy** — the cost/freshness tradeoff between views (Bronze), tables (Silver), and incremental models (Gold roadmap)
- **Jinja macros** — how `multiply.sql` eliminates formula drift and `generate_schema.sql` enforces environment-level schema isolation
- **SCD Type 2 snapshots** — preserving historical state for point-in-time reporting in audit and finance contexts
- **dbt testing** — `properties.yml` schema tests as a data quality contract; singular tests for business rule enforcement
- **Source declarations** — how `source()` creates a clean, auditable boundary between ingestion and transformation
- **Analyses folder** — using dbt's analyses layer for exploratory SQL without polluting the production DAG

---

Built by **Vedant Patwardhan** · [GitHub](https://github.com/vedantpatwardhan) · [LinkedIn](https://linkedin.com/in/vedantpatwardhan)