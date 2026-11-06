# PySpark End-to-End Project — Restaurant Sales KPIs (Databricks)

End-to-end PySpark project built on **Databricks Free Edition** (serverless, Unity Catalog),
following the "End to End Pyspark Project" walkthrough by *learn by doing it*
(https://youtu.be/uz3LybMH7II) and verified value-for-value against a locally computed
ground truth.

## Business problem

A restaurant group sells through three channels — dine-in (Restaurant), Swiggy, and zomato —
across India, the UK, and the USA. Two raw CSV extracts exist (sales + menu) with no headers
and messy fields. The business needs 8 KPIs to understand customers, products, and sales
trends over time.

## Architecture

```
sales.csv (117 rows) ──┐
                       ├─► Unity Catalog Volume ─► PySpark notebook (serverless)
menu.csv  (6 rows)   ──┘     /Volumes/pyspark_project/sales/raw
                                  │
                    explicit schema load ─► clean (trim/cast/to_date)
                                  │        ─► derive year / month / quarter
                                  │        ─► join sales ⨝ menu on product_id
                                  ▼
                         8 KPI DataFrames ─► 9 Unity Catalog tables
                                  ▼
                    AI/BI dashboard: pyspark_sales_kpi_dashboard
```

## The 8 KPIs

1. Total amount spent by each customer
2. Total amount spent by each food category
3. Total sales by month
4. Total sales by year
5. Total sales by quarter
6. Top ordered items (product order counts)
7. Customer visit frequency (distinct restaurant-visit days)
8. Total sales by country and by order source

**Headline results:** grand total sales **14,340** · top item **sandwich (48 orders)** ·
best customer **B (4,440)** · strongest market **UK (7,020)**.

## Data quality issues found & fixed

- Neither file has a header row → read with `header=false` + explicit `StructType` schema
- Leading spaces inside fields (`" 100"`) silently turn integer prices into NULLs under a
  strict schema read → files re-read as text, every field `trim()`-ed, then cast /
  `to_date(..., "yyyy-MM-dd")`
- Stray trailing comma on some rows → exact field-count schemas on the cleaning read
- Duplicate order rows exist in the source and are kept as separate orders

## Verification

Every KPI was cross-checked two ways: an independent local computation from the raw CSVs,
and a full local PySpark run of this exact notebook. All groupings (customer, category,
month, year, quarter, country, source) sum to the same grand total: **14,340**.

## Files

- `notebooks/pyspark_end_to_end_project.py` — the Databricks notebook (Steps 2–4)
- `notebooks/save_kpi_tables.py` — Step 5 helper: writes the 9 KPI tables for the dashboard
- `data/sales.csv`, `data/menu.csv` — source data (public dataset from the video description)
- `output/EXPECTED_KPIS.md` — ground-truth results used for verification

## How to run

1. Databricks Free Edition workspace → create catalog `pyspark_project`, schema `sales`,
   volume `raw`; upload `data/sales.csv` and `data/menu.csv` into the volume
2. Create notebook `pyspark_end_to_end_project` (Python, Serverless), run cells in order
3. Append `save_kpi_tables.py` cells, run, then build the `pyspark_sales_kpi_dashboard`
   dashboard from the 9 KPI tables

## Final dashboard

Standalone dashboard with all 9 KPI charts on one page:
[`output/final_dashboard.html`](output/final_dashboard.html) — download the file and open it
in any browser (GitHub does not preview HTML inline). Rendered from the verified results,
including the KPI 5 quarterly chart (Q1 6,600 · Q2 5,920 · Q3 910 · Q4 910).

## Databricks dashboard

The AI/BI dashboard `pyspark_sales_kpi_dashboard` (header + 9 bar charts) was built in
Databricks Free Edition and verified widget-by-widget against the ground truth in
`output/EXPECTED_KPIS.md` on 2026-11-05/06. Screenshots of every verified widget are kept
with the author's local project copy.
