# Expected KPI results — ground truth (computed locally from the raw CSVs, 2026-11-03)

Use in Step 5 to verify the Databricks notebook output. Method: parsed the raw files exactly
as downloaded (trim spaces, ISO dates, join sales→menu on product_id, price per order row,
duplicate rows kept as separate orders — 3 exact duplicate sales rows exist in the source).

Data shape: sales 117 rows, menu 6 rows, all product_ids match, grand total = 14340.
Every grouping below sums to 14340 — the cross-check the notebook must reproduce.

- KPI1 per customer: A 4260 · B 4440 · C 2400 · D 1200 · E 2040
- KPI2 per food item: sandwich 5760 · Chowmin 3600 · PIZZA 2100 · Dosa 1320 · Pasta 1080 · Biryani 480
- KPI3 per month: 1→2960 · 2→2730 · 3→910 · 5→2960 · 6→2960 · 7→910 · 11→910 (only 7 months exist in data)
- KPI4 per year: 2022→4350 · 2023→9990
- KPI5 per quarter: Q1 6600 · Q2 5920 · Q3 910 · Q4 910
- KPI6 product order counts: sandwich 48 · Chowmin 24 · PIZZA 21 · Dosa 12 · Pasta 6 · Biryani 6 (top item: sandwich)
- KPI7 restaurant visit days (distinct order dates, source=Restaurant): A 6 · B 6 · C 3 · D 1 · E 5
- KPI8a per country: India 4860 · UK 7020 · USA 2460
- KPI8b per source: Swiggy 6330 · zomato 4920 · Restaurant 3090

Menu (product_id, item, price): 1 PIZZA 100 · 2 Chowmin 150 · 3 sandwich 120 · 4 Dosa 110 · 5 Biryani 80 · 6 Pasta 180
Sales columns (no header row): product_id, customer_id (A–E), order_date (2022-01-01 → 2023-11-11), country, source
Quirks to handle in Steps 2–3: no header rows in either file; leading spaces in date/name/price
fields; trailing empty field on some rows in both files.

## Local Spark verification (2026-11-03, PySpark 4.3.0 + Temurin 21, local[2])

The exact Databricks notebook (notebooks/pyspark_end_to_end_project.py) was run end-to-end
locally against data/sales.csv + data/menu.csv (only the /Volumes paths swapped for local
paths and display() mapped to show()). Full output: output/local_spark_run.txt.
RESULT: every value above matched exactly — grand total 14340, all 8 KPIs, join = 117 rows.
Also confirmed empirically: the Step 2 schema read parses sales dates fine, but ALL menu
prices come out NULL (" 100" with leading space fails IntegerType) and product names keep
their leading space — the Step 3 re-read-as-text + trim + cast fixes both (clean menu = 6/6
prices). A trailing-comma arity bug in the first Step 3 draft was caught and fixed by giving
the raw re-reads exact all-string schemas.
