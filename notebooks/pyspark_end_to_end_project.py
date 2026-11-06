# Databricks notebook source
# Project: End to End PySpark Project (restaurant sales KPIs)
# Run on: Databricks Free Edition, Serverless compute, Python
# Data: /Volumes/pyspark_project/sales/raw/sales.csv and menu.csv (uploaded in Step 1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 — Load the data
# MAGIC We tell PySpark exactly what each column is (a schema), then read the two CSV files.
# MAGIC Neither file has a heading row, so header = false.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, trim, to_date, sum as spark_sum, count, countDistinct, year, month, quarter, desc

# COMMAND ----------

sales_schema = StructType([
    StructField("product_id",  IntegerType(), True),
    StructField("customer_id", StringType(),  True),
    StructField("order_date",  DateType(),    True),
    StructField("country",     StringType(),  True),
    StructField("source",      StringType(),  True)
])

menu_schema = StructType([
    StructField("product_id",   IntegerType(), True),
    StructField("product_name", StringType(),  True),
    StructField("price",        IntegerType(), True)
])

# COMMAND ----------

sales_df = (spark.read
    .format("csv")
    .option("header", "false")
    .schema(sales_schema)
    .load("/Volumes/pyspark_project/sales/raw/sales.csv"))

menu_df = (spark.read
    .format("csv")
    .option("header", "false")
    .schema(menu_schema)
    .load("/Volumes/pyspark_project/sales/raw/menu.csv"))

# COMMAND ----------

sales_df.printSchema()
sales_df.show(10)
print("sales rows:", sales_df.count())   # expected: 117

# COMMAND ----------

menu_df.printSchema()
menu_df.show()
print("menu rows:", menu_df.count())     # expected: 6

# COMMAND ----------

# Look closely at the two shows above — two problems are hiding in plain sight:
# 1. Every menu price came out NULL. The file's price field looks like " 100"
#    (a space in front), and Spark's integer reader refuses it.
# 2. Food names kept their leading space (" PIZZA").
# The sales dates survived (" 2023-01-01" parsed fine), but we fix everything
# the same safe way in Step 3. Count of broken prices:
print("menu rows with missing price:", menu_df.filter(col("price").isNull()).count())  # expected: 6

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3 — Clean & prepare
# MAGIC The raw files have extra spaces inside the fields (dates look like " 2023-01-01").
# MAGIC We re-read both files as plain text, trim every field, then convert types safely.
# MAGIC This gives the same clean result no matter how the spaces fall.

# COMMAND ----------

# Read both files again, this time EVERYTHING as plain text, with an exact
# field list. (An exact schema also protects us from the stray trailing comma
# at the end of some lines — extra empty fields are simply ignored.)
raw_sales_schema = StructType([
    StructField("product_id",  StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date",  StringType(), True),
    StructField("country",     StringType(), True),
    StructField("source",      StringType(), True)
])

raw_menu_schema = StructType([
    StructField("product_id",   StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("price",        StringType(), True)
])

raw_sales = (spark.read
    .format("csv")
    .option("header", "false")
    .schema(raw_sales_schema)
    .load("/Volumes/pyspark_project/sales/raw/sales.csv"))

raw_menu = (spark.read
    .format("csv")
    .option("header", "false")
    .schema(raw_menu_schema)
    .load("/Volumes/pyspark_project/sales/raw/menu.csv"))

# COMMAND ----------

sales_clean = (raw_sales
    .withColumn("product_id",  trim(col("product_id")).cast("int"))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_date",  to_date(trim(col("order_date")), "yyyy-MM-dd"))
    .withColumn("country",     trim(col("country")))
    .withColumn("source",      trim(col("source")))
    .dropna(subset=["product_id", "customer_id", "order_date"]))

menu_clean = (raw_menu
    .withColumn("product_id",   trim(col("product_id")).cast("int"))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("price",        trim(col("price")).cast("int"))
    .dropna(subset=["product_id", "price"]))

sales_clean.show(10)
print("clean sales rows:", sales_clean.count())   # expected: 117
print("clean menu rows:", menu_clean.count())     # expected: 6

# COMMAND ----------

# The moment of truth: prices must be back (100/150/120/110/80/180), names trimmed
menu_clean.show()

# COMMAND ----------

# Add the three helper columns the KPIs need, taken from the order date
sales_clean = (sales_clean
    .withColumn("order_year",    year(col("order_date")))
    .withColumn("order_month",   month(col("order_date")))
    .withColumn("order_quarter", quarter(col("order_date"))))

sales_clean.show(10)

# COMMAND ----------

# Join the two lists: every sale gets its food name and price from the menu
sales_enriched = sales_clean.join(menu_clean, on="product_id", how="inner")

sales_enriched.show(10)
print("joined rows:", sales_enriched.count())     # expected: 117 (no sale lost)

# COMMAND ----------

# Sanity check before KPIs: total of all prices must be 14340
sales_enriched.select(spark_sum("price").alias("grand_total")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4 — The 8 KPIs
# MAGIC Every KPI is the same pattern: join is already done, so groupBy one column,
# MAGIC add up (or count) the price, sort. In Databricks, click the chart icon under
# MAGIC each display() result to set the chart type noted above each cell.

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 1 — Total amount spent by each customer** (table / bar chart)

# COMMAND ----------

kpi1_customer_total = (sales_enriched
    .groupBy("customer_id")
    .agg(spark_sum("price").alias("total_spent"))
    .orderBy("customer_id"))

display(kpi1_customer_total)
# expected: A 4260, B 4440, C 2400, D 1200, E 2040

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 2 — Total amount spent by each food category** (bar chart)

# COMMAND ----------

kpi2_category_total = (sales_enriched
    .groupBy("product_name")
    .agg(spark_sum("price").alias("total_spent"))
    .orderBy(desc("total_spent")))

display(kpi2_category_total)
# expected: sandwich 5760, Chowmin 3600, PIZZA 2100, Dosa 1320, Pasta 1080, Biryani 480

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 3 — Total sales by month** (bar chart)

# COMMAND ----------

kpi3_monthly = (sales_enriched
    .groupBy("order_month")
    .agg(spark_sum("price").alias("total_sales"))
    .orderBy("order_month"))

display(kpi3_monthly)
# expected: 1:2960, 2:2730, 3:910, 5:2960, 6:2960, 7:910, 11:910

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 4 — Total sales by year** (line chart)

# COMMAND ----------

kpi4_yearly = (sales_enriched
    .groupBy("order_year")
    .agg(spark_sum("price").alias("total_sales"))
    .orderBy("order_year"))

display(kpi4_yearly)
# expected: 2022:4350, 2023:9990

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 5 — Total sales by quarter** (bar chart)

# COMMAND ----------

kpi5_quarterly = (sales_enriched
    .groupBy("order_quarter")
    .agg(spark_sum("price").alias("total_sales"))
    .orderBy("order_quarter"))

display(kpi5_quarterly)
# expected: Q1 6600, Q2 5920, Q3 910, Q4 910

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 6 — Top ordered items: how many times each product was ordered** (funnel chart)

# COMMAND ----------

kpi6_product_counts = (sales_enriched
    .groupBy("product_id", "product_name")
    .agg(count("product_id").alias("order_count"))
    .orderBy(desc("order_count")))

display(kpi6_product_counts)
# expected: sandwich 48, Chowmin 24, PIZZA 21, Dosa 12, Pasta 6, Biryani 6

# COMMAND ----------

# The single top item
display(kpi6_product_counts.limit(1))
# expected: sandwich, 48

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 7 — Customer visit frequency: on how many different days each customer
# MAGIC visited the restaurant** (only orders whose source is Restaurant; bar chart)

# COMMAND ----------

kpi7_visit_frequency = (sales_enriched
    .filter(col("source") == "Restaurant")
    .groupBy("customer_id")
    .agg(countDistinct("order_date").alias("visit_days"))
    .orderBy("customer_id"))

display(kpi7_visit_frequency)
# expected: A 6, B 6, C 3, D 1, E 5

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 8 — Total sales by country, and by order source** (pie / bar chart)

# COMMAND ----------

kpi8a_country = (sales_enriched
    .groupBy("country")
    .agg(spark_sum("price").alias("total_sales"))
    .orderBy(desc("total_sales")))

display(kpi8a_country)
# expected: UK 7020, India 4860, USA 2460

# COMMAND ----------

kpi8b_source = (sales_enriched
    .groupBy("source")
    .agg(spark_sum("price").alias("total_sales"))
    .orderBy(desc("total_sales")))

display(kpi8b_source)
# expected: Swiggy 6330, zomato 4920, Restaurant 3090

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5 — Dashboard & finish (done in the Databricks UI)
# MAGIC Add each KPI display above to a dashboard named pyspark_sales_kpi_dashboard,
# MAGIC one tile per KPI in order 1-8, then re-run this notebook and confirm the
# MAGIC numbers still match the expected values in the comments.
