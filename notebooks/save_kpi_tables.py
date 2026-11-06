# Databricks notebook source
# Step 5 helper — run AFTER the main notebook (pyspark_end_to_end_project) in the SAME
# workspace. In Databricks, attach this as extra cells at the end of the main notebook
# instead (variables kpi1..kpi8b live in that notebook's session).
# Saves every KPI DataFrame as a Unity Catalog table so the AI/BI dashboard can use them.

# COMMAND ----------

kpi1_customer_total.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi1_customer_total")
kpi2_category_total.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi2_category_total")
kpi3_monthly.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi3_monthly_sales")
kpi4_yearly.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi4_yearly_sales")
kpi5_quarterly.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi5_quarterly_sales")
kpi6_product_counts.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi6_product_counts")
kpi7_visit_frequency.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi7_visit_frequency")
kpi8a_country.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi8a_country_sales")
kpi8b_source.write.mode("overwrite").saveAsTable("pyspark_project.sales.kpi8b_source_sales")

print("9 KPI tables saved under pyspark_project.sales")

# COMMAND ----------

# Check: read one back
spark.table("pyspark_project.sales.kpi6_product_counts").show(1)
# expected: 3 | sandwich | 48
