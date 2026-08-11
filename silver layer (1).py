# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access Using App

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Reading all the files

# COMMAND ----------

df_hosp= spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@pharmastoragedatalake01.dfs.core.windows.net/Hospital01")

# COMMAND ----------

df_sales= spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@pharmastoragedatalake01.dfs.core.windows.net/Sales01")

# COMMAND ----------

df_Inventory= spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@pharmastoragedatalake01.dfs.core.windows.net/Inventory01")

# COMMAND ----------

df_Rep= spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@pharmastoragedatalake01.dfs.core.windows.net/Reports01")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.count(), len(df_sales.columns)

# COMMAND ----------

df_sales.printSchema()

# COMMAND ----------

df_sales.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_sales.columns
]).display()

# COMMAND ----------

df_sales.count() - df_sales.dropDuplicates().count()

# COMMAND ----------

df_sales.filter(col("Quantity_Sold") < 0).display()

# COMMAND ----------

df_sales.select(
    "Quantity_Sold",
    "Unit_Price",
    "Discount_Percent",
    "Total_Sales"
).summary().display()

# COMMAND ----------

df_sales.select(
    min("Sale_Date").alias("Min_Date"),
    max("Sale_Date").alias("Max_Date")
).display()

# COMMAND ----------

df_sales_clean= df_sales
df_sales_clean = df_sales_clean.dropDuplicates()

# COMMAND ----------

df_sales_clean = df_sales_clean.withColumn(
    "Discount_Percent",
    coalesce(col("Discount_Percent"), lit(0.0))
)

# COMMAND ----------

df_sales_clean = df_sales_clean.filter(
    col("Customer_ID").isNotNull()
)

# COMMAND ----------

df_sales_clean = df_sales_clean.filter(
    col("Quantity_Sold") >= 0
)

# COMMAND ----------

df_sales_clean.count()

# COMMAND ----------

from pyspark.sql.functions import col, round

df_sales_clean = df_sales_clean.withColumn(
    "Calculated_Sales",
    round(
        col("Quantity_Sold") *
        col("Unit_Price") *
        (1 - col("Discount_Percent") / 100),
        2
    )
)

# COMMAND ----------

df_sales_clean.select(
    "Quantity_Sold",
    "Unit_Price",
    "Discount_Percent",
    "Total_Sales",
    "Calculated_Sales"
).display()

# COMMAND ----------

df_sales_clean.filter(
    col("Total_Sales") != col("Calculated_Sales")
).display()

# COMMAND ----------

df_sales_clean = df_sales_clean.drop("Total_Sales")

# COMMAND ----------

df_sales_clean = df_sales_clean.withColumnRenamed("CalculatedSales", "Total_Sales")

# COMMAND ----------

df_sales_clean.display()

# COMMAND ----------

df_sales_clean.write.format("parquet").mode("append").option("path", "abfss://silver@pharmastoragedatalake01.dfs.core.windows.net/Sales").save()
#Sending sales data into Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory

# COMMAND ----------

df_Inventory = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("abfss://bronze@pharmastoragedatalake01.dfs.core.windows.net/Inventory01")

# COMMAND ----------

df_Inventory.display()

# COMMAND ----------

df_Inventory.printSchema()

# COMMAND ----------

df_Inventory.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_Inventory.columns
]).display()

# COMMAND ----------

df_Inventory.count() - df_Inventory.dropDuplicates().count()

# COMMAND ----------

df_Inventory.select(
    "Opening_Stock",
    "Production_Qty",
    "Quantity_Sold",
    "Closing_Stock"
).summary().display()

# COMMAND ----------

df_Inventory.filter(
    col("Expiry_Date") < col("Manufacturing_Date")
).display()

# COMMAND ----------

df_Inventory_clean = df_Inventory

# COMMAND ----------

df_Inventory_clean = df_Inventory_clean.dropna(
    subset=["Product_ID", "Batch_ID", "Manufacturing_Date", "Expiry_Date"]
)

# COMMAND ----------

df_Inventory_clean = df_Inventory_clean.fillna({
    "Warehouse": "Unknown"
})

# COMMAND ----------

df_Inventory_clean = df_Inventory_clean.withColumn(
    "Calculated_Closing_Stock",
    col("Opening_Stock") +
    col("Production_Qty") -
    col("Quantity_Sold")
)

# COMMAND ----------

df_Inventory_clean.select(
    "Opening_Stock",
    "Production_Qty",
    "Quantity_Sold",
    "Closing_Stock",
    "Calculated_Closing_Stock"
).display()

# COMMAND ----------

df_Inventory_clean.filter(
    col("Closing_Stock") != col("Calculated_Closing_Stock")
).display()

# COMMAND ----------

df_Inventory_clean = df_Inventory_clean.drop(
    "Calculated_Closing_Stock"
)

# COMMAND ----------



df_Inventory_clean = df_Inventory_clean.withColumn(
    "Days_To_Expiry",
    datediff(col("Expiry_Date"), current_date())
)

# COMMAND ----------



df_Inventory_clean = df_Inventory_clean.withColumn(
    "Stock_Status",
    when(col("Closing_Stock") <= 0, "Out of Stock")
    .when(col("Closing_Stock") < 500, "Low Stock")
    .otherwise("Healthy")
)

# COMMAND ----------

df_Inventory_clean = df_Inventory_clean.withColumn(
    "Expiry_Status",
    when(col("Days_To_Expiry") < 0, "Expired")
    .when(col("Days_To_Expiry") <= 90, "Critical")
    .when(col("Days_To_Expiry") <= 180, "Expiring Soon")
    .otherwise("Healthy")
)

# COMMAND ----------

df_Inventory_clean.filter(
    col("Expiry_Date") < col("Manufacturing_Date")
).display()

# COMMAND ----------

df_Inventory_clean.write.format("parquet").mode("append").option("path", "abfss://silver@pharmastoragedatalake01.dfs.core.windows.net/Inventory").save()
#Sending sInventory data into Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hospital

# COMMAND ----------

df_hosp.display()

# COMMAND ----------

df_hosp.printSchema()

# COMMAND ----------


df_hosp.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_hosp.columns
]).display()

# COMMAND ----------

df_hosp.count() - df_hosp.dropDuplicates().count()

# COMMAND ----------

df_hosp.select("Hospital_Name").distinct().display()

# COMMAND ----------



df_hosp.select(
    "Hospital_Name",
    lower(trim(col("Hospital_Name"))).alias("Standard_Name")
).display()

# COMMAND ----------

df_hosp.groupBy(
    lower(trim(col("Hospital_Name"))).alias("Standard_Name")
).count().filter(
    col("count") > 1
).display()

# COMMAND ----------

df_hosp.groupBy(
    lower(trim(col("Hospital_Name"))).alias("Standard_Name")
).agg(
    countDistinct(col("Hospital_Name")).alias("Different_Original_Names")
).filter(
    col("Different_Original_Names") > 1
).display()

# COMMAND ----------

df_hosp.printSchema()

# COMMAND ----------

df_hosp_clean = df_hosp
for c in ["Customer_ID", "Customer_Type", "Hospital_Name",
          "Doctor_Name", "Specialty", "City", "State",
          "Region", "Customer_Segment"]:

    df_hosp_clean = df_hosp_clean.withColumn(
        c,
        trim(col(c))
    )

# COMMAND ----------

df_hosp_clean.select("Region").distinct().display()
df_hosp_clean.select("Specialty").distinct().display()
df_hosp_clean.select("Customer_Type").distinct().display()

# COMMAND ----------

df_hosp_clean.write.format("parquet").mode("append").option("path", "abfss://silver@pharmastoragedatalake01.dfs.core.windows.net/Hospital").save()
#Sending Hospital data into Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reports

# COMMAND ----------

df_Rep.display()

# COMMAND ----------

df_Rep.printSchema()

# COMMAND ----------

df_Rep.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_Rep.columns
]).display()

# COMMAND ----------

df_Rep.count() - df_Rep.dropDuplicates().count()

# COMMAND ----------

df_Rep.select("Quality_Status").distinct().display()
df_Rep.select("Test_Type").distinct().display()
df_Rep.select("Defect_Type").distinct().display()

# COMMAND ----------

df_Rep.select(
    "Test_Result",
    "Specification_Limit",
    "Rejection_Qty"
).summary().display()

# COMMAND ----------

df_Rep.filter(
    col("Test_Result") > col("Specification_Limit")
).count()

# COMMAND ----------

df_Rep.groupBy("Quality_Status").count().display()


# COMMAND ----------

df_Rep.filter(
    (col("Test_Result") > col("Specification_Limit")) &
    (col("Quality_Status") != "Failed")
).count()

# COMMAND ----------

df_Rep.filter(
    col("Test_Result") > col("Specification_Limit")
).select(
    "Test_Result",
    "Specification_Limit",
    "Quality_Status"
).display()

# COMMAND ----------

df_Rep.groupBy(
    "Quality_Status"
).agg(
    count("*").alias("Total"),
    sum(
        when(
            col("Test_Result") > col("Specification_Limit"),
            1
        ).otherwise(0)
    ).alias("Above_Limit")
).display()

# COMMAND ----------

df_Rep.filter(
    (col("Quality_Status") == "Failed") &
    (col("Rejection_Qty") == 0)
).count()

# COMMAND ----------

df_Rep.filter(
    (col("Quality_Status") == "Passed") &
    (col("Rejection_Qty") > 0)
).count()

# COMMAND ----------

df_Rep_clean = df_Rep.withColumn(
    "Rejection_Status",
    when(col("Rejection_Qty") > 0, "Rejected")
    .otherwise("No Rejection")
)

# COMMAND ----------

df_Rep_clean.groupBy("Rejection_Status").count().display()

# COMMAND ----------

df_Rep_clean.filter(
    (
        (col("Quality_Status") == "Failed") &
        (col("Rejection_Status") != "Rejected")
    ) |
    (
        (col("Quality_Status") == "Passed") &
        (col("Rejection_Status") != "No Rejection")
    )
).count()

# COMMAND ----------

df_Rep_clean.printSchema()

# COMMAND ----------

df_Rep_clean.write.format("parquet").mode("append").option("path", "abfss://silver@pharmastoragedatalake01.dfs.core.windows.net/Reports").save()
#Sending Hospital data into Silver layer.