# Databricks notebook source
# COMMAND ----------
dbutils.widgets.text("catalog", "retail_dab_dev")
dbutils.widgets.text("schema", "raw")
dbutils.widgets.text("num_transactions", "8000000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_transactions = int(dbutils.widgets.get("num_transactions"))

# COMMAND ----------
from pyspark.sql.functions import col, rand, expr, when, round as spark_round, date_add, lit

sales_base = spark.range(0, num_transactions).withColumnRenamed("id", "transaction_id")

sales_base = (
    sales_base
    .withColumn("store_id", (rand(seed=1) * 50).cast("int") + 1)
    .withColumn("product_id", (rand(seed=2) * 2000).cast("int") + 1)
    .withColumn("customer_id", (rand(seed=3) * 50000).cast("int") + 1)
    .withColumn("quantity", (rand(seed=4) * 5).cast("int") + 1)
    .withColumn(
        "transaction_date",
        date_add(lit("2023-01-01"), (rand(seed=5) * 900).cast("int"))
    )
    .withColumn(
        "payment_method",
        when(rand(seed=6) < 0.4, "Credit Card")
        .when(rand(seed=6) < 0.65, "Debit Card")
        .when(rand(seed=6) < 0.85, "Cash")
        .otherwise("Digital Wallet")
    )
    .withColumn(
        "discount_pct",
        when(rand(seed=7) < 0.7, 0.0)
        .otherwise(spark_round(rand(seed=8) * 0.3, 2))
    )
    .withColumn(
        "is_returned",
        when(rand(seed=9) < 0.05, True).otherwise(False)
    )
)

# COMMAND ----------
products_lookup = spark.table(f"{catalog}.{schema}.products").select("product_id", "unit_price", "unit_cost")

sales_with_price = (
    sales_base
    .join(products_lookup, on="product_id", how="left")
    .withColumn("gross_amount", spark_round(col("unit_price") * col("quantity"), 2))
    .withColumn("discount_amount", spark_round(col("gross_amount") * col("discount_pct"), 2))
    .withColumn("net_amount", spark_round(col("gross_amount") - col("discount_amount"), 2))
)

# COMMAND ----------
(
    sales_with_price
    .write
    .mode("overwrite")
    .partitionBy("transaction_date")
    .format("delta")
    .saveAsTable(f"{catalog}.{schema}.sales_transactions")
)

# COMMAND ----------
row_count = spark.table(f"{catalog}.{schema}.sales_transactions").count()
print(f"Sales fact table generated successfully in {catalog}.{schema}")
print(f"Total transactions: {row_count}")