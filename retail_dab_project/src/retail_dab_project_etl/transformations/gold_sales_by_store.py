import dlt
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count

@dlt.table(
    name="gold.daily_sales_by_store",
    comment="Daily aggregated sales revenue and transaction counts per store"
)
def daily_sales_by_store():
    sales = dlt.read("sales_cleaned")
    stores = spark.table("retail_dab_dev.raw.stores")

    joined = sales.join(stores, on="store_id", how="left")

    return (
        joined
        .groupBy("transaction_date", "store_id", "store_name", "region", "store_type")
        .agg(
            spark_sum("net_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_units_sold"),
            spark_count("transaction_id").alias("transaction_count")
        )
    )