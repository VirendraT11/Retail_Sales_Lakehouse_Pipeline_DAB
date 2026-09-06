import dlt
from pyspark.sql.functions import sum as spark_sum, count as spark_count, min as spark_min, max as spark_max, round as spark_round, when

@dlt.table(
    name="gold.customer_lifetime_value",
    comment="All-time cumulative spend and order behavior per customer"
)
def customer_lifetime_value():
    sales = dlt.read("sales_cleaned")
    customers = spark.table("retail_dab_dev.raw.customers")

    joined = sales.join(customers, on="customer_id", how="left")

    aggregated = (
        joined
        .groupBy("customer_id", "first_name", "last_name", "loyalty_tier", "signup_date")
        .agg(
            spark_sum("net_amount").alias("total_lifetime_spend"),
            spark_count("transaction_id").alias("total_orders"),
            spark_min("transaction_date").alias("first_purchase_date"),
            spark_max("transaction_date").alias("last_purchase_date"),
            spark_sum(when(joined.is_returned == True, 1).otherwise(0)).alias("return_count")
        )
    )

    return aggregated.withColumn(
        "avg_order_value",
        spark_round(aggregated.total_lifetime_spend / aggregated.total_orders, 2)
    )