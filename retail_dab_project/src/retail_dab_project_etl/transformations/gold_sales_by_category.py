import dlt
from pyspark.sql.functions import sum as spark_sum, count as spark_count

@dlt.table(
    name="gold.daily_sales_by_category",
    comment="Daily aggregated sales revenue and discount spend per product category"
)
def daily_sales_by_category():
    sales = dlt.read("sales_cleaned")
    products = spark.table("retail_dab_dev.raw.products")

    joined = sales.join(products, on="product_id", how="left")

    return (
        joined
        .groupBy("transaction_date", "category")
        .agg(
            spark_sum("net_amount").alias("total_revenue"),
            spark_sum("discount_amount").alias("total_discount_given"),
            spark_sum("quantity").alias("total_units_sold"),
            spark_count("transaction_id").alias("transaction_count")
        )
    )