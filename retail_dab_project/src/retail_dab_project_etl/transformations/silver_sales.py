import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="sales_cleaned",
    comment="Deduplicated, validated sales transactions"
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_net_amount", "net_amount >= 0")
@dlt.expect_or_drop("valid_discount_pct", "discount_pct >= 0 AND discount_pct <= 1")
def sales_cleaned():
    raw_sales = spark.read.table("retail_dab_dev.raw.sales_transactions")

    deduped = raw_sales.dropDuplicates(["transaction_id"])

    return deduped