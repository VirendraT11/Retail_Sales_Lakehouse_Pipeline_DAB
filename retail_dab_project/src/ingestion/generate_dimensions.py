# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "retail_dab_dev")
dbutils.widgets.text("schema", "raw")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Landing zone for raw source data'")

# COMMAND ----------
from faker import Faker
import random

fake = Faker()
Faker.seed(42)
random.seed(42)

regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
store_types = ["Flagship", "Mall", "Outlet", "Online"]

num_stores = 50
stores_data = []

for store_id in range(1, num_stores + 1):
    stores_data.append({
        "store_id": store_id,
        "store_name": f"{fake.city()} {random.choice(store_types)}",
        "region": random.choice(regions),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "opened_date": fake.date_between(start_date="-10y", end_date="-1y"),
        "store_type": random.choice(store_types),
    })

stores_df = spark.createDataFrame(stores_data)
stores_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.stores")

# COMMAND ----------
categories_price_bands = {
    "Electronics": (50, 1200),
    "Apparel": (10, 150),
    "Grocery": (2, 40),
    "Home & Kitchen": (15, 500),
    "Sports & Outdoors": (10, 300),
    "Beauty": (5, 100),
    "Toys": (5, 150),
}

num_products = 2000
products_data = []

for product_id in range(1, num_products + 1):
    category = random.choice(list(categories_price_bands.keys()))
    price_min, price_max = categories_price_bands[category]
    unit_price = round(random.uniform(price_min, price_max), 2)
    cost_ratio = random.uniform(0.4, 0.75)
    unit_cost = round(unit_price * cost_ratio, 2)

    products_data.append({
        "product_id": product_id,
        "product_name": f"{fake.word().capitalize()} {category.split()[0]} {random.randint(100,999)}",
        "category": category,
        "unit_price": unit_price,
        "unit_cost": unit_cost,
        "brand": fake.company(),
        "launch_date": fake.date_between(start_date="-5y", end_date="today"),
    })

products_df = spark.createDataFrame(products_data)
products_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.products")

# COMMAND ----------
num_customers = 50000

loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
loyalty_weights = [0.55, 0.30, 0.12, 0.03]

customers_data = []

for customer_id in range(1, num_customers + 1):
    signup_date = fake.date_between(start_date="-6y", end_date="today")
    customers_data.append({
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "signup_date": signup_date,
        "loyalty_tier": random.choices(loyalty_tiers, weights=loyalty_weights, k=1)[0],
        "state": fake.state_abbr(),
    })

customers_df = spark.createDataFrame(customers_data)
customers_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customers")

# COMMAND ----------
print(f"Dimensions generated successfully in {catalog}.{schema}")
print(f"Stores: {stores_df.count()}, Products: {products_df.count()}, Customers: {customers_df.count()}")