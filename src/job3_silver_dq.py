import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--bronze_schema", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--gold_schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

# Create silver schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.silver_schema}")

orders_df = spark.table(f"{args.catalog}.{args.bronze_schema}.bronze_orders")
products_df = spark.table(f"{args.catalog}.{args.bronze_schema}.bronze_products")

# Simple DQ + enrichment
clean_orders_df = orders_df.filter(
    col("order_id").isNotNull() &
    col("product_id").isNotNull() &
    col("quantity").isNotNull() &
    col("amount").isNotNull()
)

silver_df = clean_orders_df.join(products_df, on="product_id", how="left")

silver_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.silver_schema}.silver_sales_clean"
)

print(
    f"[{args.env}] Created table "
    f"{args.catalog}.{args.silver_schema}.silver_sales_clean"
)