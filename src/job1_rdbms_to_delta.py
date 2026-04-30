import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--bronze_schema", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--gold_schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

print("Pipeline started")
# Use catalog first
spark.sql(f"USE CATALOG {args.catalog}")

# Create bronze schema inside the selected catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.bronze_schema}")

# Simulated RDBMS extract -> bronze
data = [
    (1, "C001", "P100", 2, 500.0, "2026-04-15"),
    (2, "C002", "P101", 1, 1200.0, "2026-04-15"),
    (3, "C003", "P102", 3, 300.0, "2026-04-15"),
    (4, "C004", "P103", 1, 950.0, "2026-04-16"),
    (5, "C005", "P104", 4, 150.0, "2026-04-16")
]

df = spark.createDataFrame(
    data,
    ["order_id", "customer_id", "product_id", "quantity", "amount", "order_date"]
)

target_table = f"{args.bronze_schema}.bronze_orders"

df.write.mode("overwrite").format("delta").saveAsTable(target_table)

print(
    f"[{args.env}] Created table "
    f"{args.catalog}.{target_table}"
)