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

# Create bronze schema if it does not exist
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.bronze_schema}")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev_main.bronze")

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

df.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.bronze_schema}.bronze_orders"
)

print(
    f"[{args.env}] Created table "
    f"{args.catalog}.{args.bronze_schema}.bronze_orders"
)