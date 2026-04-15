import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

data = [
    (1, "C001", "P100", 2, 500.0, "2026-04-15"),
    (2, "C002", "P101", 1, 1200.0, "2026-04-15"),
    (3, "C003", "P102", 3, 300.0, "2026-04-15")
]

df = spark.createDataFrame(
    data,
    ["order_id", "customer_id", "product_id", "quantity", "amount", "order_date"]
)

df.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.schema}.bronze_orders"
)

print(f"[{args.env}] bronze_orders created")