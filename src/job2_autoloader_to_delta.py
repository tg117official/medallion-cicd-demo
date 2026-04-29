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

# Use the target catalog first
spark.sql(f"USE CATALOG {args.catalog}")

# Create bronze schema inside the selected catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.bronze_schema}")

# Simulated file/Auto Loader landing -> bronze
data = [
    ("P100", "Laptop", "Electronics", 500.0),
    ("P101", "Phone", "Electronics", 1200.0),
    ("P102", "Headphones", "Accessories", 300.0),
    ("P103", "Keyboard", "Accessories", 950.0),
    ("P104", "Mouse", "Accessories", 150.0)
]

df = spark.createDataFrame(
    data,
    ["product_id", "product_name", "category", "list_price"]
)

target_table = f"{args.bronze_schema}.bronze_products"

df.write.mode("overwrite").format("delta").saveAsTable(target_table)

print(
    f"[{args.env}] Created table "
    f"{args.catalog}.{target_table}"
)