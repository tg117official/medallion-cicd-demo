import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

data = [
    ("P100", "Laptop", "Electronics"),
    ("P101", "Phone", "Electronics"),
    ("P102", "Chair", "Furniture")
]

df = spark.createDataFrame(data, ["product_id", "product_name", "category"])

df.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.schema}.bronze_products"
)

print(f"[{args.env}] bronze_products created")