import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

orders = spark.table(f"{args.catalog}.{args.schema}.bronze_orders")
products = spark.table(f"{args.catalog}.{args.schema}.bronze_products")

valid_orders = (
    orders
    .filter(F.col("order_id").isNotNull())          # rule 1
    .filter(F.col("customer_id").isNotNull())       # rule 2
    .filter(F.col("product_id").isNotNull())        # rule 3
    .filter(F.col("quantity") > 0)                  # rule 4
    .filter(F.col("amount") > 0)                    # rule 5
)

silver = (
    valid_orders.alias("o")
    .join(products.alias("p"), "product_id", "left")
    .withColumn("dq_passed", F.lit(True))
)

silver.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.schema}.silver_sales_clean"
)

print(f"[{args.env}] silver_sales_clean created")