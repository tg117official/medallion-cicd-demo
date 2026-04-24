import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--bronze_schema", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--gold_schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

# Create gold schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.gold_schema}")

silver_df = spark.table(f"{args.catalog}.{args.silver_schema}.silver_sales_clean")

gold_df = (
    silver_df.groupBy("product_id", "product_name", "category")
    .agg(
        count("*").alias("total_orders"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("amount").alias("total_sales_amount")
    )
)

gold_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.gold_schema}.gold_sales_summary"
)

print(
    f"[{args.env}] Created table "
    f"{args.catalog}.{args.gold_schema}.gold_sales_summary"
)