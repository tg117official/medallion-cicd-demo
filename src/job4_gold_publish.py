import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--env", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

silver = spark.table(f"{args.catalog}.{args.schema}.silver_sales_clean")

dim_product = (
    silver
    .select("product_id", "product_name", "category")
    .dropDuplicates()
)

fact_sales = (
    silver
    .groupBy("order_date", "product_id")
    .agg(
        F.sum("quantity").alias("total_qty"),
        F.sum("amount").alias("total_sales")
    )
)

nosql_ready = (
    silver
    .groupBy("customer_id")
    .agg(
        F.collect_list(
            F.struct("order_id", "product_id", "product_name", "amount", "order_date")
        ).alias("orders")
    )
)

dim_product.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.schema}.dim_product"
)

fact_sales.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.schema}.fact_sales_summary"
)

nosql_ready.write.mode("overwrite").format("delta").saveAsTable(
    f"{args.catalog}.{args.schema}.nosql_customer_orders"
)

print(f"[{args.env}] gold tables created")