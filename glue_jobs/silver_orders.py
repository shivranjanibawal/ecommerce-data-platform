from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sys

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Install delta at job level via --extra-jars or Glue connector
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

BRONZE_PATH = "s3://dev-ecommerce-bronze-layer/orders/"
SILVER_PATH = "s3://dev-ecommerce-silver-layer/orders/"

# Read today's bronze data
raw_df = spark.read.json(BRONZE_PATH)

# 1. SCHEMA VALIDATION — enforce expected columns exist
expected_cols = {'order_id', 'customer_id', 'total_amount', 'status', 'created_at'}
actual_cols   = set(raw_df.columns)
missing       = expected_cols - actual_cols
if missing:
    raise ValueError(f"Schema mismatch! Missing columns: {missing}")

# 2. DEDUPLICATION — keep latest record per order_id
clean_df = (
    raw_df
    .dropDuplicates(['order_id'])
    .filter(F.col('order_id').isNotNull())
    .filter(F.col('total_amount') >= 0)
    # Standardize column names + types
    .withColumn('order_id',    F.col('order_id').cast('string'))
    .withColumn('total_amount',F.col('total_amount').cast('double'))
    .withColumn('created_at',  F.to_timestamp('created_at'))
    .withColumn('status',      F.lower(F.trim(F.col('status'))))
    .withColumn('_ingested_at',F.current_timestamp())
)

# 3. WRITE AS DELTA (upsert by order_id to avoid duplicates on reruns)
from delta.tables import DeltaTable
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    delta_table.alias("existing").merge(
        clean_df.alias("updates"),
        "existing.order_id = updates.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    clean_df.write.format("delta").partitionBy("created_at").save(SILVER_PATH)

print(f"Silver orders written. Row count: {clean_df.count()}")