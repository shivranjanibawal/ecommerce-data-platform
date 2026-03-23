from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys
from datetime import date

args = getResolvedOptions(sys.argv, ['BUCKET', 'PG_URL', 'PG_USER', 'PG_PASS', 'LAST_RUN'])
glueContext = GlueContext(SparkContext.getOrCreate())

# Pull only rows changed since last run (watermark-based CDC)
df = glueContext.read.format("jdbc").options(
    url=args['PG_URL'],
    dbtable=f"(SELECT * FROM customers WHERE updated_at > '{args['LAST_RUN']}') AS cdc",
    user=args['PG_USER'],
    password=args['PG_PASS']
).load()

# Write as Parquet, partitioned by date
output_path = f"s3://{args['BUCKET']}/customers/year={date.today().year}/month={date.today().month:02d}/day={date.today().day:02d}/"
df.write.mode("overwrite").parquet(output_path)
print(f"Written {df.count()} customer change records")