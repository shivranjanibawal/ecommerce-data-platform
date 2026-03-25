# great_expectations/validate_silver_orders.py
import great_expectations as gx
import pandas as pd

context = gx.get_context()

# Load the silver data for today's partition
df = pd.read_parquet("s3://dev-ecommerce-silver-layer/orders/")

# Create a validator
validator = context.sources.pandas_default.read_dataframe(df)

# Define expectations
validator.expect_column_to_exist("order_id")
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("total_amount", min_value=0, max_value=100000)
validator.expect_column_values_to_be_in_set("status", ["pending","completed","cancelled","refunded"])
validator.expect_column_values_to_not_be_null("created_at")

# Run and check
results = validator.validate()

if not results.success:
    # Send CloudWatch alert
    import boto3
    cw = boto3.client('cloudwatch')
    cw.put_metric_data(
        Namespace='EcommercePipeline',
        MetricData=[{
            'MetricName': 'DataQualityFailures',
            'Value': len([r for r in results.results if not r.success]),
            'Unit': 'Count'
        }]
    )
    raise Exception(f"Data quality checks failed! See results: {results}")

print("All data quality checks passed!")