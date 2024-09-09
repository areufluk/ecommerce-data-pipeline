from pyspark.sql import SparkSession
from datetime import timedelta, datetime
from pyspark.sql.functions import sum, count, max, date_format
import json

# Initialize Spark Session
spark = SparkSession.builder.appName('Transform analytic data').getOrCreate()

# Read service account keyfile
f = open ('/opt/airflow/keyfile/keyfile.json', 'r')
service_account = json.loads(f.read())

# Setup spark config
spark.conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark.conf.set('fs.gs.auth.service.account.enable', 'true')
spark.conf.set("fs.gs.auth.service.account.private.key.id", service_account['private_key_id'])
spark.conf.set("fs.gs.auth.service.account.email", service_account['client_email'])
spark.conf.set("fs.gs.auth.service.account.private.key", service_account['private_key'])

# Read order data from GCS
order_data_path = f'gs://ecommerce-data-pipeline/transform/order/'
df_order = spark.read.parquet(order_data_path)

sales_by_product = df_order.groupBy("product_category", "product_name").agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("gross_price").alias("total_gross_revenue"),
    sum("discount").alias("total_discount_given"),
    sum("net_price").alias("total_net_revenue")
)

# Write data from PostgreSQL into a DataFrame
sales_by_product.write.mode('overwrite').format('parquet').save('gs://ecommerce-data-pipeline/refine/sales_by_product')

sales_by_campaign = df_order.groupBy("campaign_type", "platform").agg(
    count("order_id").alias("total_orders"),
    sum("gross_price").alias("total_gross_revenue"),
    sum("discount").alias("total_discount"),
    sum("net_price").alias("total_net_revenue")
)

# Write data from PostgreSQL into a DataFrame
sales_by_campaign.write.mode('overwrite').format('parquet').save('gs://ecommerce-data-pipeline/refine/sales_by_campaign')

customer_purchase_behavior = df_order.groupBy("customer_id", "country", "age", "job").agg(
    count("order_id").alias("total_orders"),
    sum("net_price").alias("total_spent"),
    (sum("net_price") / count("order_id")).alias("avg_order_value"),
    max("order_datetime").alias("last_order_date")
)

# Write data from PostgreSQL into a DataFrame
customer_purchase_behavior.write.mode('overwrite').format('parquet').save('gs://ecommerce-data-pipeline/refine/customer_purchase_behavior')


daily_sales_summary = df_order.groupBy(date_format("order_datetime", "yyyy-MM-dd").alias("order_date")).agg(
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_quantity_sold"),
    sum("gross_price").alias("total_gross_revenue"),
    sum("discount").alias("total_discount"),
    sum("net_price").alias("total_net_revenue")
)

# Write data from PostgreSQL into a DataFrame
daily_sales_summary.write.mode('overwrite').format('parquet').save('gs://ecommerce-data-pipeline/refine/daily_sales_summary')
