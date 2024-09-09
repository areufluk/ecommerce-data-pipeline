from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format
from pyspark.sql.functions import sum, count, max, date_format
from datetime import timedelta, datetime
import json


# Initialize Spark Session
spark = SparkSession.builder.appName('Transform order data').getOrCreate()

# Read service account keyfile
f = open ('/opt/airflow/keyfile/keyfile.json', 'r')
service_account = json.loads(f.read())

# Setup spark config
spark.conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark.conf.set('fs.gs.auth.service.account.enable', 'true')
spark.conf.set("fs.gs.auth.service.account.private.key.id", service_account['private_key_id'])
spark.conf.set("fs.gs.auth.service.account.email", service_account['client_email'])
spark.conf.set("fs.gs.auth.service.account.private.key", service_account['private_key'])
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Read order data from GCS
order_data_path = f'gs://ecommerce-data-pipeline/raw/order/'
df_order = spark.read.parquet(order_data_path)

# Read customer data from GCS
customer_data_path = f'gs://ecommerce-data-pipeline/raw/customer'
df_customer = spark.read.parquet(customer_data_path)

# Join df_customer to df_order with 'customer_id'
df_order = df_order.join(df_customer, df_order.customer_id == df_customer.id, 'left')

# Read campaign data from GCS
campaign_data_path = f'gs://ecommerce-data-pipeline/raw/campaign'
df_campaign = spark.read.parquet(campaign_data_path)

# Join df_campaign to df_order with 'campaign_id'
df_order = df_order.join(df_campaign.select('id', 'campaign_type', 'platform'), df_order.campaign_id == df_campaign.id, 'left')

# Read customer data from GCS
product_data_path = f'gs://ecommerce-data-pipeline/raw/product'
df_product = spark.read.parquet(product_data_path)

# Join df_product to df_order with 'product_id'
df_order = df_order.join(df_product, df_order.product_id == df_product.id, 'left')

# Extract date and time column from datetime
df_order = df_order \
    .withColumn('order_date', to_date(df_order['order_datetime'])) \
    .withColumn('order_time', date_format(df_order['order_datetime'], 'HH:mm:ss'))

df_order = df_order.select(
    'order_id', 'order_datetime', 'order_date', 'order_time',
    'quantity', 'gross_price', 'discount', 'net_price', 
    'customer_id', 'country', 'age', 'job',
    'campaign_type', 'platform',
    'product_name', 'product_category'
)

# Write data from PostgreSQL into a DataFrame
df_order.write.mode('overwrite').format('parquet').partitionBy('order_date').save('gs://ecommerce-data-pipeline/transform/order')

spark.stop()
