from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from datetime import timedelta, datetime
import json
import os
import sys


# Initialize Spark Session
spark = SparkSession.builder.appName('Extract raw order data').getOrCreate()

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

# Define PostgreSQL connection properties
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
SALES_DB = os.getenv('SALES_DB')
pg_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{SALES_DB}"
pg_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# SQL query to extract data
start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
query = '''(
    SELECT *
    FROM public.orders
    WHERE order_datetime::DATE BETWEEN \'{start_date}\' AND \'{end_date}\'
    ) tmp_table
'''.format(
    start_date=start_date,
    end_date=end_date
)

# Read data from PostgreSQL into a DataFrame
df = spark.read.jdbc(url=pg_url, table=query, properties=pg_properties)

# Extract date from order_datetime column
df = df.withColumn('order_date', to_date(df['order_datetime']))

# Write data from PostgreSQL into a DataFrame
df.write.mode('overwrite').format('parquet').partitionBy('order_date').save('gs://ecommerce-data-pipeline/raw/order')

spark.stop()
