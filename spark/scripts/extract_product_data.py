from pyspark.sql import SparkSession
import json
import os


# Initialize Spark Session
spark = SparkSession.builder.appName('Extract product data').getOrCreate()

# Read service account keyfile
f = open ('/opt/airflow/keyfile/keyfile.json', 'r')
service_account = json.loads(f.read())

# Setup spark config
spark.conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark.conf.set('fs.gs.auth.service.account.enable', 'true')
spark.conf.set("fs.gs.auth.service.account.private.key.id", service_account['private_key_id'])
spark.conf.set("fs.gs.auth.service.account.email", service_account['client_email'])
spark.conf.set("fs.gs.auth.service.account.private.key", service_account['private_key'])

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
query = '''(
    SELECT *
    FROM public.products
    ) tmp_table
'''

# Read data from PostgreSQL into a DataFrame
df = spark.read.jdbc(url=pg_url, table=query, properties=pg_properties)

# Write data from PostgreSQL into a DataFrame
df.write.mode('overwrite').format('parquet').save('gs://ecommerce-data-pipeline/raw/product')

spark.stop()
