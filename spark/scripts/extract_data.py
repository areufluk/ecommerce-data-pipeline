from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from datetime import timedelta, datetime
import json


# Initialize Spark Session
spark = SparkSession.builder.appName('Extract raw data').getOrCreate()

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
pg_url = "jdbc:postgresql://34.143.159.125:5432/sales-db"
pg_properties = {
    "user": "opd-airflow",
    "password": "4H96yEphgtmNO_xZXM4M",
    "driver": "org.postgresql.Driver"
}

# SQL query to extract data
order_date = datetime.today().date() - timedelta(days=1)
query = '''(
    SELECT *
    FROM public.order_data
    WHERE order_datetime::DATE = \'{order_date}\'
    ) tmp_table
'''.format(
    order_date=order_date.strftime('%Y-%m-%d')
)

# Read data from PostgreSQL into a DataFrame
df = spark.read.jdbc(url=pg_url, table=query, properties=pg_properties)

# Extract date from order_datetime column
df = df.withColumn('order_date', to_date(df['order_datetime']))

# Write data from PostgreSQL into a DataFrame
df.write.mode('overwrite').format('parquet').partitionBy('order_date').save('gs://ecommerce-data-pipeline/raw/order')

spark.stop()
