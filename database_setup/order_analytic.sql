CREATE OR REPLACE EXTERNAL TABLE order_analytic.sales_by_product (
    product_category STRING,
    product_name STRING,
    total_quantity_sold INT64,
    total_gross_revenue NUMERIC,
    total_discount_given NUMERIC,
    total_net_revenue NUMERIC
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ecommerce-data-pipeline/refine/sales_by_product/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE order_analytic.sales_by_campaign (
    campaign_type STRING,
    platform STRING,
    total_orders INT64,
    total_gross_revenue NUMERIC,
    total_discount NUMERIC,
    total_net_revenue NUMERIC
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ecommerce-data-pipeline/refine/sales_by_campaign/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE order_analytic.customer_purchase_behavior (
    customer_id INT64,
    country STRING,
    age INT64,
    job STRING,
    total_orders INT64,
    total_spent NUMERIC,
    avg_order_value NUMERIC,
    last_order_date TIMESTAMP
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ecommerce-data-pipeline/refine/customer_purchase_behavior/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE order_analytic.daily_sales_summary (
    order_date STRING,
    total_orders INT64,
    total_quantity_sold INT64,
    total_gross_revenue NUMERIC,
    total_discount NUMERIC,
    total_net_revenue NUMERIC
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ecommerce-data-pipeline/refine/daily_sales_summary/*.parquet']
);
