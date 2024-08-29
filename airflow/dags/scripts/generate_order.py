import random
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

import os
import pandas as pd
import numpy as np
from datetime import timedelta, datetime


def postgresql_connection():
    connection = psycopg2.connect(
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('SALES_DB')
    )
    connection.autocommit = True
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    return cursor


def generate_random_times(num_times: int) -> list:
    random_times = []
    for _ in range(num_times):
        hour = np.random.randint(0, 24)
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        random_times.append(timedelta(hours=hour, minutes=minute, seconds=second))
    return random_times


def create_or_update_order(start_date: str = None, end_date: str = None):
    MIN_ORDER_AMOUNT = 50
    MAX_ORDER_AMOUNT = 50

    if not start_date:
        start_date = datetime.today().date() - timedelta(days=1)

    if not end_date:
        end_date = start_date

    date_range_list = pd.date_range(start_date, end_date).date.tolist()

    # Create database connection
    sales_database = postgresql_connection()

    # Query customer data
    sales_database.execute('''
        SELECT MIN(id), MAX(id)
        FROM customers;
    '''.format(start_date=start_date, end_date=end_date))
    customers_id_range = sales_database.fetchall()[0]

    # Query campaign data
    sales_database.execute('''
        SELECT id, discount
        FROM campaigns;
    '''.format(start_date=start_date, end_date=end_date))
    campaigns = sales_database.fetchall()
    campaign_dict = {campaign[0]: campaign[1] for campaign in campaigns}
    campaigns_id_range = [campaigns[0][0], campaigns[-1][0]]

    # Query product data
    sales_database.execute('''
        SELECT id, price
        FROM products;
    '''.format(start_date=start_date, end_date=end_date))
    products = sales_database.fetchall()
    product_dict = {product[0]: product[1] for product in products}
    products_id_range = [products[0][0], products[-1][0]]

    columns = [
        'order_datetime', 'order_id', 'customer_id', 'campaign_id',
        'product_id', 'quantity', 'gross_price', 'discount', 'net_price'
    ]
    df_orders = pd.DataFrame(columns=columns)

    for date_obj in date_range_list:
        created_order_amount = random.randint(MIN_ORDER_AMOUNT, MAX_ORDER_AMOUNT)
        
        # Generate random time in each day
        date_str = date_obj.strftime('%Y%m%d')
        random_times = generate_random_times(created_order_amount)
        df_order = pd.DataFrame(random_times, columns=['time'])
        df_order['order_datetime'] = pd.to_datetime(date_str) + df_order['time']
        df_order = df_order.sort_values('order_datetime').reset_index(drop=True)
        del df_order['time']

        # Generate order id (run id number from last order in the day)
        # order id template : ORD20240701-000001
        df_order['order_index'] = df_order.index + 1
        df_order['order_id'] = df_order['order_index'].apply(
            lambda row: f'ORD{date_str}-{str(row).zfill(6)}'
        )
        del df_order['order_index']

        # Random customer into each order
        df_order['customer_id'] = df_order['order_id'].apply(
            lambda row: random.randint(customers_id_range[0], customers_id_range[1])
        )

        # Random product into each order
        df_order['product_id'] = df_order['order_id'].apply(
            lambda row: random.randint(products_id_range[0], products_id_range[1])
        )

        # Random quantity between 1-5 and multiply to product price
        df_order['quantity'] = df_order['order_id'].apply(
            lambda row: random.randint(1, 5)
        )
        df_order['gross_price'] = df_order.apply(
            lambda row: product_dict[row['product_id']] * row['quantity'], axis=1
        )

        # Random campaign into each order, get discount percentage
        df_order['campaign_id'] = df_order['order_id'].apply(
            lambda row: random.randint(campaigns_id_range[0], campaigns_id_range[1])
        )
        df_order['discount_percentage'] = df_order.apply(
            lambda row: campaign_dict[row['campaign_id']], axis=1
        )

        # Calculate discount
        df_order['discount'] = df_order.apply(
            lambda row: float(row['gross_price'] * row['discount_percentage']) / 100, axis=1
        )

        # Calculate net_price from gross_price and discount
        df_order['net_price'] = df_order.apply(
            lambda row: float(row['gross_price']) - row['discount'], axis=1
        )

        if df_orders.empty:
            df_orders = df_order
        else:
            df_orders = pd.concat([df_orders, df_order], axis=0)

    del df_orders['discount_percentage']
    print(df_orders.head(50))

    # Query exists data to check which order should created or updated
    sales_database.execute('''
        SELECT order_id
        FROM orders
        WHERE order_datetime::DATE BETWEEN \'{start_date}\' AND \'{end_date}\';
    '''.format(start_date=start_date, end_date=end_date))
    df_exists = pd.DataFrame.from_records(sales_database.fetchall(), columns=['order_id'])

    df_orders = df_orders.merge(df_exists, how='outer', on='order_id', indicator=True)

    # The 'left_only' data is new data that have to be created
    created_data = df_orders[df_orders['_merge'] == 'left_only'][columns].to_dict('records')
    
    insert_statement = 'INSERT INTO orders ({}) VALUES %s'.format(','.join(columns))
    values = [[value for value in data.values()] for data in created_data]
    execute_values(sales_database, insert_statement, values)

    # The 'both' data is old data that exists in google sheet and have to be updated
    updated_data = df_orders[df_orders['_merge'] == 'both'][columns].to_dict('records')
    upsert_statement = '''
        UPDATE orders AS old_data
        SET 
            order_datetime = new_data.order_datetime,
            campaign_id = new_data.campaign_id,
            product_id = new_data.product_id,
            quantity = new_data.quantity,
            gross_price = new_data.gross_price,
            discount = new_data.discount,
            net_price = new_data.net_price
        FROM (VALUES %s) AS new_data({}) 
        WHERE new_data.order_id = old_data.order_id;
    '''.format(','.join(columns))
    values = [[value for value in data.values()] for data in updated_data]
    execute_values(sales_database, upsert_statement, values)
