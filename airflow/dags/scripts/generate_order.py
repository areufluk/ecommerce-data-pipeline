import random
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

import os
import pandas as pd
import numpy as np
from urllib.parse import urlparse
from datetime import timedelta, datetime


def postgresql_connection():
    connection_string = os.getenv('SALES_DB')
    connection_parse = urlparse(connection_string)
    connection = psycopg2.connect(
        user=connection_parse.username,
        password=connection_parse.password,
        host=connection_parse.hostname,
        port=connection_parse.port,
        database=connection_parse.path[1:]
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
    PRODUCT_LIST = ['watch', 'necklace', 'bracelet', 'earrings', 'wallet']
    PRODUCT_PRICE_DICT = {
        'watch': 1000,
        'necklace': 800,
        'bracelet': 500,
        'earrings': 1200,
        'wallet': 1500
    }
    STORE_LOCATION_LIST = ['bangkok', 'phuket', 'chiang mai', 'khon kaen']

    MIN_ORDER_AMOUNT = 50
    MAX_ORDER_AMOUNT = 50

    if not start_date:
        start_date = datetime.today().date() - timedelta(days=1)

    if not end_date:
        end_date = start_date

    date_range_list = pd.date_range(start_date, end_date).date.tolist()

    # Create database connection
    sales_database = postgresql_connection()

    columns = ['order_datetime', 'order_id', 'product', 'quantity', 'price', 'store_location']
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

        # Random product into each order
        df_order['product'] = df_order['order_id'].apply(
            lambda row: random.choice(PRODUCT_LIST)
        )

        # Random quantity between 1-5 and multiply to product price
        df_order['quantity'] = df_order['order_id'].apply(
            lambda row: random.randint(1, 5)
        )
        df_order['price'] = df_order.apply(
            lambda row: PRODUCT_PRICE_DICT[row['product']] * row['quantity'], axis=1
        )

        # Random store location into each order
        df_order['store_location'] = df_order['order_id'].apply(
            lambda row: random.choice(STORE_LOCATION_LIST)
        )

        if df_orders.empty:
            df_orders = df_order
        else:
            df_orders = pd.concat([df_orders, df_order], axis=0)

    # Query exists data to check which order should created or updated
    sales_database.execute('''
        SELECT order_id
        FROM order_data
        WHERE order_datetime::DATE BETWEEN \'{start_date}\' AND \'{end_date}\';
    '''.format(start_date=start_date, end_date=end_date))
    df_exists = pd.DataFrame.from_records(sales_database.fetchall(), columns=['order_id'])

    df_orders = df_orders.merge(df_exists, how='outer', on='order_id', indicator=True)

    # The 'left_only' data is new data that have to be created
    created_data = df_orders[df_orders['_merge'] == 'left_only'][columns].to_dict('records')
    
    insert_statement = 'INSERT INTO order_data ({}) VALUES %s'.format(','.join(columns))
    values = [[value for value in data.values()] for data in created_data]
    execute_values(sales_database, insert_statement, values)

    # The 'both' data is old data that exists in google sheet and have to be updated
    updated_data = df_orders[df_orders['_merge'] == 'both'][columns].to_dict('records')
    upsert_statement = '''
        UPDATE order_data AS old_data
        SET 
            order_datetime = new_data.order_datetime,
            product = new_data.product,
            quantity = new_data.quantity,
            price = new_data.price,
            store_location = new_data.store_location
        FROM (VALUES %s) AS new_data({}) 
        WHERE new_data.order_id = old_data.order_id;
    '''.format(','.join(columns))
    values = [[value for value in data.values()] for data in updated_data]
    execute_values(sales_database, upsert_statement, values)
