import random
import psycopg2
import os
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_random_times(num_times: int) -> list:
    random_times = []
    for _ in range(num_times):
        hour = np.random.randint(0, 24)
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        random_times.append(timedelta(hours=hour, minutes=minute, seconds=second))
    return random_times

def create_order(start_date: str, end_date: str = None):
    PRODUCT_LIST = ['watch', 'necklace', 'bracelet', 'earrings', 'wallet']
    PRODUCT_PRICE_DICT = {
        'watch': 1000,
        'necklace': 800,
        'bracelet': 500,
        'earrings': 1200,
        'wallet': 1500
    }
    STORE_LOCATION_LIST = ['bangkok', 'phuket', 'chiang mai', 'khon kaen']

    MIN_ORDER_AMOUNT = 30
    MAX_ORDER_AMOUNT = 50

    if not end_date:
        end_date = start_date
    date_range_list = pd.date_range(start_date, end_date).date.tolist()

    for date_obj in date_range_list:
        created_order_amount = random.randint(MIN_ORDER_AMOUNT, MAX_ORDER_AMOUNT)
        
        # generate random time in each day
        date_str = date_obj.strftime('%Y%m%d')
        random_times = generate_random_times(created_order_amount)
        df_order = pd.DataFrame(random_times, columns=['time'])
        df_order['order_datetime'] = pd.to_datetime(date_str) + df_order['time']
        df_order = df_order.sort_values('datetime').reset_index(drop=True)
        del df_order['time']

        # generate order id (run id number from last order in the day)
        # order id template : ORD20240701-000001
        df_order['order_index'] = df_order.index + 1
        df_order['order_id'] = df_order['order_index'].apply(
            lambda row: f'ORD{date_str}-{str(row).zfill(6)}'
        )
        del df_order['order_index']

        # random product into each order
        df_order['product'] = df_order['order_id'].apply(
            lambda row: random.choice(PRODUCT_LIST)
        )

        # random quantity between 1-5 and multiply to product price
        df_order['quantity'] = df_order['order_id'].apply(
            lambda row: random.randint(1, 5)
        )
        df_order['price'] = df_order.apply(
            lambda row: PRODUCT_PRICE_DICT[row['product']] * row['quantity'], axis=1
        )

        # random store location into each order
        df_order['store_location'] = df_order['order_id'].apply(
            lambda row: random.choice(STORE_LOCATION_LIST)
        )

        print(df_order.head(60))
