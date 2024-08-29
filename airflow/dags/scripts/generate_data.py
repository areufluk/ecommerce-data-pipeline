import random
import psycopg2
import psycopg2.extras
import os
from psycopg2.extras import execute_values
from urllib.parse import urlparse


# List of campaign types
CAMPAIGN_TYPES = [
    'Brand Awareness', 'Product Launch', 'Seasonal Promotion', 'Flash Sale', 
    'Customer Retention', 'New Customer Acquisition', 'Cross-Selling',
    'Upselling', 'Event Promotion', 'Loyalty Program'
]

CAMPAIGN_PLATFORMS = [
    'Facebook', 'Instagram', 'Google Ads', 'LinkedIn',
    'Twitter', 'YouTube', 'TikTok', 'Reddit'
]

# List of job industry types
JOB_INDUSTRY_TYPES = [
    'IT', 'Healthcare', 'Finance & Banking', 'Education', 'Manufacturing',
    'Retail', 'Marketing & Advertising', 'Hospitality & Tourism', 'Construction',
    'Media & Entertainment', 'Real Estate', 'Automotive', 'Telecommunications',
    'Transportation & Logistics', 'Energy & Utilities', 'Legal Services', 
    'Government & Public Administration', 'Agriculture', 'Food & Beverage',
    'Pharmaceutical & Biotechnology', 'Aerospace & Defense', 'Insurance',
    'Nonprofit & Social Services', 'Engineering & Architecture', 'Arts & Design',
    'Human Resources & Staffing', 'E-commerce', 'Environmental Services',
    'Fashion & Apparel', 'Sports & Recreation'
]

# List of popular countries
COUNTRIES = [
    'United States', 'China', 'India', 'Japan', 'Germany', 'United Kingdom', 'France',
    'Brazil', 'Canada', 'Russia', 'Australia', 'Italy', 'South Korea', 'Spain', 'Mexico',
    'Indonesia', 'Turkey', 'Saudi Arabia', 'South Africa', 'Argentina', 'Netherlands',
    'Switzerland', 'Sweden', 'Poland', 'Belgium', 'Norway', 'Austria', 'Thailand', 'Malaysia',
    'Singapore', 'Israel', 'Vietnam', 'Egypt', 'Philippines', 'Chile', 'Nigeria', 'Pakistan',
    'Bangladesh', 'Greece', 'Portugal', 'Denmark', 'Finland', 'Ireland', 'New Zealand',
    'Ukraine', 'United Arab Emirates', 'Colombia', 'Peru', 'Venezuela', 'Hungary'
]

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

def generate_customer(number_of_customers: int):
    created_data = list()
    MIN_AGE = 15
    MAX_AGE = 70

    for i in range(number_of_customers):
        data = {
            'country': random.choice(COUNTRIES),
            'age': random.randint(MIN_AGE, MAX_AGE),
            'job': random.choice(JOB_INDUSTRY_TYPES)
        }
        created_data.append(data)
    
    # Create database connection
    sales_database = postgresql_connection()
    columns = ['country', 'age', 'job']

    insert_statement = 'INSERT INTO customers ({}) VALUES %s'.format(','.join(columns))
    values = [[value for value in data.values()] for data in created_data]
    execute_values(sales_database, insert_statement, values)

def generate_campaign(number_of_campaigns: int):
    created_data = list()
    MIN_DISCOUNT = 5
    MAX_DISCOUNT = 50

    for i in range(number_of_campaigns):
        data = {
            'campaign_type': random.choice(CAMPAIGN_TYPES),
            'discount': random.randint(MIN_DISCOUNT, MAX_DISCOUNT),
            'platform': random.choice(CAMPAIGN_PLATFORMS)
        }
        created_data.append(data)
    
    # Create database connection
    sales_database = postgresql_connection()
    columns = ['campaign_type', 'discount', 'platform']

    insert_statement = 'INSERT INTO campaigns ({}) VALUES %s'.format(','.join(columns))
    values = [[value for value in data.values()] for data in created_data]
    execute_values(sales_database, insert_statement, values)

def generate_product():
    created_data = [
        {'product_name': 'Smartphone X Pro', 'product_category': 'Electronics', 'price': 34900},
        {'product_name': 'Wireless Earbuds', 'product_category': 'Electronics', 'price': 6900},
        {'product_name': 'Gaming Laptop', 'product_category': 'Electronics', 'price': 52900},
        {'product_name': '4K Ultra HD TV', 'product_category': 'Electronics', 'price': 27900},
        {'product_name': 'Bluetooth Speaker', 'product_category': 'Electronics', 'price': 5200},
        
        {'product_name': 'Stainless Steel Refrigerator', 'product_category': 'Home Appliances', 'price': 42000},
        {'product_name': 'Top Load Washing Machine', 'product_category': 'Home Appliances', 'price': 17500},
        {'product_name': 'Microwave Oven', 'product_category': 'Home Appliances', 'price': 5200},
        {'product_name': 'Air Purifier', 'product_category': 'Home Appliances', 'price': 10500},
        {'product_name': 'Dishwasher', 'product_category': 'Home Appliances', 'price': 24500},
        
        {'product_name': 'Leather Sofa', 'product_category': 'Furniture', 'price': 52500},
        {'product_name': 'King Size Bed', 'product_category': 'Furniture', 'price': 42000},
        {'product_name': 'Dining Table Set', 'product_category': 'Furniture', 'price': 28000},
        {'product_name': 'Office Desk', 'product_category': 'Furniture', 'price': 8700},
        {'product_name': 'Bookshelf', 'product_category': 'Furniture', 'price': 7000},
        
        {'product_name': 'Men\'s Running Shoes', 'product_category': 'Footwear', 'price': 4200},
        {'product_name': 'Women\'s Sandals', 'product_category': 'Footwear', 'price': 2600},
        {'product_name': 'Hiking Boots', 'product_category': 'Footwear', 'price': 5200},
        {'product_name': 'Leather Formal Shoes', 'product_category': 'Footwear', 'price': 7000},
        {'product_name': 'Casual Sneakers', 'product_category': 'Footwear', 'price': 3150},
        
        {'product_name': 'Historical Fiction Novel', 'product_category': 'Books', 'price': 700},
        {'product_name': 'Science Fiction Anthology', 'product_category': 'Books', 'price': 875},
        {'product_name': 'Cookbook for Beginners', 'product_category': 'Books', 'price': 1050},
        {'product_name': 'Self-Help Guide', 'product_category': 'Books', 'price': 630},
        {'product_name': 'Children\'s Storybook', 'product_category': 'Books', 'price': 525},
        
        {'product_name': 'Face Moisturizer', 'product_category': 'Beauty Products', 'price': 1750},
        {'product_name': 'Lipstick Set', 'product_category': 'Beauty Products', 'price': 1400},
        {'product_name': 'Shampoo & Conditioner', 'product_category': 'Beauty Products', 'price': 875},
        {'product_name': 'Perfume Spray', 'product_category': 'Beauty Products', 'price': 2100},
        {'product_name': 'Nail Polish Kit', 'product_category': 'Beauty Products', 'price': 700},
        
        {'product_name': 'Yoga Mat', 'product_category': 'Sports Equipment', 'price': 1050},
        {'product_name': 'Dumbbell Set', 'product_category': 'Sports Equipment', 'price': 3500},
        {'product_name': 'Tennis Racket', 'product_category': 'Sports Equipment', 'price': 5250},
        {'product_name': 'Soccer Ball', 'product_category': 'Sports Equipment', 'price': 875},
        {'product_name': 'Treadmill', 'product_category': 'Sports Equipment', 'price': 28000},
        
        {'product_name': 'Action Figure Set', 'product_category': 'Toys & Games', 'price': 1400},
        {'product_name': 'Puzzle Game', 'product_category': 'Toys & Games', 'price': 700},
        {'product_name': 'Board Game Collection', 'product_category': 'Toys & Games', 'price': 2100},
        {'product_name': 'Remote Control Car', 'product_category': 'Toys & Games', 'price': 2800},
        {'product_name': 'Dollhouse', 'product_category': 'Toys & Games', 'price': 4200},
        
        {'product_name': 'Non-Stick Frying Pan', 'product_category': 'Kitchenware', 'price': 1225},
        {'product_name': 'Cutlery Set', 'product_category': 'Kitchenware', 'price': 1750},
        {'product_name': 'Blender', 'product_category': 'Kitchenware', 'price': 2450},
        {'product_name': 'Pressure Cooker', 'product_category': 'Kitchenware', 'price': 3500},
        {'product_name': 'Glassware Set', 'product_category': 'Kitchenware', 'price': 1575},
        
        {'product_name': 'Car Phone Mount', 'product_category': 'Automotive Accessories', 'price': 875},
        {'product_name': 'Seat Covers', 'product_category': 'Automotive Accessories', 'price': 3500},
        {'product_name': 'Car Vacuum Cleaner', 'product_category': 'Automotive Accessories', 'price': 2625},
        {'product_name': 'Dash Cam', 'product_category': 'Automotive Accessories', 'price': 5250},
        {'product_name': 'Tire Inflator', 'product_category': 'Automotive Accessories', 'price': 1750}
    ]
    
    # Create database connection
    sales_database = postgresql_connection()
    columns = ['product_name', 'product_category', 'price']

    insert_statement = 'INSERT INTO products ({}) VALUES %s'.format(','.join(columns))
    values = [[value for value in data.values()] for data in created_data]
    execute_values(sales_database, insert_statement, values)

if __name__ == '__main__':
    generate_customer(20000)
    generate_campaign(300)
    generate_product()
