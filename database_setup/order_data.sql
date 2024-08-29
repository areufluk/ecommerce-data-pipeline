-- Create a new database
CREATE DATABASE "sales-db";

-- Connect to created database
\c sales-db;

-- Create new table in sales-db
DROP TABLE IF EXISTS customers;
CREATE TABLE customers(
    id SERIAL PRIMARY KEY,
    country VARCHAR(50) NOT NULL,
    age INT NOT NULL,
    job VARCHAR(100) NOT NULL
);

DROP TABLE IF EXISTS campaigns;
CREATE TABLE campaigns(
    id SERIAL PRIMARY KEY,
    campaign_type VARCHAR(30) NOT NULL,
    discount INT NOT NULL,
    platform VARCHAR(30) NOT NULL
);

DROP TABLE IF EXISTS products;
CREATE TABLE products(
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(30) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders(
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL,
    order_datetime TIMESTAMP NOT NULL,
    customer_id INT NOT NULL,
    campaign_id INT,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    gross_price DECIMAL(10, 2) NOT NULL,
    discount DECIMAL(10, 2) NOT NULL,
    net_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE SET NULL,
    FOREIGN KEY(campaign_id) REFERENCES campaigns(id) ON DELETE SET NULL,
    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE SET NULL
);