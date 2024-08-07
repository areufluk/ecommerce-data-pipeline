CREATE TABLE order_data (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL,
    order_datetime TIMESTAMP NOT NULL,
	product VARCHAR(60) NOT NULL,
	store_location VARCHAR(60) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);