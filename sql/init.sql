CREATE TABLE cafe_sales_raw (
    transaction_id TEXT,
    item TEXT,
    quantity INTEGER,
    price_per_unit FLOAT,
    total_spent FLOAT,
    payment_method TEXT,
    location TEXT,
    transaction_date DATE
);

CREATE TABLE cafe_sales_clean (
    transaction_id TEXT,
    item TEXT,
    quantity INTEGER,
    price_per_unit FLOAT,
    total_spent FLOAT,
    payment_method TEXT,
    location TEXT,
    transaction_date DATE,
    spending_category TEXT,
    is_high_value_order BOOLEAN,
    processed_timestamp TIMESTAMP
);