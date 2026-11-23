CREATE SCHEMA IF NOT EXISTS olist;

CREATE TABLE olist.dim_geography (
    geography_key INT PRIMARY KEY, state_code VARCHAR(5),
    state_name VARCHAR(60), region_name VARCHAR(30)
) DISTSTYLE ALL;

CREATE TABLE olist.dim_customer (
    customer_key INT PRIMARY KEY, customer_id VARCHAR(40),
    customer_unique_id VARCHAR(40), customer_city VARCHAR(80),
    customer_state VARCHAR(5), customer_zip_code_prefix VARCHAR(10),
    geography_key INT,
    data_source VARCHAR(10)              -- CHANGE: history / api
);

CREATE TABLE olist.dim_seller (
    seller_key INT PRIMARY KEY, seller_id VARCHAR(40),
    seller_city VARCHAR(80), seller_state VARCHAR(5),
    seller_zip_code_prefix VARCHAR(10), geography_key INT,
    data_source VARCHAR(10)              -- CHANGE: history / api
) DISTSTYLE ALL;

CREATE TABLE olist.dim_product (
    product_key INT PRIMARY KEY, product_id VARCHAR(40),
    category_pt VARCHAR(80), category_en VARCHAR(80),
    product_weight_g DOUBLE PRECISION, product_photos_qty INT,
    data_source VARCHAR(10)              -- CHANGE: history / api
);

CREATE TABLE olist.dim_payment_type (
    payment_type_key INT PRIMARY KEY, payment_type VARCHAR(30)
) DISTSTYLE ALL;

CREATE TABLE olist.dim_date (
    date_key INT PRIMARY KEY, full_date DATE, cal_year INT, cal_quarter INT,
    cal_month INT, month_name VARCHAR(12), day_of_month INT,
    day_name VARCHAR(12), is_weekend INT
) DISTSTYLE ALL;

CREATE TABLE olist.fact_sales (
    order_id VARCHAR(40), order_item_id INT, customer_key INT, seller_key INT,
    product_key INT, purchase_date_key INT, delivered_date_key INT,
    estimated_date_key INT, order_status VARCHAR(20),
    price DECIMAL(12,2), freight_value DECIMAL(12,2), item_total DECIMAL(12,2),
    data_source VARCHAR(10)              -- CHANGE: history / api
) DISTKEY(order_id) SORTKEY(purchase_date_key);

CREATE TABLE olist.fact_orders (
    order_id VARCHAR(40), customer_key INT, purchase_date_key INT,
    delivered_date_key INT, estimated_date_key INT, order_status VARCHAR(20),
    item_count INT, items_value DECIMAL(12,2), freight_total DECIMAL(12,2),
    payment_total DECIMAL(12,2), max_installments INT, avg_review DECIMAL(3,2),
    delivery_days INT, delay_days INT, is_late INT,
    data_source VARCHAR(10)              -- CHANGE: history / api
) DISTKEY(order_id) SORTKEY(purchase_date_key);

CREATE TABLE olist.fact_payments (
    order_id VARCHAR(40), payment_sequential INT, customer_key INT,
    payment_type_key INT, purchase_date_key INT, payment_installments INT,
    payment_value DECIMAL(12,2),
    data_source VARCHAR(10)              -- CHANGE: history / api
) DISTKEY(order_id) SORTKEY(purchase_date_key);