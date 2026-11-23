-- =====================================================================
-- Olist capstone - Step 4 fix: recreate 2 raw tables in Athena
-- Problem: the Glue crawler named the columns col0, col1... and kept the
--          header row as data, because every column in these CSVs is text.
-- Fix:     drop the table (S3 data is NOT deleted, EXTERNAL table)
--          and recreate it with OpenCSVSerde + skip the header row.
-- Database: olist_raw_db    Bucket: s3://olistcap-data-2026/
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1) raw_orders
-- ---------------------------------------------------------------------

-- 1a. Remove the broken table definition (the CSV file in S3 stays)
DROP TABLE IF EXISTS olist_raw_db.raw_orders;

-- 1b. Create it again with the correct columns
CREATE EXTERNAL TABLE olist_raw_db.raw_orders (
  order_id                      string,
  customer_id                   string,
  order_status                  string,
  order_purchase_timestamp      string,
  order_approved_at             string,
  order_delivered_carrier_date  string,
  order_delivered_customer_date string,
  order_estimated_delivery_date string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://olistcap-data-2026/raw/orders/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 1c. Check: all rows are there (expected 99441)
SELECT COUNT(*) AS total_orders
FROM raw_orders;

-- 1d. Check: columns and values look right
SELECT *
FROM raw_orders
LIMIT 5;

-- 1e. Check: order status counts (expected delivered = 96478)
SELECT order_status, COUNT(*) AS orders
FROM raw_orders
GROUP BY order_status
ORDER BY orders DESC;


-- ---------------------------------------------------------------------
-- 2) raw_category_translation
-- ---------------------------------------------------------------------

-- 2a. Remove the broken table definition (the CSV file in S3 stays)
DROP TABLE IF EXISTS olist_raw_db.raw_category_translation;

-- 2b. Create it again with the correct columns
CREATE EXTERNAL TABLE olist_raw_db.raw_category_translation (
  product_category_name         string,
  product_category_name_english string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://olistcap-data-2026/raw/category_translation/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 2c. Check: all rows are there (expected 71)
SELECT COUNT(*) AS categories
FROM raw_category_translation;

-- 2d. Check: columns and values look right
SELECT *
FROM raw_category_translation
LIMIT 5;


-- ---------------------------------------------------------------------
-- 3) API (new data) checks
-- ---------------------------------------------------------------------

-- 3a. New orders per day from the API (expected about 200 per day,
--     starting 2018-10-18)
SELECT event_date, COUNT(DISTINCT order_id) AS new_orders
FROM raw_api_orders
GROUP BY event_date
ORDER BY event_date;

-- 3b. Old (CSV) and new (API) orders do not overlap (expected 0)
SELECT COUNT(*) AS orders_in_both
FROM raw_api_orders a
JOIN raw_orders h ON a.order_id = h.order_id;