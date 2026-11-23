$bucket = "olistcap-data-2026"

aws s3 cp .\data\olist_orders_dataset.csv s3://$bucket/raw/orders/
aws s3 cp .\data\olist_order_items_dataset.csv s3://$bucket/raw/order_items/
aws s3 cp .\data\olist_order_payments_dataset.csv s3://$bucket/raw/payments/
aws s3 cp .\data\olist_order_reviews_dataset.csv s3://$bucket/raw/reviews/
aws s3 cp .\data\olist_customers_dataset.csv s3://$bucket/raw/customers/
aws s3 cp .\data\olist_sellers_dataset.csv s3://$bucket/raw/sellers/
aws s3 cp .\data\olist_products_dataset.csv s3://$bucket/raw/products/
aws s3 cp .\data\olist_geolocation_dataset.csv s3://$bucket/raw/geolocation/
aws s3 cp .\data\product_category_name_translation.csv s3://$bucket/raw/category_translation/


# glue database creation

aws glue create-database --database-input Name=olist_raw_db
aws glue create-database --database-input Name=olist_curated_db
aws glue create-database --database-input Name=olist_trusted_db 

