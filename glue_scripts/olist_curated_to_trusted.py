"""
AWS Glue job 2: olist_curated_to_trusted   (capstone guide Step 6)

Builds the star schema from the curated tables:
  dimensions: dim_geography, dim_customer, dim_seller, dim_product, dim_payment_type, dim_date
  facts:      fact_sales (one row per item), fact_orders (one row per order),
              fact_payments (one row per payment)
Writes one Parquet file per table to trusted/<table>/ for Redshift COPY (Step 7).

This is the guide's script. Lines marked "CHANGE:" are the only differences. They are needed
because this project also loads new data from its own API (data_source column, longer dates).
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
B = args["BUCKET"]

# timestamps in a format Redshift COPY reads cleanly
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
# CHANGE: no _SUCCESS files (the guide's Step 7 warns they can break COPY)
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


def cur(name):
    return spark.read.parquet(f"s3://{B}/curated/{name}/")


def out(df, name):
    df.coalesce(1).write.mode("overwrite").parquet(f"s3://{B}/trusted/{name}/")
    print(f"trusted/{name}: {df.count()} rows")


def money(c):
    return F.col(c).cast("decimal(12,2)")


def dkey(c):  # timestamp -> yyyymmdd int, or -1 (Unknown) when missing
    return F.coalesce(F.date_format(F.to_date(c), "yyyyMMdd").cast("int"), F.lit(-1))


w = lambda col: Window.orderBy(col)

# ---------- dim_geography (from the API data) ----------
dim_geo = (cur("states")
           .withColumn("geography_key", F.row_number().over(w("state_code")))
           .select("geography_key", "state_code", "state_name", "region_name"))
unknown_geo = spark.createDataFrame(
    [(-1, "NA", "Unknown", "Unknown")],
    "geography_key int, state_code string, state_name string, region_name string")
dim_geo = dim_geo.unionByName(unknown_geo)
out(dim_geo, "dim_geography")
geo_lookup = dim_geo.select("geography_key", "state_code")

# ---------- dim_customer ----------
cust = cur("customers")
dim_customer = (cust
                .join(geo_lookup, cust.customer_state == geo_lookup.state_code, "left")
                .withColumn("geography_key", F.coalesce("geography_key", F.lit(-1)))
                .withColumn("customer_key", F.row_number().over(w("customer_id")))
                .select("customer_key", "customer_id", "customer_unique_id", "customer_city",
                        "customer_state", "customer_zip_code_prefix", "geography_key",
                        "data_source"))  # CHANGE: history / api
out(dim_customer, "dim_customer")

# ---------- dim_seller ----------
sell = cur("sellers")
dim_seller = (sell
              .join(geo_lookup, sell.seller_state == geo_lookup.state_code, "left")
              .withColumn("geography_key", F.coalesce("geography_key", F.lit(-1)))
              .withColumn("seller_key", F.row_number().over(w("seller_id")))
              .select("seller_key", "seller_id", "seller_city", "seller_state",
                      "seller_zip_code_prefix", "geography_key",
                      "data_source"))  # CHANGE: history / api
out(dim_seller, "dim_seller")

# ---------- dim_product (English category, Portuguese as fallback) ----------
# CHANGE: join only the two translation columns (both curated tables have data_source)
trans = cur("category_translation").select("product_category_name",
                                           "product_category_name_english")
dim_product = (cur("products")
               # CHANGE: the guide's job 1 fills missing categories with "unknown"; ours keeps null
               .fillna({"product_category_name": "unknown"})
               .join(trans, "product_category_name", "left")
               .withColumn("category_en", F.coalesce("product_category_name_english",
                                                     "product_category_name"))
               .withColumn("product_key", F.row_number().over(w("product_id")))
               .select("product_key", "product_id",
                       F.col("product_category_name").alias("category_pt"),
                       "category_en",
                       # CHANGE: Redshift column is DOUBLE PRECISION; our curated column is int
                       F.col("product_weight_g").cast("double").alias("product_weight_g"),
                       F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
                       "data_source"))  # CHANGE: history / api
out(dim_product, "dim_product")

# ---------- dim_payment_type ----------
dim_pt = (cur("payments").select("payment_type").distinct()
          .withColumn("payment_type_key", F.row_number().over(w("payment_type")))
          .select("payment_type_key", "payment_type"))
out(dim_pt, "dim_payment_type")

# ---------- dim_date (2016-2019 plus an Unknown row) ----------
# CHANGE: the guide stops at 2019-12-31. The API feed can run to 2020-12-31, so the end
# year follows the data (never earlier than 2019, as in the guide).
orders = cur("orders")
last_day = orders.select(F.greatest(F.max("order_purchase_timestamp"),
                                    F.max("order_delivered_customer_date"),
                                    F.max("order_estimated_delivery_date")).alias("d")).first()["d"]
end_year = max(2019, last_day.year) if last_day else 2019
dates = spark.sql("SELECT explode(sequence(to_date('2016-01-01'), "
                  f"to_date('{end_year}-12-31'), interval 1 day)) AS full_date")
dim_date = (dates
            .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
            .withColumn("cal_year", F.year("full_date"))
            .withColumn("cal_quarter", F.quarter("full_date"))
            .withColumn("cal_month", F.month("full_date"))
            .withColumn("month_name", F.date_format("full_date", "MMMM"))
            .withColumn("day_of_month", F.dayofmonth("full_date"))
            .withColumn("day_name", F.date_format("full_date", "EEEE"))
            .withColumn("is_weekend", F.when(F.dayofweek("full_date").isin(1, 7), 1).otherwise(0))
            .select("date_key", "full_date", "cal_year", "cal_quarter", "cal_month",
                    "month_name", "day_of_month", "day_name", "is_weekend"))
unknown_date = spark.createDataFrame(
    [(-1, None, None, None, None, "Unknown", None, "Unknown", 0)],
    "date_key int, full_date date, cal_year int, cal_quarter int, cal_month int, "
    "month_name string, day_of_month int, day_name string, is_weekend int")
out(dim_date.unionByName(unknown_date), "dim_date")

# ---------- inputs for the facts ----------
# (orders was already read above for dim_date)
items = cur("order_items")
pay = cur("payments")
rev = cur("reviews")
cust_k = dim_customer.select("customer_id", "customer_key")

# ---------- fact_sales: one row per order item ----------
fact_sales = (items
              .join(orders.select("order_id", "customer_id", "order_status",
                                  "order_purchase_timestamp",
                                  "order_delivered_customer_date",
                                  "order_estimated_delivery_date"), "order_id", "inner")
              .join(cust_k, "customer_id", "left")
              .join(dim_seller.select("seller_id", "seller_key"), "seller_id", "left")
              .join(dim_product.select("product_id", "product_key"), "product_id", "left")
              .select("order_id", "order_item_id", "customer_key", "seller_key", "product_key",
                      dkey("order_purchase_timestamp").alias("purchase_date_key"),
                      dkey("order_delivered_customer_date").alias("delivered_date_key"),
                      dkey("order_estimated_delivery_date").alias("estimated_date_key"),
                      "order_status",
                      money("price").alias("price"),
                      money("freight_value").alias("freight_value"),
                      (F.col("price") + F.col("freight_value")).cast("decimal(12,2)")
                      .alias("item_total"),
                      "data_source"))  # CHANGE: history / api
out(fact_sales, "fact_sales")

# ---------- fact_orders: one row per order (aggregate children FIRST) ----------
items_agg = items.groupBy("order_id").agg(
    F.count("*").cast("int").alias("item_count"),
    F.sum("price").alias("items_value"),
    F.sum("freight_value").alias("freight_total"))
pay_agg = pay.groupBy("order_id").agg(
    F.sum("payment_value").alias("payment_total"),
    F.max("payment_installments").alias("max_installments"))
rev_agg = rev.groupBy("order_id").agg(F.avg("review_score").alias("avg_review"))

fact_orders = (orders
               .join(cust_k, "customer_id", "left")
               .join(items_agg, "order_id", "left")
               .join(pay_agg, "order_id", "left")
               .join(rev_agg, "order_id", "left")
               .withColumn("delivery_days",
                           F.datediff(F.to_date("order_delivered_customer_date"),
                                      F.to_date("order_purchase_timestamp")))
               .withColumn("delay_days",
                           F.datediff(F.to_date("order_delivered_customer_date"),
                                      F.to_date("order_estimated_delivery_date")))
               .withColumn("is_late", F.when(
                   (F.col("order_status") == "delivered")
                   & F.col("order_delivered_customer_date").isNotNull(),
                   F.when(F.col("delay_days") > 0, 1).otherwise(0)))
               .select("order_id", "customer_key",
                       dkey("order_purchase_timestamp").alias("purchase_date_key"),
                       dkey("order_delivered_customer_date").alias("delivered_date_key"),
                       dkey("order_estimated_delivery_date").alias("estimated_date_key"),
                       "order_status", "item_count",
                       money("items_value").alias("items_value"),
                       money("freight_total").alias("freight_total"),
                       money("payment_total").alias("payment_total"),
                       "max_installments",
                       F.round("avg_review", 2).cast("decimal(3,2)").alias("avg_review"),
                       "delivery_days", "delay_days", "is_late",
                       "data_source"))  # CHANGE: history / api
out(fact_orders, "fact_orders")

# ---------- fact_payments: one row per payment ----------
fact_payments = (pay
                 .join(orders.select("order_id", "customer_id", "order_purchase_timestamp"),
                       "order_id", "inner")
                 .join(cust_k, "customer_id", "left")
                 .join(dim_pt, "payment_type", "left")
                 .select("order_id", "payment_sequential", "customer_key", "payment_type_key",
                         dkey("order_purchase_timestamp").alias("purchase_date_key"),
                         "payment_installments",
                         money("payment_value").alias("payment_value"),
                         "data_source"))  # CHANGE: history / api
out(fact_payments, "fact_payments")

# ---------- row-count guard ----------
assert fact_orders.count() == orders.count(), "fact_orders grain broken"

job.commit()