"""
AWS Glue job 1: olist-raw-to-curated   (capstone Step 5)

1. Reads the OLD data: the 9 Olist CSV files in raw/<table>/
2. Reads the NEW data: the API JSON files the Lambda saved in raw/api_<table>/event_date=<day>/
3. Reads the WEBSITE uploads: JSON files the upload validator saved in raw/upload_<table>/
   (any of the 9 Olist tables; an uploaded row replaces an older row with the same key)
4. Combines everything into one table per entity, with a data_source column
   (history / api / upload)
5. Removes duplicates (for orders it keeps the newest status of each order)
6. Cleans the values: trims text, empty text -> null, real types, zip codes back to 5 digits,
   review comments on one line, misspelled column names fixed
7. Writes Parquet to curated/<table>/ (overwrite: every run rebuilds curated from all of raw)
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.shuffle.partitions", "8")  # small data: a few files per table, not 200
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")  # no _SUCCESS files

BUCKET = "s3://olistcap-data-2026"
RAW = f"{BUCKET}/raw"
CURATED = f"{BUCKET}/curated"

# ---- the columns of every table (same names in the CSV files and in the API data) ----
COLUMNS = {
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
               "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id",
                    "shipping_limit_date", "price", "freight_value"],
    "payments": ["order_id", "payment_sequential", "payment_type",
                 "payment_installments", "payment_value"],
    "reviews": ["review_id", "order_id", "review_score", "review_comment_title",
                "review_comment_message", "review_creation_date", "review_answer_timestamp"],
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                  "customer_city", "customer_state"],
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "products": ["product_id", "product_category_name", "product_name_lenght",
                 "product_description_lenght", "product_photos_qty", "product_weight_g",
                 "product_length_cm", "product_height_cm", "product_width_cm"],
    "category_translation": ["product_category_name", "product_category_name_english"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                    "geolocation_city", "geolocation_state"],
    "states": ["state_id", "state_code", "state_name", "region_id", "region_code", "region_name"],
}

# ---- the column(s) that identify one row; used to remove duplicates ----
KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "products": ["product_id"],
    "category_translation": ["product_category_name"],
    "states": ["state_code"],
    # geolocation is grouped by zip code instead, see clean_geolocation()
}
HISTORY_TABLES = [t for t in COLUMNS if t != "states"]  # states only comes from the API
UPLOAD_TABLES = HISTORY_TABLES                            # the website can add to any Olist table
SOURCE_RANK = {"upload": 2, "api": 1, "history": 0}       # who wins when the same key appears twice

# ---- how each column is cleaned ----
TIMESTAMP_COLS = {"order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                  "order_delivered_customer_date", "order_estimated_delivery_date",
                  "shipping_limit_date", "review_creation_date", "review_answer_timestamp"}
DOUBLE_COLS = {"price", "freight_value", "payment_value", "geolocation_lat", "geolocation_lng"}
INT_COLS = {"order_item_id", "payment_sequential", "payment_installments", "review_score",
            "product_name_lenght", "product_description_lenght", "product_photos_qty",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
            "state_id", "region_id"}
ZIP_COLS = {"customer_zip_code_prefix", "seller_zip_code_prefix", "geolocation_zip_code_prefix"}
LOWER_COLS = {"order_status", "payment_type", "product_category_name",
              "product_category_name_english", "customer_city", "seller_city", "geolocation_city"}
UPPER_COLS = {"customer_state", "seller_state", "geolocation_state", "state_code", "region_code"}
ONE_LINE_COLS = {"review_comment_title", "review_comment_message"}
RENAME = {"product_name_lenght": "product_name_length",            # typos in the Olist files
          "product_description_lenght": "product_description_length"}


def as_text(df, table, extra=()):
    """Keep only the table's columns, all as text. A column the source does not have becomes null."""
    have = set(df.columns)
    missing = [c for c in COLUMNS[table] if c not in have]
    if missing:
        print(f"WARNING {table}: columns not found in the source, set to null: {missing}")
    cols = [(F.col(c).cast("string") if c in have else F.lit(None).cast("string")).alias(c)
            for c in COLUMNS[table]]
    return df.select(cols + [F.col(c) for c in extra])


def read_history(table):
    """The OLD data: one Olist CSV file."""
    df = (spark.read
          .option("header", True)
          .option("multiLine", True)  # review comments contain line breaks
          .option("quote", '"')
          .option("escape", '"')      # a quote inside a value is written as ""
          .csv(f"{RAW}/{table}/"))
    # remove an invisible byte-order mark or quotes around the header names
    df = df.toDF(*[c.replace("\ufeff", "").strip().strip('"') for c in df.columns])
    return (as_text(df, table)
            .withColumn("data_source", F.lit("history"))
            .withColumn("event_date", F.lit(None).cast("string")))


def read_api(table):
    """The NEW data: every JSON file the Lambda saved for this table, from all event_date folders."""
    try:
        df = spark.read.option("recursiveFileLookup", True).json(f"{RAW}/api_{table}/")
    except Exception as err:  # folder missing or empty
        print(f"{table}: no API data yet ({str(err)[:150]})")
        return None
    day = F.regexp_extract(F.input_file_name(), r"event_date(?:=|%3D)([0-9-]{10})", 1)
    df = as_text(df.withColumn("event_date", day), table, extra=["event_date"])
    return (df.withColumn("event_date", F.when(F.col("event_date") == "", None)
                                         .otherwise(F.col("event_date")))
              .withColumn("data_source", F.lit("api")))


def read_upload(table):
    """The WEBSITE data: every JSON file the upload validator saved for this table."""
    try:
        df = spark.read.option("recursiveFileLookup", True).json(f"{RAW}/upload_{table}/")
    except Exception as err:  # folder missing or empty: nobody uploaded yet
        print(f"{table}: no uploads yet ({str(err)[:150]})")
        return None
    return (as_text(df, table)
            .withColumn("data_source", F.lit("upload"))
            .withColumn("event_date", F.lit(None).cast("string")))


def clean(df):
    """Trim text, empty text -> null, then give every column its real type."""
    exprs = []
    for c in df.columns:
        col = F.trim(F.col(c))
        col = F.when(col == "", None).otherwise(col)
        if c in TIMESTAMP_COLS:
            col = F.to_timestamp(col)
        elif c in DOUBLE_COLS:
            col = col.cast("double")
        elif c in INT_COLS:
            col = col.cast("double").cast("int")  # "40" and "40.0" both become 40
        elif c in ZIP_COLS:
            col = F.lpad(col.cast("double").cast("int").cast("string"), 5, "0")  # 1046 -> "01046"
        elif c in LOWER_COLS:
            col = F.lower(col)
        elif c in UPPER_COLS:
            col = F.upper(col)
        elif c in ONE_LINE_COLS:
            col = F.regexp_replace(col, r"\s+", " ")  # line breaks inside comments -> one space
        exprs.append(col.alias(RENAME.get(c, c)))
    return df.select(exprs)


def keep_latest(df, table):
    """One row per key. Upload beats API beats CSV, a later event_date beats an earlier one, and for orders
    a later status beats an earlier one (created < approved < invoiced < processing < shipped < final)."""
    rank = F.coalesce(*[F.when(F.col("data_source") == s, r) for s, r in SOURCE_RANK.items()])
    order_by = [rank.desc(),
                F.col("event_date").desc_nulls_last()]
    if table == "orders":
        status_rank = (F.when(F.col("order_status").isin("delivered", "canceled", "unavailable"), 6)
                        .when(F.col("order_status") == "shipped", 5)
                        .when(F.col("order_status") == "processing", 4)
                        .when(F.col("order_status") == "invoiced", 3)
                        .when(F.col("order_status") == "approved", 2)
                        .otherwise(1))
        order_by.append(status_rank.desc())
    w = Window.partitionBy(*KEYS[table]).orderBy(*order_by)
    return (df.dropna(subset=KEYS[table])
              .withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn", "event_date"))


def clean_geolocation(df):
    """About 1 million GPS points -> one row per zip code prefix (average position).
    Points outside Brazil are ignored when averaging."""
    in_brazil = F.col("geolocation_lat").between(-34, 6) & F.col("geolocation_lng").between(-74, -34)
    df = (df.withColumn("geolocation_lat", F.when(in_brazil, F.col("geolocation_lat")))
            .withColumn("geolocation_lng", F.when(in_brazil, F.col("geolocation_lng"))))
    from_history = F.max(F.when(F.col("data_source") == "history", 1).otherwise(0))
    return (df.dropna(subset=["geolocation_zip_code_prefix"])
              .groupBy("geolocation_zip_code_prefix")
              .agg(F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
                   F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
                   F.min("geolocation_city").alias("geolocation_city"),
                   F.min("geolocation_state").alias("geolocation_state"),
                   F.when(from_history == 1, "history")
                    .when(F.max(F.when(F.col("data_source") == "api", 1).otherwise(0)) == 1, "api")
                    .otherwise("upload").alias("data_source")))


# ---------------------------------------------------------------- main ----
print("table                   history      api   upload      total")
for table in COLUMNS:
    parts = []
    if table in HISTORY_TABLES:
        parts.append(read_history(table))
    api = read_api(table)
    if api is not None:
        parts.append(api)
    if table in UPLOAD_TABLES:
        upload = read_upload(table)
        if upload is not None:
            parts.append(upload)
    if not parts:
        print(f"{table}: nothing to write")
        continue

    df = parts[0]
    for part in parts[1:]:
        df = df.unionByName(part)
    df = clean(df)
    df = clean_geolocation(df) if table == "geolocation" else keep_latest(df, table)

    path = f"{CURATED}/{table}/"
    if table == "orders":  # guide Step 5: curated/orders partitioned by purchase year and month
        (df.withColumn("purchase_year", F.year("order_purchase_timestamp"))
           .withColumn("purchase_month", F.month("order_purchase_timestamp"))
           .write.mode("overwrite").partitionBy("purchase_year", "purchase_month").parquet(path))
    else:
        df.write.mode("overwrite").parquet(path)

    counts = {r["data_source"]: r["count"]
              for r in spark.read.parquet(path).groupBy("data_source").count().collect()}
    print(f"{table:22s}{counts.get('history', 0):>9,}{counts.get('api', 0):>9,}"
          f"{counts.get('upload', 0):>9,}{sum(counts.values()):>11,}")

job.commit()