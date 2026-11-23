"""Pipeline step: reload the nine Redshift tables from S3 trusted/ (guide Step 11.2)."""
import os
import time

import boto3

rsd = boto3.client("redshift-data")
WG = os.environ["WORKGROUP"]                # olistcap-wg
DB = os.environ["DATABASE"]                 # dev  (the guide says olist)
SECRET = os.environ["SECRET_ARN"]           # Redshift admin secret (Secrets Manager)
BUCKET = os.environ["BUCKET"]               # olistcap-data-2026
ROLE = os.environ["REDSHIFT_ROLE_ARN"]      # OlistRedshiftRole ARN

TABLES = ["dim_geography", "dim_customer", "dim_seller", "dim_product",
          "dim_payment_type", "dim_date", "fact_sales", "fact_orders", "fact_payments"]


def run(sql):
    st = rsd.execute_statement(WorkgroupName=WG, Database=DB, SecretArn=SECRET, Sql=sql)
    while True:
        d = rsd.describe_statement(Id=st["Id"])
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(2)
    if d["Status"] != "FINISHED":
        raise RuntimeError(f"{sql[:70]} -> {d.get('Error')}")


def lambda_handler(event, context):
    for t in TABLES:                                       # full reload, safe to rerun
        run(f"TRUNCATE olist.{t}")
        run(f"COPY olist.{t} FROM 's3://{BUCKET}/trusted/{t}/' "
            f"IAM_ROLE '{ROLE}' FORMAT AS PARQUET")
    run("REFRESH MATERIALIZED VIEW olist.mv_monthly_sales")
    return {"loaded": TABLES}