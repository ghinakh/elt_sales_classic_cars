# cutomers, products productlines, (orders, order_details, payment)

import boto3
import sys
import csv
import pymysql
import io
import json
import logging
from datetime import date
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Ambil parameter dari Glue job arguments
args = getResolvedOptions(sys.argv, ["table_name", "load_type"])
table_name = args["table_name"]
load_type = args["load_type"]

# Fungsi ambil kredensial database dari AWS Secrets Manager
def get_rds_credentials(secret_name, region_name="us-east-1"):
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response.get("SecretString")
        return json.loads(secret) if secret else None
    except ClientError as e:
        logger.error(f"Failed to retrieve RDS secret: {e}")
        return None

# Ambil kredensial dari secret "secret_name_sales_classic_cars"
credentials = get_rds_credentials("secret_name_sales_classic_cars")
if not credentials:
    sys.exit("[ERROR] Failed to get credential RDS")

# Simpan informasi koneksi database
DB_CONFIG = {
    "host": credentials["host"],
    "user": credentials["username"],
    "password": credentials["password"],
    "database": "sales_classic_cars"
}

# Lokasi tujuan file CSV di S3
S3_BUCKET = "ginn-aws-bde-bucket"
S3_KEY = f"raw_landing_zone/classicmodels_db/{table_name}/data.csv"

# Inisialisasi DynamoDB dan table konfigurasi incremental load
dynamodb = boto3.resource("dynamodb")
config_table = dynamodb.Table("incremental_load_configurations")

# Ambil konfigurasi incremental dari DynamoDB
def fetch_incremental_config(table_name):
    try:
        response = config_table.get_item(Key={"table_name": table_name})
        item = response.get("Item", {})
        return item.get("load_column"), item.get("last_extracted_value")
    except Exception as e:
        logger.error(f"[ERROR] Error reading config from DynamoDB: {e}")
        return None, None
    
def build_query(table_name, load_type, incr_column, last_value):
    if table_name == "orderdetails":
        filter_clause = f"WHERE o.{incr_column} > '{last_value}'" if last_value else ""
        return f"""
            SELECT od.*, o.{incr_column} AS orderDate
            FROM orderdetails od
            JOIN orders o ON od.orderNumber = o.orderNumber
            {filter_clause}
            ORDER BY o.{incr_column} DESC
        """
    sql = f"SELECT * FROM {table_name}"
    if load_type == "incremental":
        if last_value:
            sql += f" WHERE {incr_column} > '{last_value}'"
        sql += f" ORDER BY {incr_column} DESC"
    return sql

# Update nilai terakhir kolom incremental ke DynamoDB
def update_last_value(table_name, last_value):
    try:
        config_table.update_item(
            Key={"table_name": table_name},
            UpdateExpression="SET last_extracted_value = :val",
            ExpressionAttributeValues={":val": last_value},
            ReturnValues="UPDATED_NEW"
        )
        logger.info(f"[INFO] Updated last_extracted_value to {last_value}")
    except Exception as e:
        logger.error(f"[ERROR] Failed to update last_extracted_value: {e}")

# Konversi hasil query ke CSV
def convert_to_csv(data):
    if not data:
        return ""
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return csv_buffer.getvalue()

# Fungsi utama Glue Job
def main():
    # Ambil konfigurasi incremental (kalau ada)
    incr_column, last_value = None, None
    if load_type == "incremental":
        incr_column, last_value = fetch_incremental_config(table_name)
        if not incr_column:
            logger.error("[ERROR] No incremental column found. Exiting...")
            sys.exit(1)

    try:
        # Koneksi ke database
        conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cursor:
            # Buat query sesuai tipe load
            sql = build_query(table_name, load_type, incr_column, last_value)
            cursor.execute(sql)
            records = cursor.fetchall()

        # Convert ke CSV
        csv_content = convert_to_csv(records)

        # Upload ke S3
        s3 = boto3.client("s3")
        s3.put_object(Body=csv_content, Bucket=S3_BUCKET, Key=S3_KEY)
        logger.info(f"[INFO] Data uploaded to s3://{S3_BUCKET}/{S3_KEY}")

        # Simpan nilai incremental terbaru
        if records and load_type == "incremental":
            newest_value = str(records[0][incr_column])
            update_last_value(table_name, newest_value)

    except Exception as e:
        logger.error(f"[ERROR] Error in main: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

# Run job
if __name__ == "__main__":
    main()