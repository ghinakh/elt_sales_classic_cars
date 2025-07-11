
import redshift_connector
import sys
import json
import logging
import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger("glue-custom-logger")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] %(message)s")
handler.setFormatter(formatter)

# Agar handler tidak ditambahkan berkali-kali
if not logger.handlers:
    logger.addHandler(handler)

# Get the table name from the arguments
args = getResolvedOptions(sys.argv, ["table_name"])
table_name = args["table_name"]

# Define constants
redshift_iam_arn = "arn:aws:iam::476887127487:role/service-role/AmazonRedshift-CommandsAccessRole-20250709T105439"
secret_name = "amazon_redshift_credentials"
region_name = "us-east-1"
# Lokasi file di S3
S3_BUCKET = "ginn-aws-bde-bucket"
S3_RULE_KEY = f"codes/rule/data_quality.json"
S3_CLEANING_KEY = f"codes/sql/clean/clean_{table_name}.sql"
S3_MERGE_KEY = f"codes/sql/merge/merge_{table_name}.sql"

def get_redshift_credentials(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"[ERROR] Unable to retrieve secret: {e}")
        sys.exit(1)  # Exit with an error status

    if 'SecretString' not in get_secret_value_response:
        logger.error("[ERROR] Secret does not contain a string.")
        sys.exit(1)  # Exit with an error status

    secret = get_secret_value_response['SecretString']
    credentials = json.loads(secret)
    return credentials

def load_json_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        json_content = response["Body"].read().decode("utf-8")
        return json.loads(json_content)
    except Exception as e:
        logger.error(f"[ERROR] Failed to load JSON from S3: {e}")
        return None
    
def load_sql_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        sql_content = response["Body"].read().decode("utf-8")
        return sql_content
    except Exception as e:
        logger.error(f"[ERROR] Failed to load SQL from S3 ({key}): {e}")
        sys.exit(1)

def run_json_dq_rules(cursor, dq_rules, table_name):
    rules = dq_rules.get(table_name, [])
    if not rules:
        logger.warning(f"[WARNING] No DQ rules defined for table: {table_name}")
        return

    logger.info(f"[INFO] Running Data Quality checks for table: {table_name}")
    for rule in rules:
        rule_desc = rule.get("rule")
        sql = rule.get("sql")
        if not sql:
            continue
        try:
            cursor.execute(sql)
            count = cursor.fetchone()[0]
            logger.info(f"[INFO] Rule: {rule_desc} → {count} issue(s) found")
        except Exception as e:
            logger.error(f"[ERROR] Failed to execute rule '{rule_desc}': {e}")

credentials = get_redshift_credentials(secret_name, region_name)
if not credentials:
    logger.error("[ERROR] Failed to fetch Redshift credentials.")
    sys.exit(1)

user_name = credentials['username']
host = credentials['host']
password = credentials['password']
db_name = credentials['dbname']

try:
    if table_name not in ['customers', 'products', 'orders']:
        logger.error("[ERROR] Unsupported table: {table_name}", exc_info=True)
        sys.exit(1)

    conn = redshift_connector.connect(host=host, database=db_name, port=5439, user=user_name, password=password)
    cursor = conn.cursor()

    if table_name == 'customers':
        dq_json = load_json_from_s3(S3_BUCKET, S3_RULE_KEY)
        if dq_json:
            run_json_dq_rules(cursor, dq_json, table_name)

        clean_sql = load_sql_from_s3(S3_BUCKET, S3_CLEANING_KEY)
        tmp_table = f"tmp_clean_{table_name}"
        create_tmp_sql = f"CREATE TEMP TABLE {tmp_table} AS {clean_sql}"
        cursor.execute(create_tmp_sql)
        logger.info(f"[INFO] Cleaning rule applied → temporary cleaned table '{tmp_table}' created.")

        # merge the data into dim table
        merge_sql = load_sql_from_s3(S3_BUCKET, S3_MERGE_KEY)
        target_table = f"processed_zone.dim_{table_name}"
        merge_sql = merge_sql.replace(f"raw_zone.{table_name}", target_table)
        merge_sql = merge_sql.replace(f"raw_zone.tmp_{table_name}", tmp_table) 
        merge_sql = merge_sql.replace(f"source.salesRepEmployeeNumber, ", "") 
        merge_sql = merge_sql.replace(f"salesRepEmployeeNumber, ", "") 
        cursor.execute(merge_sql)
        logger.info(f"[INFO] MERGE for cleaned table {table_name} → into {target_table}")   
        
        if dq_json:
            run_json_dq_rules(cursor, dq_json, "dim_customers")

    elif table_name == 'products':
        # products and productlines
        tbls = ['products', 'productlines']
        for tbl in tbls:
            dq_json = load_json_from_s3(S3_BUCKET, S3_RULE_KEY)
            if dq_json:
                run_json_dq_rules(cursor, dq_json, tbl)

        clean_prod_sql = load_sql_from_s3(S3_BUCKET, S3_CLEANING_KEY)
        S3_CLEANING_KEY = S3_CLEANING_KEY.replace(f"{table_name}", "productlines")
        clean_pline_sql = load_sql_from_s3(S3_BUCKET, S3_CLEANING_KEY)

        tmp_table = f"tmp_clean_{table_name}"
        create_tmp_sql = f"CREATE TEMP TABLE {tmp_table} AS {clean_prod_sql}"
        cursor.execute(create_tmp_sql)
        logger.info(f"[INFO] Cleaning rule applied → temporary cleaned table '{tmp_table}' created.")

        tmp_table_lines = f"tmp_clean_productlines"
        create_tmp_pl_sql = f"CREATE TEMP TABLE {tmp_table_lines} AS {clean_pline_sql}"
        cursor.execute(create_tmp_pl_sql)
        logger.info(f"[INFO] Cleaning rule applied → temporary cleaned table '{tmp_table_lines}' created.")
        
        join_sql = f"""
            SELECT
                p.productCode, p.productName, p.productLine, p.productScale, 
                p.productVendor, p.productDescription, p.quantityInStock,
                p.buyPrice, p.MSRP, pl.textDescription AS textDescriptionProductLine
            FROM {tmp_table} p
            LEFT JOIN {tmp_table_lines} pl ON p.productline = pl.productline
        """
        create_tmp_dim_sql = f"CREATE TEMP TABLE tmp_dim_products AS {join_sql}"
        cursor.execute(create_tmp_dim_sql)
        logger.info("[INFO] Joined applied → temporary joined table tmp_dim_products created.")

        S3_MERGE_KEY = S3_MERGE_KEY.replace(f"{table_name}", "joined")
        merge_sql = load_sql_from_s3(S3_BUCKET, S3_MERGE_KEY)  
        cursor.execute(merge_sql)
        logger.info(f"[INFO] MERGE for tmp_dim_products  → into processed_zone.dim_products") 

        if dq_json:
            run_json_dq_rules(cursor, dq_json, "dim_products")

    elif table_name == 'orders':
        # orders and orderdetails
        tbls = ['orders', 'orderdetails']
        for tbl in tbls:
            dq_json = load_json_from_s3(S3_BUCKET, S3_RULE_KEY)
            if dq_json:
                run_json_dq_rules(cursor, dq_json, tbl)

        clean_order_sql = load_sql_from_s3(S3_BUCKET, S3_CLEANING_KEY)
        S3_CLEANING_KEY = S3_CLEANING_KEY.replace(f"{table_name}", "orderdetails")
        clean_orderdetails_sql = load_sql_from_s3(S3_BUCKET, S3_CLEANING_KEY)

        tmp_table = f"tmp_clean_{table_name}"
        create_tmp_sql = f"CREATE TEMP TABLE {tmp_table} AS {clean_order_sql}"
        cursor.execute(create_tmp_sql)
        logger.info(f"[INFO] Cleaning rule applied → temporary cleaned table '{tmp_table}' created.")

        tmp_table_lines = f"tmp_clean_orderdetails"
        create_tmp_od_sql = f"CREATE TEMP TABLE {tmp_table_lines} AS {clean_orderdetails_sql}"
        cursor.execute(create_tmp_od_sql)
        logger.info(f"[INFO] Cleaning rule applied → temporary cleaned table '{tmp_table_lines}' created.")

        join_sql = f"""
            SELECT
                od.ordernumber, od.productcode, od.orderLineNumber, o.customernumber, od.quantityOrdered, od.priceEach, od.totalPrices, 
                o.orderDate, o.requiredDate, o.shippedDate, o.status, o.comments
            FROM {tmp_table_lines} od
            LEFT JOIN {tmp_table} o ON o.ordernumber = od.ordernumber
        """

        create_tmp_fact_sql = f"CREATE TEMP TABLE tmp_fact_sales AS {join_sql}"
        cursor.execute(create_tmp_fact_sql)
        logger.info("[INFO] Joined applied → temporary joined table tmp_fact_sales created.")

        S3_MERGE_KEY = S3_MERGE_KEY.replace(f"{table_name}", "joined_fact")
        merge_sql = load_sql_from_s3(S3_BUCKET, S3_MERGE_KEY)
        cursor.execute(merge_sql)
        logger.info(f"[INFO] MERGE for tmp_fact_sales → into processed_zone.fact_sales") 

        if dq_json:
            run_json_dq_rules(cursor, dq_json, "fact_sales")

except Exception as e:
    logger.error("[ERROR] Error during Redshift operations", exc_info=True)
    conn.rollback()
    sys.exit(1)
finally:
    if conn:
        conn.close()

