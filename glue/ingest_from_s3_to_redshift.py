import redshift_connector
import boto3
import sys
import json
import logging
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
bucket_name = "your-s3-bucket"
redshift_iam_arn = "your-redshift-iam-arn"
secret_name = "amazon_redshift_credentials"
region_name = "us-east-1"

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

credentials = get_redshift_credentials(secret_name, region_name)
if not credentials:
    logger.error("[ERROR] Failed to fetch Redshift credentials.")
    sys.exit(1)

user_name = credentials['username']
host = credentials['host']
password = credentials['password']
db_name = credentials['dbname']

def load_merge_sql_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        sql_content = response["Body"].read().decode("utf-8")
        return sql_content
    except Exception as e:
        logger.error(f"[ERROR] Failed to load SQL from S3: {e}")
        sys.exit(1)

try:
    conn = redshift_connector.connect(host=host, database=db_name, port=5439, user=user_name, password=password)
    cursor = conn.cursor()

    # load data dari S3 ke Redshift ke tabel sementara (tmp)
    copy_s3_to_tmp = f"""
        COPY raw_zone.tmp_{table_name}
        FROM 's3://{bucket_name}/raw_landing_zone/classicmodels_db/{table_name}/data.csv'
        IAM_ROLE '{redshift_iam_arn}'
        CSV
        IGNOREHEADER 1;
    """
    cursor.execute(copy_s3_to_tmp)
    logger.info("[INFO] Data ingestion into temporary Redshift table completed successfully.")

    # --- DEBUG: Read CSV content from S3 before loading ---
    debug_key = f"raw_landing_zone/classicmodels_db/{table_name}/data.csv"
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=debug_key)
        csv_content = response["Body"].read().decode("utf-8")
        logger.info(f"[DEBUG] First few lines of CSV for {table_name}:\n" + "\n".join(csv_content.splitlines()[:5]))
    except Exception as e:
        logger.error(f"[ERROR] Failed to read CSV preview from S3: {e}")

    # sinkronisasi data, kalau data sudah ada, update; kalau belum ada, insert.
    # Load and execute the corresponding MERGE SQL file
    sql_file_key = f"codes/sql/merge/merge_{table_name}.sql"
    merge_sql = load_merge_sql_from_s3(bucket_name, sql_file_key)
    cursor.execute(merge_sql)
    logger.info(f"[INFO] MERGE command for table {table_name} executed successfully.")

    # Truncate the temporary table after merge
    truncate_tmp = f"TRUNCATE TABLE raw_zone.tmp_{table_name}"
    cursor.execute(truncate_tmp)
    logger.info(f"[INFO] Temporary table {table_name} truncated.")

    # Commit all changes
    conn.commit()

except Exception as e:
    logger.error("[ERROR] Error during Redshift operations", exc_info=True)
    conn.rollback()
    sys.exit(1)
finally:
    if conn:
        conn.close()