import boto3
import uuid
from decimal import Decimal

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('incremental_load_configurations')

def upload_data_to_dynamodb():
    # Define the data to upload
    data = [
        {"table_name": "orders", "load_column": "orderDate", "last_extracted_value": None},
        {"table_name": "orderdetails", "load_column": 'orderDate', "last_extracted_value": None},  # ikut orders
        {"table_name": "payments", "load_column": "paymentDate", "last_extracted_value": None},
        {"table_name": "customers", "load_column": None, "last_extracted_value": None},
        {"table_name": "products", "load_column": None, "last_extracted_value": None},
        {"table_name": "productlines", "load_column": None, "last_extracted_value": None}
    ]
    
    # Prepare batch writer
    with table.batch_writer() as batch:
        for item in data:
            # Convert None to NULL (DynamoDB format) and handle Decimal conversion
            processed_item = {
                k: v if v is not None else None for k, v in item.items()
            }
            # Generate a unique ID for each item
            processed_item['id'] = str(uuid.uuid4())
            batch.put_item(Item=processed_item)
            
    print("Data uploaded successfully!")

# Execute the function
upload_data_to_dynamodb()