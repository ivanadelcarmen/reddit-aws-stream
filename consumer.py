import base64
import json
import os
import boto3
import uuid
from utils import transform_message

client = boto3.client('kinesis')

def lambda_handler(event, context):
    for partition_key in event['records']:
        partition_value = event['records'][partition_key]
        for record in partition_value:
            message = (base64.b64decode(record['value'])).decode('utf-8')
            message = json.loads(message)
            new_message = transform_message(message, 'text') # Process and transform the message

            kinesis_message = (json.dumps(new_message)).encode('utf-8')
            client.put_record(
                StreamName=os.getenv('KINESIS_NAME'),
                Data=kinesis_message,
                PartitionKey=str(uuid.uuid4())
            )