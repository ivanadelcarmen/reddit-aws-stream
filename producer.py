import os
import json
from kafka import KafkaProducer

topic = os.getenv('KAFKA_TOPIC')
bootstrap_server_one = os.getenv('KAFKA_BS_FST')
bootstrap_server_two = os.getenv('KAFKA_BS_SND')

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_server_one, bootstrap_server_two], 
    value_serializer=lambda x: (json.dumps(x)).encode('utf-8')
)

def lambda_handler(event, context):
    for item in event['Records']:
        message = json.loads(item['body'])
        producer.send(topic, value=message)