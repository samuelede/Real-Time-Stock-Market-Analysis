from kafka import KafkaConsumer
import json
import time

# --- Configuration matching producer_setup.py ---
consumer = KafkaConsumer(
    'stock-analysis',
    bootstrap_servers='localhost:9094',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock-analysis-group', # Define a consumer group
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting Kafka consumer. Waiting for messages on topic 'customer_info'...")

for message in consumer:
    data = message.value

    # Print the received data
    print(f"Received data: {data}")

consumer.close()
print("Kafka consumer closed.")