from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Data options
stores = ["Warsaw", "Krakow", "Gdansk", "Wroclaw"]
categories = ["electronics", "clothing", "food", "books"]

# Function to generate transaction
def generate_transaction(tx_num):
    return {
        "tx_id": f"TX{tx_num:04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }

# Send 50 transactions (1 per second)
for i in range(1, 51):
    tx = generate_transaction(i)
    producer.send("transactions", value=tx)
    producer.flush()
    print(f"Sent: {tx}")
    time.sleep(1)

print("Finished sending 50 transactions.")
