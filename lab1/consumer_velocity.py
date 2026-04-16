from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Store timestamps for each user
user_timestamps = defaultdict(list)

print("Listening for velocity anomalies...")

for message in consumer:
    tx = message.value

    user_id = tx["user_id"]
    tx_id = tx["tx_id"]
    amount = tx["amount"]
    store = tx["store"]
    category = tx["category"]

    current_time = datetime.fromisoformat(tx["timestamp"])

    # Add current transaction time to the user's history
    user_timestamps[user_id].append(current_time)

    # Keep only timestamps from the last 60 seconds
    cutoff_time = current_time - timedelta(seconds=60)
    user_timestamps[user_id] = [
        t for t in user_timestamps[user_id] if t >= cutoff_time
    ]

    # Trigger alert if more than 3 transactions occurred in 60 seconds
    if len(user_timestamps[user_id]) > 3:
        print(
            f"VELOCITY ALERT: user {user_id} made "
            f"{len(user_timestamps[user_id])} transactions within 60 seconds | "
            f"Latest: {tx_id} | {amount:.2f} PLN | {store} | {category}"
        )
