from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

print("Listening and counting transactions per store...")

for message in consumer:
    tx = message.value
    store = tx["store"]
    amount = tx["amount"]

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- Store Summary ---")
        print(f"{'Store':<10} | {'Count':<5} | {'Total Amount':<12} | {'Avg Amount':<10}")
        print("-" * 55)

        for s in store_counts:
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count if count > 0 else 0
            print(f"{s:<10} | {count:<5} | {total:<12.2f} | {avg:<10.2f}")
