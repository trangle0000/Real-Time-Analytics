from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "total_revenue": 0.0,
    "min_amount": float("inf"),
    "max_amount": float("-inf")
})

msg_count = 0

print("Listening and calculating category statistics...")

for message in consumer:
    tx = message.value
    category = tx["category"]
    amount = tx["amount"]

    stats[category]["count"] += 1
    stats[category]["total_revenue"] += amount
    stats[category]["min_amount"] = min(stats[category]["min_amount"], amount)
    stats[category]["max_amount"] = max(stats[category]["max_amount"], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- Category Statistics ---")
        print(f"{'Category':<12} | {'Count':<5} | {'Revenue':<12} | {'Min':<10} | {'Max':<10}")
        print("-" * 65)

        for cat, data in stats.items():
            print(
                f"{cat:<12} | "
                f"{data['count']:<5} | "
                f"{data['total_revenue']:<12.2f} | "
                f"{data['min_amount']:<10.2f} | "
                f"{data['max_amount']:<10.2f}"
            )
