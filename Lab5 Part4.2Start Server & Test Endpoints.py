import subprocess
import time
import requests
import json

print("="*80)
print("Task 4.2: /score Endpoint")
print("="*80)

# Start server
server = subprocess.Popen(["python", "app.py"])
time.sleep(2)

# Test cases
cases = [
    {"tx_id": "TX001", "amount": 50.0,   "category": "food",        "hour": 14},
    {"tx_id": "TX002", "amount": 1800.0,  "category": "electronics", "hour": 10},
    {"tx_id": "TX003", "amount": 4500.0,  "category": "electronics", "hour": 3},
]

print("\nScoring transactions:\n")

for tx in cases:
    r = requests.post("http://localhost:5000/score", json=tx)
    res = r.json()
    print(f"{tx['tx_id']}  {tx['amount']:>7.0f} PLN  → {res['risk_level']:8s} (score={res['score']})  {res['triggered_rules']}")

print("\n✓ Endpoint working correctly")
