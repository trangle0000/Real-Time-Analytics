
print("\n" + "="*80)
print("HOMEWORK 3: Score 10 Transactions in a Loop")
print("="*80)

# Create 10 test transactions
transactions = [
    {"tx_id": "TX001", "amount": 50.0,    "category": "food",        "hour": 14},
    {"tx_id": "TX002", "amount": 1200.0,  "category": "food",        "hour": 10},
    {"tx_id": "TX003", "amount": 1800.0,  "category": "electronics", "hour": 9},
    {"tx_id": "TX004", "amount": 2500.0,  "category": "jewelry",     "hour": 15},
    {"tx_id": "TX005", "amount": 3200.0,  "category": "electronics", "hour": 20},
    {"tx_id": "TX006", "amount": 4500.0,  "category": "electronics", "hour": 3},
    {"tx_id": "TX007", "amount": 800.0,   "category": "food",        "hour": 5},
    {"tx_id": "TX008", "amount": 2000.0,  "category": "clothing",    "hour": 12},
    {"tx_id": "TX009", "amount": 3500.0,  "category": "electronics", "hour": 2},
    {"tx_id": "TX010", "amount": 400.0,   "category": "book",        "hour": 18},
]

print(f"\nScoring {len(transactions)} transactions:\n")

results = []

for tx in transactions:
    r = requests.post("http://localhost:5000/score", json=tx)
    result = r.json()
    results.append(result)
    
    # Print each result
    print(f"{result['tx_id']} | Amount: ${tx['amount']:>7.0f} | "
          f"Risk: {result['risk_level']:8s} | Score: {result['score']} | "
          f"Rules: {len(result['triggered_rules'])}")

# Print summary table
print("\n" + "="*80)
print("Summary Table")
print("="*80 + "\n")

# Create table data
table_data = []
for i, result in enumerate(results, 1):
    tx = transactions[i-1]
    table_data.append([
        result['tx_id'],
        f"${tx['amount']:.0f}",
        tx['category'],
        tx['hour'],
        result['risk_level'],
        result['score'],
        len(result['triggered_rules']),
    ])

# Print formatted table
print(f"{'TX ID':<8} {'Amount':<8} {'Category':<12} {'Hour':<5} {'Risk':<10} {'Score':<6} {'Rules':<6}")
print("-" * 70)
for row in table_data:
    print(f"{row[0]:<8} {row[1]:<8} {row[2]:<12} {row[3]:<5} {row[4]:<10} {row[5]:<6} {row[6]:<6}")

# Statistics
print("\n" + "="*80)
print("Risk Level Distribution")
print("="*80 + "\n")

risk_counts = {}
for result in results:
    risk = result['risk_level']
    risk_counts[risk] = risk_counts.get(risk, 0) + 1

for risk in ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']:
    count = risk_counts.get(risk, 0)
    percentage = (count / len(results)) * 100
    print(f"{risk:10s}: {count} ({percentage:5.1f}%)")
