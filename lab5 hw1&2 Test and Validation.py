
print("\n" + "="*80)
print("HOMEWORK 1: Test /stats endpoint")
print("="*80)

# Test /stats with no requests yet
print("\n1. /stats before any requests:")
r = requests.get("http://localhost:5000/stats")
print(json.dumps(r.json(), indent=2))

print("\n" + "="*80)
print("HOMEWORK 2: Test negative amount validation")
print("="*80)

print("\n1. POST /score with negative amount (should return 400):")
r = requests.post("http://localhost:5000/score", json={
    "tx_id": "INVALID",
    "amount": -500.0,
    "category": "food"
})
print(f"Status: {r.status_code}")
print(f"Response: {json.dumps(r.json(), indent=2)}")
print(f"✓ Correctly returned 400 (Bad Request)")
