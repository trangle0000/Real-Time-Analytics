
print("\n" + "="*80)
print("Final /stats Results")
print("="*80 + "\n")

r = requests.get("http://localhost:5000/stats")
stats = r.json()

print(json.dumps(stats, indent=2))

print("\n" + "="*80)
print("✓ Homework Complete!")
print("="*80)
