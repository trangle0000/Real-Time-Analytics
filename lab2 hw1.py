print("\n✓ HOMEWORK 1: Lowest avg amount per hour for store 'Gdańsk'")
print("-"*80)

hw1_result = (
    df.filter(col("store") == "Gdańsk")
    .groupBy(window("timestamp", "1 hour"))
    .agg(
        _round(avg("amount"), 2).alias("avg_PLN"),
        count("tx_id").alias("tx_count"),
    )
    .select(
        col("window.start").alias("from"),
        col("window.end").alias("to"),
        "avg_PLN",
        "tx_count",
    )
    .orderBy("avg_PLN")
)

print("\nResult (sorted by lowest average):")
hw1_result.show(truncate=False)

# Get the answer
hw1_answer = hw1_result.first()
print(f"\n★ ANSWER HW1: The hour with lowest average is {hw1_answer['from']} to {hw1_answer['to']}")
print(f"   Average amount: {hw1_answer['avg_PLN']} PLN")
print(f"   Transaction count: {hw1_answer['tx_count']}")
