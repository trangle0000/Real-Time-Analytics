from pyspark.sql.functions import window, col, count, sum as _sum, round as _round, desc

hw3_result = (
    df.groupBy(window("timestamp", "15 minutes"))
    .agg(
        count("tx_id").alias("tx_count"),
        _round(_sum("amount"), 2).alias("total_PLN"),
    )
    .select(
        col("window.start").alias("from"),
        col("window.end").alias("to"),
        "tx_count",
        "total_PLN",
    )
    .orderBy(desc("tx_count"))
)

print("="*80)
print("✓ HOMEWORK 3: Peak transaction volume - 15-minute windows")
print("="*80)
print("\nResult (sorted by highest transaction count):")
hw3_result.show(truncate=False)

# Get the answer
hw3_answer = hw3_result.first()
print(f"\n★ ANSWER HW3: Peak 15-minute window")
print(f"   Time: {hw3_answer['from']} to {hw3_answer['to']}")
print(f"   Peak transaction count: {hw3_answer['tx_count']} transactions")
print(f"   Total amount: {hw3_answer['total_PLN']} PLN")
