print("\n" + "="*80)
print("✓ HOMEWORK 2: Transactions per category in 09:00–09:30 window")
print("-"*80)

hw2_result = (
    df.filter(
        (col("timestamp") >= "2024-01-01 09:00:00") &
        (col("timestamp") < "2024-01-01 09:30:00")
    )
    .groupBy("category")
    .agg(count("tx_id").alias("tx_count"))
    .orderBy(desc("tx_count"))
)

print("\nResult (sorted by count):")
hw2_result.show(truncate=False)

print("\n★ ANSWER HW2: Category breakdown (09:00-09:30):")
for row in hw2_result.collect():
    print(f"   {row['category']}: {row['tx_count']} transactions")
