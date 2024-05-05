from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit, when

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("WithdrawalManager") \
    .getOrCreate()

# Step 2: Read CSV files into DataFrames
balances_df = spark.read.csv("balances.csv", header=True)
withdrawals_df = spark.read.csv("withdrawals.csv", header=True)

# Step 3: Calculate total balance for each account and balance_order
total_balance_df = balances_df.groupBy("account_id") \
                              .agg(spark_sum("balance_amount").alias("total_balance"))

# Step 4: Calculate total withdrawal amount for each account
total_withdrawal_df = withdrawals_df.groupBy("account_id").agg(spark_sum("withdraw_amount").alias("total_withdrawal"))

# Step 5: Join total balance and total withdrawal
result_df = balances_df.join(total_withdrawal_df, "account_id", "inner") \
                            .withColumn("initial_balance", col("balance_amount")) \
                            .withColumn("total_withdrawal", col("total_withdrawal")) 

result_df = result_df.join(total_balance_df, "account_id", "inner") \
                        .withColumn("total_balance", col("total_balance")) 

# Step 6: Sort balances by account_id and balance_order in ascending order
sorted_balance_df = result_df.orderBy("account_id", "balance_order")

# Step 7: Iterate over balances to deduct withdrawal amount
results = []
account_id = sorted_balance_df.first().account_id
remaining_withdrawal = sorted_balance_df.first().total_withdrawal
for row in sorted_balance_df.collect():
    if remaining_withdrawal == 0 and account_id != row["account_id"]:
        remaining_withdrawal = row["total_withdrawal"]

    if float(row["total_balance"]) < float(row["total_withdrawal"]):
        available_balance = float(row["balance_amount"])
        status = "ACTIVE"
        validation_result = "WITHDRAW_FAILED"
        remaining_withdrawal = 0
    else:
        if float(row["balance_amount"]) >= remaining_withdrawal:
            available_balance = float(row["balance_amount"]) - remaining_withdrawal
            status = "BALANCE WITHDREW" if float(row["balance_amount"]) == remaining_withdrawal else "ACTIVE"
            remaining_withdrawal = 0
            validation_result = "WITHDRAW_SUCCESS"
        else:
            available_balance = 0
            remaining_withdrawal = remaining_withdrawal - float(row["balance_amount"])
            status = "BALANCE WITHDREW"
            validation_result = "WITHDRAW_SUCCESS"
    
    results.append((
        row["account_id"],
        int(row["balance_order"]),
        float(row["initial_balance"]),
        float(available_balance),
        status,
        validation_result
    ))

# Step 8: Write result DataFrame to CSV
result_df = spark.createDataFrame(results, ["account_id", "balance_order", "initial_balance", "available_balance", "status", "validation_result"])

result_df.write.csv("result.csv", mode="overwrite", header=True)

# Step 9: Stop SparkSession
spark.stop()
