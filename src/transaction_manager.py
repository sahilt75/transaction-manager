from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import DataFrame
from enum import Enum

from utils.logger import logger


class BalanceStatus(Enum):
    ACTIVE = "ACTIVE"
    BALANCE_WITHDREW = "BALANCE WITHDREW"

class WithdrawValidationResult(Enum):
    SUCCESS = "WITHDRAW_SUCCESS"
    FAILURE = "WITHDRAW_FAILED"


class TransactionManager:
    def __init__(self, spark, balances_df, withdrawals_df) -> None:
        self.spark = spark
        self.balances_df = balances_df
        self.withdrawals_df = withdrawals_df
    
    def group_and_sort(self) -> DataFrame:
        """
        Grouping and sorting of the dataframes

        Returns:
            sorted_balance_df (DataFrame): sorted df of the balances with total balance and total withdrawal amount
        """
        # Calculate total balance for each account
        total_balance_df = self.balances_df.groupBy("account_id") \
                                    .agg(spark_sum("balance_amount").alias("total_balance"))

        # Calculate total withdrawal amount for each account
        total_withdrawal_df = self.withdrawals_df.groupBy("account_id").agg(spark_sum("withdraw_amount").alias("total_withdrawal"))

        # Join total balance and total withdrawal
        result_df = self.balances_df.join(total_withdrawal_df, "account_id", "inner") \
                                    .withColumn("initial_balance", col("balance_amount")) \
                                    .withColumn("total_withdrawal", col("total_withdrawal")) 

        result_df = result_df.join(total_balance_df, "account_id", "inner") \
                                .withColumn("total_balance", col("total_balance")) 

        # Sort balances by account_id and balance_order in ascending order
        sorted_balance_df = result_df.orderBy("account_id", "balance_order")

        return sorted_balance_df

    def apply_withdrawal_rules(self, sorted_balance_df: DataFrame) -> list:
        """
        Apply withdrawal rules on sorted dataframe

        Args:
            sorted_balance_df (DataFrame): sorted df of the balances with total balance and total withdrawal amount

        Returns:
            results (list): resultant list of the withdrawal status of each balance at order level
        """
        results = []

        account_id = sorted_balance_df.first().account_id
        remaining_withdrawal = sorted_balance_df.first().total_withdrawal

        # Iterate over balances to deduct withdrawal amount
        for row in sorted_balance_df.collect():
            if remaining_withdrawal == 0 and account_id != row["account_id"]:
                remaining_withdrawal = row["total_withdrawal"]

            if float(row["total_balance"]) < float(row["total_withdrawal"]):
                available_balance = float(row["balance_amount"])
                status = BalanceStatus.ACTIVE.name
                validation_result = WithdrawValidationResult.FAILURE.name
                remaining_withdrawal = 0
            else:
                if float(row["balance_amount"]) >= remaining_withdrawal:
                    available_balance = float(row["balance_amount"]) - remaining_withdrawal
                    status = BalanceStatus.BALANCE_WITHDREW.name if float(row["balance_amount"]) == remaining_withdrawal else BalanceStatus.ACTIVE.name
                    remaining_withdrawal = 0
                    validation_result = WithdrawValidationResult.SUCCESS.name
                else:
                    available_balance = 0
                    remaining_withdrawal = remaining_withdrawal - float(row["balance_amount"])
                    status = BalanceStatus.BALANCE_WITHDREW.name
                    validation_result = WithdrawValidationResult.SUCCESS.name
            
            results.append((
                row["account_id"],
                int(row["balance_order"]),
                float(row["initial_balance"]),
                float(available_balance),
                status,
                validation_result
            ))
        
        return results

    def process_withdrawals(self) -> None:
        """
        Process withdrawals on balances
        """
        # grouping and sorting
        sorted_balance_df = self.group_and_sort()
        logger.info("grouping and sorting done..")

        # apply rules
        withdraw_results = self.apply_withdrawal_rules(sorted_balance_df=sorted_balance_df)
        logger.info("Rules applied..")

        # Write result DataFrame to CSV
        result_df = self.spark.createDataFrame(withdraw_results, ["account_id", "balance_order", "initial_balance", "available_balance", "status", "validation_result"])
        result_df.write.csv("data/result.csv", mode="overwrite", header=True)

