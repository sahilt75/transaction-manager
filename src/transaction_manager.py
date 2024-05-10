from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import DataFrame
from enum import Enum
from pyspark.sql.functions import lit, expr, when, asc
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.logger import logger


class BalanceStatus(Enum):
    ACTIVE = "ACTIVE"
    BALANCE_WITHDREW = "BALANCE WITHDREW"

class WithdrawValidationResult(Enum):
    SUCCESS = "WITHDRAW_SUCCESS"
    PARTIAL_SUCCESS = "WITHDRAW_SUCCESS_PARTIAL"
    FAILURE = "WITHDRAW_FAILED"
    NOT_ATTEMPTED = "WITHDRAW_NOT_ATTEMPTED"


class TransactionManager:
    def __init__(self, spark, balances_df, withdrawals_df) -> None:
        self.spark = spark
        self.balances_df = balances_df
        self.withdrawals_df = withdrawals_df
    
    def group_and_sort(self) -> None:
        """
        Grouping and sorting of the dataframes
        """

        # Add total_balance column for each account_id based on available_balance
        window = Window.partitionBy("account_id")
        self.balances_df = self.balances_df.withColumn("total_balance", spark_sum("available_balance").over(window))
        
        # Add initial_balance column as available_balance
        self.balances_df = self.balances_df.withColumn("initial_balance", col("available_balance"))
        
        # Sort balances DataFrame based on account_id*balance_order
        self.balances_df = self.balances_df.orderBy(col("account_id"), col("balance_order"))
        
        # Sort withdrawals DataFrame based on account_id*withdraw_order
        self.withdrawals_df = self.withdrawals_df.orderBy(col("account_id"), col("withdraw_order"))

        # Cast columns to appropriate types
        self.balances_df = self.balances_df.withColumn("available_balance", self.balances_df["available_balance"].cast("float")) \
                                        .withColumn("balance_order", self.balances_df["balance_order"].cast("int")) \
                                        .withColumn("validation_result", lit(WithdrawValidationResult.NOT_ATTEMPTED.value))
        self.withdrawals_df = self.withdrawals_df.withColumn("withdraw_amount", self.withdrawals_df["withdraw_amount"].cast("float")) \
                                                .withColumn("withdraw_order", self.withdrawals_df["withdraw_order"].cast("int"))

        
    def apply_withdrawal_rules(self) -> None:
        """
        Apply withdrawal rules on sorted dataframe
        """
        # Deduction from balances
        for row in self.withdrawals_df.collect():
            account_id = row["account_id"]
            withdraw_amount = row["withdraw_amount"]
            
            # Filter balances for the current account_id
            account_balances = self.balances_df.filter(col("account_id") == account_id).collect()
            
            total_balance = sum(balance["available_balance"] for balance in account_balances)
            
            for balance in account_balances:
                if withdraw_amount == 0:
                    continue

                if withdraw_amount > total_balance:
                    new_balance = balance["available_balance"]
                    validation_result = WithdrawValidationResult.FAILURE.value
                    status = BalanceStatus.ACTIVE.value
                else:
                    if balance["available_balance"] >= withdraw_amount:
                        new_balance = balance["available_balance"] - withdraw_amount
                        status = BalanceStatus.BALANCE_WITHDREW.value if new_balance == 0 else BalanceStatus.ACTIVE.value
                        withdraw_amount = 0
                        validation_result = WithdrawValidationResult.SUCCESS.value
                    else:
                        new_balance = 0
                        withdraw_amount -= balance["available_balance"]
                        validation_result = WithdrawValidationResult.PARTIAL_SUCCESS.value
                        status = BalanceStatus.BALANCE_WITHDREW.value
                
                # Update balances row with appropriate values
                self.balances_df = self.balances_df.withColumn("available_balance", 
                                               when((col("account_id") == account_id) & (col("balance_order") == balance["balance_order"]), new_balance)
                                               .otherwise(col("available_balance")))
                self.balances_df = self.balances_df.withColumn("status", 
                                               when((col("account_id") == account_id) & (col("balance_order") == balance["balance_order"]), status)
                                               .otherwise(col("status")))
                self.balances_df = self.balances_df.withColumn("validation_result", 
                                               when((col("account_id") == account_id) & (col("balance_order") == balance["balance_order"]), validation_result)
                                               .otherwise(col("validation_result")))
                

    def process_withdrawals(self) -> None:
        """
        Process withdrawals on balances
        """
        # grouping and sorting
        self.group_and_sort()
        logger.info("grouping and sorting done..")

        # apply rules
        self.apply_withdrawal_rules()
        logger.info("Rules applied..")

        # Write result DataFrame to CSV
        self.balances_df.repartition(1).write.csv("data/result.csv", mode="overwrite", header=True)

