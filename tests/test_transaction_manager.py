import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.transaction_manager import TransactionManager, BalanceStatus, WithdrawValidationResult

@pytest.fixture(scope="module")
def spark_session():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("pytest") \
        .getOrCreate()
    
    yield spark
    
    # Stop SparkSession at the end
    spark.stop()

def test_apply_withdrawal_rules(spark_session):
    # Define schema for balances DataFrame
    balances_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("balance_order", IntegerType(), True),
        StructField("balance_amount", FloatType(), True)
    ])

    # Define schema for withdrawals DataFrame
    withdrawals_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("withdraw_amount", FloatType(), True)
    ])

    # Sample data for balances and withdrawals
    balances_data = [("15", 1, 10000.0), ("15", 2, 2000.0), ("15", 3, 4000.0), ("16", 1, 5000.0)]
    withdrawals_data = [("15", 13000.0), ("16", 18000.0)]

    # Create DataFrames
    balances_df = spark_session.createDataFrame(balances_data, schema=balances_schema)
    withdrawals_df = spark_session.createDataFrame(withdrawals_data, schema=withdrawals_schema)

    # Initialize TransactionManager
    transaction_manager = TransactionManager(spark_session, balances_df, withdrawals_df)

    # Apply withdrawal rules
    sorted_balance_df = transaction_manager.group_and_sort()
    results = transaction_manager.apply_withdrawal_rules(sorted_balance_df)

    # Assertions
    print(results)
    assert len(results) == 4
    assert results[0] == ("15", 1, 10000.0, 0.0, BalanceStatus.BALANCE_WITHDREW.name, WithdrawValidationResult.SUCCESS.name)
    assert results[1] == ("15", 2, 2000.0, 0.0, BalanceStatus.BALANCE_WITHDREW.name, WithdrawValidationResult.SUCCESS.name)
    assert results[2] == ("15", 3, 4000.0, 3000.0, BalanceStatus.ACTIVE.name, WithdrawValidationResult.SUCCESS.name)
    assert results[3] == ("16", 1, 5000.0, 5000.0, BalanceStatus.ACTIVE.name, WithdrawValidationResult.FAILURE.name)
