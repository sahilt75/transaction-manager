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
        StructField("available_balance", FloatType(), True),
        StructField("status", StringType(), True),
    ])

    # Define schema for withdrawals DataFrame
    withdrawals_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("withdraw_amount", FloatType(), True),
        StructField("withdraw_order", IntegerType(), True),
    ])

    # Sample data for balances and withdrawals
    balances_data = [("1", 1, 400.0, "ACTIVE"), ("1", 2, 100.0, "ACTIVE"), ("2", 1, 500.0, "ACTIVE")]
    withdrawals_data = [("2", 700.0, 1), ("1", 100.0, 1), ("1", 400.0, 2)]

    # Create DataFrames
    balances_df = spark_session.createDataFrame(balances_data, schema=balances_schema)
    withdrawals_df = spark_session.createDataFrame(withdrawals_data, schema=withdrawals_schema)

    # Initialize TransactionManager
    transaction_manager = TransactionManager(spark_session, balances_df, withdrawals_df)

    # Apply withdrawal rules
    transaction_manager.group_and_sort()
    transaction_manager.apply_withdrawal_rules()

    results = transaction_manager.balances_df.collect()
    results = [row.asDict() for row in results]

    # Assertions
    assert len(results) == 3
    assert results[0] == {'account_id': '1', 'balance_order': 1, 'available_balance': 0.0, 'status': 'BALANCE WITHDREW', 'total_balance': 500.0, 'initial_balance': 400.0, 'validation_result': 'WITHDRAW_SUCCESS_PARTIAL'}
    assert results[1] == {'account_id': '1', 'balance_order': 2, 'available_balance': 0.0, 'status': 'BALANCE WITHDREW', 'total_balance': 500.0, 'initial_balance': 100.0, 'validation_result': 'WITHDRAW_SUCCESS'}
    assert results[2] == {'account_id': '2', 'balance_order': 1, 'available_balance': 500.0, 'status': 'ACTIVE', 'total_balance': 500.0, 'initial_balance': 500.0, 'validation_result': 'WITHDRAW_FAILED'}
