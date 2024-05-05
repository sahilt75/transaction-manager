from pyspark.sql import SparkSession, DataFrame

class Initialise:
    def init_spark(self) -> SparkSession:
        """
        Initialise spark session

        Returns:
            spark (SparkSession): spark session
        """
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("transaction-manager") \
            .getOrCreate()

        return spark

    def load_data(self, spark: SparkSession) -> {DataFrame, DataFrame}:
        """
        Load data frames from CSV

        Args:
            spark (SparkSession): spark session

        Returns:
            balances_df: balances dataframe
            withdrawals_df: withdrawals dataframe
        """

        # Read CSV files into DataFrames
        balances_df = spark.read.csv("data/balances.csv", header=True)
        withdrawals_df = spark.read.csv("data/withdrawals.csv", header=True)

        return balances_df, withdrawals_df