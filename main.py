from src.init_app import Initialise
from src.transaction_manager import TransactionManager
from utils.logger import logger

def main():
    init_app = Initialise()

    # init spark
    spark = init_app.init_spark()
    logger.info("spark session initialised successfully..")

    # init dataframes
    balances_df, withdrawals_df = init_app.load_data(spark=spark)
    logger.info("data frames loaded successfully..")

    # process withdrawals
    transaction_manager = TransactionManager(spark=spark, balances_df=balances_df, withdrawals_df=withdrawals_df)
    transaction_manager.process_withdrawals()
    logger.info("withdrawals processed!")


if __name__ == "__main__":
    main()

