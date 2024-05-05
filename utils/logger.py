import logging

# Create a logger
logger = logging.getLogger("transaction_manager")

# Set the logging level (optional)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()

# Create a formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Set the formatter for the handler
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)
