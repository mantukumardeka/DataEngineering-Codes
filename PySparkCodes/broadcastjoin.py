from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Start Spark Session
spark = SparkSession.builder \
    .appName("BroadcastJoinExample") \
    .getOrCreate()

# Large Fact Data (Transactions)
transactions_data = [
    (101, 500, "2023-09-01"),
    (102, 200, "2023-09-01"),
    (103, 300, "2023-09-02"),
    (104, 150, "2023-09-03"),
    (105, 700, "2023-09-03")
]
transactions_df = spark.createDataFrame(transactions_data, ["cust_id", "amount", "date"])

# Small Dimension Data (Customers)
customers_data = [
    (101, "Alice", "Premium"),
    (102, "Bob", "Standard"),
    (103, "Charlie", "Premium"),
    (104, "David", "Standard")
]
customers_df = spark.createDataFrame(customers_data, ["cust_id", "name", "segment"])

# Broadcast Join (avoids shuffle of transactions_df)
result_df = transactions_df.join(
    broadcast(customers_df),  # Force Spark to broadcast this small dataset
    on="cust_id",
    how="inner"
)

# Show Results
result_df.show()
