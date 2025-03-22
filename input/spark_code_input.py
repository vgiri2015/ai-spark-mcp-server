from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, explode, sum as _sum
from pyspark.sql.types import IntegerType

# Start Spark session
spark = SparkSession.builder.appName("UnoptimizedExample").getOrCreate()

# Sample user data
users_data = [
    ("u1", "Alice Johnson", "US"),
    ("u2", "Bob Smith", "US"),
    ("u3", "Carlos Vega", "MX"),
    ("u4", "Dana Lee", "US")
]

# Sample transaction data
transactions_data = [
    ("t1", "u1", "100.50"),
    ("t2", "u2", "200.00"),
    ("t3", "u1", "350.75"),
    ("t4", "u3", "80.00"),
    ("t5", "u4", "150.00"),
    ("t6", "u4", "250.00")
]

# Create DataFrames
df_users = spark.createDataFrame(users_data, ["user_id", "full_name", "country"])
df_transactions = spark.createDataFrame(transactions_data, ["txn_id", "user_id", "amount"])

# Cache unnecessarily
df_users.cache()
df_transactions.cache()

# Add dummy column that's not used
df_users = df_users.withColumn("dummy", col("user_id"))

# Explode name unnecessarily
df_users = df_users.withColumn("name_parts", split(col("full_name"), " "))
df_users = df_users.withColumn("name_exploded", explode(col("name_parts")))

# Inefficient UDF to parse amount
def parse_amount(x):
    try:
        return int(float(x))
    except:
        return 0

parse_amount_udf = udf(parse_amount, IntegerType())
df_transactions = df_transactions.withColumn("amount_int", parse_amount_udf(col("amount")))

# Join both DataFrames
df_joined = df_transactions.join(df_users, "user_id", "inner")

# Repartition with no reason
df_joined = df_joined.repartition(10)

# Apply filters after join instead of before
df_filtered = df_joined.filter(col("country") == "US")

# Redundant selection and renaming
df_filtered = df_filtered.select(
    col("user_id").alias("uid"),
    col("amount_int").alias("txn_amount"),
    col("country")
)

# Group and filter
df_agg = df_filtered.groupBy("uid", "country").agg(_sum("txn_amount").alias("total"))
df_agg = df_agg.filter(col("total") > 200)

# Collect to driver (not good for large data)
result = df_agg.collect()

# Print output
for row in result:
    print(f"User: {row['uid']}, Country: {row['country']}, Total Spent: {row['total']}")

# Unpersist unnecessarily
df_users.unpersist()
df_transactions.unpersist()