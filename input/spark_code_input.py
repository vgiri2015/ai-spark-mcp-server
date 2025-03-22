"""
Example Spark code to optimize.
This code performs data processing on employee and department data.
"""
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("EmployeeAnalysis").getOrCreate()

# Create sample data
employees = [
    (1, "John", 30, "Sales", 50000),
    (2, "Alice", 25, "Engineering", 80000),
    (3, "Bob", 35, "Sales", 60000),
    (4, "Carol", 28, "Engineering", 85000),
    (5, "David", 40, "Marketing", 70000)
]

departments = [
    ("Sales", "NYC", 100),
    ("Engineering", "SF", 200),
    ("Marketing", "LA", 50)
]

# Create DataFrames
emp_df = spark.createDataFrame(employees, ["id", "name", "age", "dept", "salary"])
dept_df = spark.createDataFrame(departments, ["dept", "location", "budget"])

# Join and analyze data
result = emp_df.join(dept_df, "dept") \
    .groupBy("dept", "location") \
    .agg({"salary": "avg", "age": "avg", "id": "count"}) \
    .orderBy("dept")

# Show results
result.show()