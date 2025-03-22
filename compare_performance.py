from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import time

def run_original():
    # Create employee and department DataFrames
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

    emp_df = spark.createDataFrame(employees, ["id", "name", "age", "dept", "salary"])
    dept_df = spark.createDataFrame(departments, ["dept", "location", "budget"])

    start_time = time.time()
    # Complex analysis with joins, window functions, and aggregations
    result = (
        emp_df.join(dept_df, "dept")
        .withColumn("avg_dept_salary", F.avg("salary").over(Window.partitionBy("dept")))
        .withColumn("salary_vs_avg", F.col("salary") - F.col("avg_dept_salary"))
        .groupBy("dept", "location")
        .agg(
            F.count("id").alias("emp_count"),
            F.sum("salary").alias("total_salary"),
            F.avg("salary_vs_avg").alias("avg_salary_diff")
        )
        .cache()
        .orderBy(F.desc("total_salary"))
    )
    result.show()
    end_time = time.time()
    return end_time - start_time

def run_optimized():
    # Create employee and department DataFrames
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

    emp_df = spark.createDataFrame(employees, ["id", "name", "age", "dept", "salary"])
    dept_df = spark.createDataFrame(departments, ["dept", "location", "budget"])

    # Broadcast the smaller DataFrame
    broadcast_dept_df = F.broadcast(dept_df)

    start_time = time.time()
    # Complex analysis with joins, window functions, and aggregations
    result = (
        emp_df.repartition("dept")
        .join(broadcast_dept_df, "dept")
        .withColumn("avg_dept_salary", F.avg("salary").over(Window.partitionBy("dept")))
        .withColumn("salary_vs_avg", F.col("salary") - F.col("avg_dept_salary"))
        .repartition("dept", "location")
        .groupBy("dept", "location")
        .agg(
            F.count("id").alias("emp_count"),
            F.sum("salary").alias("total_salary"),
            F.avg("salary_vs_avg").alias("avg_salary_diff")
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
        .orderBy(F.desc("total_salary"))
    )
    result.show(10)
    end_time = time.time()
    return end_time - start_time

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("PerformanceComparison").getOrCreate()
    
    print("\nRunning original code...")
    original_time = run_original()
    print(f"Original execution time: {original_time:.2f} seconds")
    
    print("\nRunning optimized code...")
    optimized_time = run_optimized()
    print(f"Optimized execution time: {optimized_time:.2f} seconds")
    
    improvement = ((original_time - optimized_time) / original_time) * 100
    print(f"\nPerformance improvement: {improvement:.1f}%")
    
    spark.stop()
