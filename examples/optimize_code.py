"""
Example usage of the Spark MCP client.
"""
import asyncio
from spark_mcp.client import SparkMCPClient

async def main():
    # Create and connect to the MCP client
    client = SparkMCPClient()
    await client.connect()

    try:
        # List available tools
        tools = await client.list_tools()
        print("Available tools:", tools)

        # Example Spark code to optimize
        spark_code = """
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

        # Complex analysis with joins, window functions, and aggregations
        result = (
            emp_df.join(dept_df, "dept")
            .withColumn("avg_dept_salary", F.avg("salary").over(F.Window.partitionBy("dept")))
            .withColumn("salary_vs_avg", F.col("salary") - F.col("avg_dept_salary"))
            .groupBy("dept", "location")
            .agg(
                F.count("id").alias("emp_count"),
                F.sum("salary").alias("total_salary"),
                F.avg("salary_vs_avg").alias("avg_salary_diff")
            )
            .cache()
            .orderBy(F.desc("total_salary"))
            .show()
        )
        """

        # Optimize the code
        optimized_code = await client.optimize_spark_code(spark_code)
        print("\nOptimized code:", optimized_code)

        # Analyze performance
        performance = await client.analyze_performance(spark_code, optimized_code)
        print("\nPerformance analysis:", performance)

    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
if __name__ == "__main__":
    asyncio.run(main())
