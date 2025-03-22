#!/usr/bin/env python3
"""
Simple client script to optimize Spark code using MCP.
"""
import asyncio
import os
from spark_mcp.client import SparkMCPClient

async def optimize_spark_code():
    # Read input code
    input_file = os.path.join('input', 'spark_code_input.py')
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found!")
        return
    
    with open(input_file, 'r') as f:
        spark_code = f.read()
    
    if not spark_code.strip():
        print("Error: Input file is empty!")
        return

    try:
        # Connect to MCP server
        print("\nConnecting to MCP server...")
        client = SparkMCPClient()
        await client.connect()
        
        # Optimize code
        print("Optimizing Spark code...")
        optimized_code = await client.optimize_spark_code(spark_code)
        
        # Analyze performance
        print("\nAnalyzing performance differences...")
        performance = await client.analyze_performance(spark_code, optimized_code)
        print("\nPerformance Analysis:")
        print(performance)
        
        # Close connection
        await client.close()
        print("\nOptimization complete! Check output/optimized_spark_example.py for the optimized code.")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        return

def main():
    # Check if server is running
    if os.system("lsof -i:8080 > /dev/null 2>&1") != 0:
        print("Error: MCP server is not running! Please start it with 'python run_server.py'")
        return
    
    # Check if input file exists
    if not os.path.exists('input/spark_code_input.py'):
        print("Error: input/spark_code_input.py not found!")
        print("Please create this file with your Spark code to optimize.")
        return
    
    # Run optimization
    asyncio.run(optimize_spark_code())

if __name__ == "__main__":
    main()
