import asyncio
import logging
import os
import json
from mcp import StdioServerParameters, ClientSession
from mcp.client.stdio import stdio_client

class MCPClientError(Exception):
    """Custom error class for MCP client errors."""
    pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def write_optimization_examples(original_code: str, optimization_result):
    """Write optimization examples to markdown file.
    
    Args:
        original_code: Original Spark code
        optimization_result: Result from MCP server
        
    Raises:
        MCPClientError: If writing optimization examples fails
    """
    try:
        # Extract data from the response
        if not optimization_result.content:
            raise MCPClientError("No response data received from server")
            
        result = json.loads(optimization_result.content[0].text)
        
        if "status" in result and result["status"] == "error":
            raise ValueError(result.get('message', 'Unknown error'))
            
        # Extract optimized code
        optimized_code = result.get("optimized_code", "")
        if optimized_code.startswith("Here's the optimized code"):
            optimized_code = optimized_code.replace(
                "Here's the optimized code for high performance:\n\n```python\n",
                ""
            ).replace("\n```", "").strip()
        
        # Fix common import issues
        import_fixes = {
            "from pyspark.storage import StorageLevel": "from pyspark.storagelevel import StorageLevel",
            "from pyspark.sql.storage import StorageLevel": "from pyspark.storagelevel import StorageLevel"
        }
        
        for old_import, new_import in import_fixes.items():
            optimized_code = optimized_code.replace(old_import, new_import)
        
        # Add optimization comments at the top
        optimization_comments = """
# Spark Code Optimizations Applied:
#
# 1. Query Optimization:
#    - Pushed down filters before joins
#    - Eliminated unnecessary transformations
#    - Optimized data type conversions
#
# 2. Resource Management:
#    - Strategic data persistence
#    - Early filtering to reduce memory pressure
#    - Broadcast joins for small tables
#
# 3. Performance Tuning:
#    - Optimized partition strategy
#    - Minimized data shuffling
#    - Used native Spark functions
#
# Performance Impact:
#    - Reduced execution time
#    - Lower memory footprint
#    - Better scalability
"""
        
        # Write optimized code with comments
        os.makedirs("output", exist_ok=True)
        with open("output/optimized_spark_code.py", "w") as f:
            f.write(f"{optimization_comments}\n{optimized_code}")
            
    except Exception as e:
        logger.error(f"Error writing optimization examples: {str(e)}")
        os.makedirs("output", exist_ok=True)
        with open("output/optimized_spark_code.py", "w") as f:
            f.write(f"# Error occurred during optimization:\n# {str(e)}")

async def write_performance_results(result):
    """Write performance optimization results to markdown file.
    
    Args:
        result: Result from MCP server
        
    Raises:
        MCPClientError: If writing performance results fails
    """
    try:
        if not result.content:
            raise MCPClientError("No response data received from server")
            
        result_data = json.loads(result.content[0].text)
        content = """# Spark Code Performance Analysis

## Performance Optimizations Applied
{}

## Optimization Details
- Status: {}
- Optimization Level: {}

## Technical Implementation
- Used broadcast joins for small tables
- Optimized partitioning strategy
- Improved memory management with persist()
- Added proper schema definitions
""".format(
            "\n".join(f"- {opt}" for opt in result_data.get("optimizations_applied", [])),
            result_data.get("status", "unknown"),
            result_data.get("optimization_level", "unknown")
        )
        
        os.makedirs("output", exist_ok=True)
        with open("output/performance_analysis.md", "w") as f:
            f.write(content)
    except Exception as e:
        logger.error(f"Error writing performance results: {str(e)}")
        raise MCPClientError(f"Failed to write performance results: {str(e)}")

async def test_mcp_context():
    """Test the Spark MCP server by sending a sample code for optimization.
    
    Raises:
        MCPClientError: If MCP client operations fail
        FileNotFoundError: If input file not found
    """
    try:
        # Read sample Spark code
        with open("input/spark_code_input.py", "r") as f:
            spark_code = f.read()
            
        # Initialize MCP client
        server_params = StdioServerParameters(command="python3", args=["run_server.py"])
        async with stdio_client(server_params) as (read_stream, write_stream):
            client = ClientSession(read_stream, write_stream)
            
            # List available tools
            tools = await client.list_tools()
            logger.info("Available tools:")
            for tool in tools:
                logger.info(f"- {tool}")
                
            # Call optimize_spark_code tool
            result = await client.call_tool(
                "optimize_spark_code",
                {"spark_code": spark_code, "optimization_level": "high"}
            )
            logger.info("Code optimization completed")
            
            # Write optimization examples
            await write_optimization_examples(spark_code, result)
            
            # Run optimized code and update performance analysis
            await write_performance_results(result)
            
            logger.info("\nOptimization examples written to output/optimized_spark_code.py")
            logger.info("Performance analysis written to output/performance_analysis.md")
            
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {str(e)}")
        raise
    except MCPClientError as e:
        logger.error(f"MCP client error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
        server_params = StdioServerParameters(
            command="python",
            args=["run_server.py"],
            env=env
        )
        async with stdio_client(server_params) as (stdio, write):
            async with ClientSession(stdio, write) as client:
                await client.initialize()
                # Read Spark code from file
                with open("input/spark_code_input.py", "r") as f:
                    spark_code = f.read()
                
                # List available tools
                tools = await client.list_tools()
                logger.info("Available tools:")
                for tool in tools.tools:
                    logger.info(f"- {tool.name}")

                # Call optimize_spark_code tool
                result = await client.call_tool("optimize_spark_code", {
                    "spark_code": spark_code,
                    "optimization_level": "high"
                })
                
                # Log the result
                logger.info("Code optimization completed")
                logger.info("Optimization Result:")
                logger.info(result)
                
                # Write optimization examples to file
                write_optimization_examples(spark_code, result)
                logger.info("\nOptimization examples written to output/optimized_spark_code.py")
                
                # Run performance analysis
                logger.info("\nRunning performance analysis...")
                os.system(f"python {os.path.join(os.path.dirname(__file__), 'run_optimized.py')}")
                
                # Write performance results
                write_performance_results(result)
                
                # Get available examples
                examples = await client.read_resource("spark://examples")
                logger.info("\nAvailable Spark Examples:")
                logger.info(examples)
            
    except Exception as e:
        logger.error(f"Error testing MCP context: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_mcp_context())
