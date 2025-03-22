import asyncio
import logging
import os
from mcp import StdioServerParameters, ClientSession
from mcp.client.stdio import stdio_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def write_optimization_examples(original_code: str, optimization_result):
    """Write optimization examples to markdown file."""
    try:
        # Extract data from the response
        response_data = optimization_result.content
        if isinstance(response_data, list) and len(response_data) > 0:
            result = eval(response_data[0].text)
        else:
            raise ValueError("No response data")
        
        if "status" in result and result["status"] == "error":
            content = f"""# Error occurred during optimization:
{result.get('message', 'Unknown error')}

# Original Code:
{original_code.strip()}"""
        else:
            optimized_code = result.get("optimized_code", "# No optimized code available")
            if optimized_code.startswith("Here's the optimized code"):
                optimized_code = optimized_code.replace(
                    "Here's the optimized code for high performance:\n\n```python\n",
                    "# Optimized code for high performance:\n"
                ).replace("\n```", "").strip()
            
            content = """
# Original Code:
{}

# Optimized Code (by Claude AI):
{}

# Key Optimizations Applied:
# {}

# These optimizations significantly improve performance, especially for larger datasets.""".format(
                original_code.strip(),
                optimized_code,
                "\n".join(f"# - {opt}" for opt in result.get("optimizations_applied", []) if opt)
            )
        
        with open("optimized_spark_code.py", "w") as f:
            f.write(content)
            
    except Exception as e:
        logger.error(f"Error writing optimization examples: {str(e)}")
        content = f"""# Error occurred during optimization:
{str(e)}

# Original Code:
{original_code.strip()}"""
        with open("optimized_spark_code.py", "w") as f:
            f.write(content)

async def test_mcp_context():
    """Test the Spark MCP server by sending a sample code for optimization."""
    try:
        # Create MCP client with environment variables
        env = os.environ.copy()
        server_params = StdioServerParameters(
            command="python",
            args=["spark_mcp_server.py"],
            env=env
        )
        async with stdio_client(server_params) as (stdio, write):
            async with ClientSession(stdio, write) as client:
                await client.initialize()
                # Read Spark code from file
                with open("spark_code.py", "r") as f:
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
                logger.info("\nOptimization examples written to optimized_spark_code.py")
                
                # Get available examples
                examples = await client.read_resource("spark://examples")
                logger.info("\nAvailable Spark Examples:")
                logger.info(examples)
            
    except Exception as e:
        logger.error(f"Error testing MCP context: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_mcp_context())
