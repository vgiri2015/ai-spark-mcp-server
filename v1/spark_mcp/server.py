from typing import Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
import anthropic

# Import from our package
from .utils import logger, MCPError, MCPErrorCode
from .optimizer import SparkOptimizer

# Initialize MCP server
mcp = FastMCP("spark-mcp-server")

@mcp.tool()
async def optimize_spark_code(spark_code: str, optimization_level: Optional[str] = "medium") -> Dict[str, Any]:
    """
    Optimizes Apache Spark code based on specified optimization level.
    
    Args:
        spark_code: The Spark code to optimize
        optimization_level: Level of optimization (low, medium, high)
        
    Returns:
        Dict containing optimization results
    """
    # Validate inputs
    if not spark_code:
        raise MCPError(
            code=MCPErrorCode.INVALID_ARGUMENT,
            message="spark_code cannot be empty"
        )
        
    if optimization_level not in ["low", "medium", "high"]:
        raise MCPError(
            code=MCPErrorCode.INVALID_ARGUMENT,
            message="optimization_level must be one of: low, medium, high"
        )
    
    logger.info(f"Received request to optimize Spark code with level: {optimization_level}")
    try:
        # Create optimizer instance and optimize the code
        optimizer = SparkOptimizer()
        optimized_code = optimizer.optimize_code(spark_code, optimization_level)
        logger.info("Code optimization completed")
        
        # Return the optimized code
        return {
            "status": "success",
            "original_code": spark_code,
            "optimized_code": optimized_code,
            "optimization_level": optimization_level,
            "optimizations_applied": [
                "Used Claude AI for intelligent Spark code optimization",
                "Applied schema optimizations and type hints",
                "Added broadcast hints for small tables",
                "Optimized partitioning and storage levels"
            ]
        }
    except anthropic.APIError as e:
        logger.error(f"Claude API error: {str(e)}")
        raise MCPError(
            code=MCPErrorCode.EXTERNAL_SERVICE_ERROR,
            message=f"Error communicating with optimization service: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error optimizing code: {str(e)}")
        raise MCPError(
            code=MCPErrorCode.INTERNAL_ERROR,
            message=f"Internal optimization error: {str(e)}"
        )

@mcp.resource('spark://examples')
async def get_spark_examples() -> Dict[str, Any]:
    """Resource providing example Spark code snippets.
    
    Returns:
        Dict containing example Spark code snippets
    """
    return {
        "examples": [
            {
                "name": "Basic DataFrame Operations",
                "code": """
# Create a sample DataFrame
data = [("John", 30), ("Alice", 25), ("Bob", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Perform operations
result = df.show()
"""
            },
            {
                "name": "GroupBy and Aggregation",
                "code": """
# Group and aggregate with caching
summary = (
    df.groupBy("age")
    .agg(F.count("name").alias("count"))
    .cache()
    .show()
)
"""
            }
        ]
    }

if __name__ == "__main__":
    mcp.run()
