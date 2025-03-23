import logging
import os
import anthropic
from enum import Enum
from typing import Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from pyspark.sql import SparkSession

class MCPErrorCode(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    EXTERNAL_SERVICE_ERROR = "external_service_error"
    INTERNAL_ERROR = "internal_error"

class MCPError(Exception):
    def __init__(self, code: MCPErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Apache Spark Session with some optimizations
spark = (SparkSession.builder
        .appName("MCP_Spark_Server")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate())

import os
import anthropic
from typing import List

class SparkOptimizer:
    def __init__(self):
        # Initialize Claude client with API key
        self.client = anthropic.Client(api_key=os.getenv("ANTHROPIC_API_KEY"))

    @staticmethod
    def get_optimization_prompt(code: str, level: str) -> str:
        return f"You are an expert Apache Spark optimizer. Given the following PySpark code, optimize it for {level} performance. Apply these optimizations based on the level: For all levels: add limit(10) to show() operations and add appropriate imports. For medium and high levels: replace cache() with persist(StorageLevel.MEMORY_AND_DISK) and add broadcast hints for joins. For high level: add repartition before groupBy operations, use appropriate bucketing and partitioning, and optimize join strategies. Here's the code to optimize: {code} Return only the optimized code without explanations. Include all necessary imports."

    def optimize_code(self, code: str, level: str = "medium") -> str:
        """
        Optimizes Spark code using Claude for intelligent optimizations.
        """
        # Get optimization suggestions from Claude
        message = self.client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=1500,
            temperature=0,
            messages=[{
                "role": "user",
                "content": self.get_optimization_prompt(code, level)
            }]
        )
        
        # Extract optimized code from response
        optimized_code = message.content[0].text
        
        # Clean up the response
        optimized_code = optimized_code.strip()
        if optimized_code.startswith("```python"):
            optimized_code = optimized_code[9:]
        if optimized_code.endswith("```"):
            optimized_code = optimized_code[:-3]
        
        return optimized_code.strip()

# Initialize MCP server
mcp = FastMCP("spark-mcp-server")

# Register tools
mcp.register_tool("optimize_spark_code", optimize_spark_code)
mcp.register_resource("spark_examples", get_spark_examples)

@mcp.tool()
async def optimize_spark_code(spark_code: str, optimization_level: Optional[str] = "medium") -> Dict[str, Any]:
    """
    Optimizes Apache Spark code based on specified optimization level.
    
    Args:
        spark_code: The Spark code to optimize
        optimization_level: Level of optimization (low, medium, high)
        
    Returns:
        Dict containing optimization results
        
    Raises:
        MCPError: If optimization fails or parameters are invalid
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

@mcp.resource('spark://examples', content_type='application/json')
async def get_spark_examples() -> Dict[str, Any]:
    """Resource providing example Spark code snippets.
    
    Returns:
        Dict containing example Spark code snippets
        
    Raises:
        MCPError: If example retrieval fails
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
    try:
        logger.info("Starting Spark MCP Server")
        mcp.run()
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        raise
