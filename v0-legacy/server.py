"""
MCP Server implementation for Spark optimization.
"""
from typing import Dict, List, Optional, Any
import asyncio
import json
from dataclasses import dataclass
from pyspark.sql import SparkSession
import os
import anthropic

@dataclass
class Tool:
    name: str
    description: str
    input_schema: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema
        }

class SparkMCPServer:
    """MCP server that provides Spark code optimization capabilities."""
    
    def __init__(self, name: str = "spark-optimizer", version: str = "1.0.0"):
        self.name = name
        self.version = version
        self.client = anthropic.Client(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.tools = self._register_tools()

    def _register_tools(self) -> List[Tool]:
        """Register available tools with their schemas."""
        return [
            Tool(
                name="optimize_spark_code",
                description="Optimize Spark code using Claude AI",
                input_schema={
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Spark code to optimize"},
                        "optimization_level": {
                            "type": "string",
                            "enum": ["basic", "advanced"],
                            "default": "advanced",
                            "description": "Level of optimization to apply"
                        }
                    },
                    "required": ["code"]
                }
            ),
            Tool(
                name="analyze_performance",
                description="Analyze performance of Spark code execution",
                input_schema={
                    "type": "object",
                    "properties": {
                        "original_code": {"type": "string"},
                        "optimized_code": {"type": "string"}
                    },
                    "required": ["original_code", "optimized_code"]
                }
            )
        ]

    async def list_tools(self) -> Dict[str, List[Dict[str, Any]]]:
        """List available tools following MCP protocol."""
        return {"tools": [tool.to_dict() for tool in self.tools]}

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool following MCP protocol."""
        if name == "optimize_spark_code":
            return await self._optimize_spark_code(arguments)
        elif name == "analyze_performance":
            return await self._analyze_performance(arguments)
        raise ValueError(f"Unknown tool: {name}")

    async def _optimize_spark_code(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize Spark code using Claude AI."""
        code = args["code"]
        level = args.get("optimization_level", "advanced")
        
        # Format prompt for Claude
        prompt = f"""Please optimize this PySpark code for better performance.
        Optimization level: {level}
        
        Original code:
        ```python
        {code}
        ```
        
        Please provide optimized code with detailed explanations of improvements.
        """
        
        response = self.client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=2000,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": response.content[0].text
                }
            ]
        }

    async def _analyze_performance(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance between original and optimized code."""
        original_code = args["original_code"]
        optimized_code = args["optimized_code"]
        
        # Format prompt for Claude
        prompt = f"""Please analyze the performance differences between these two PySpark code versions:

Original code:
```python
{original_code}
```

Optimized code:
```python
{optimized_code}
```

Provide a detailed analysis of:
1. Performance improvements
2. Resource utilization
3. Scalability considerations
4. Any potential trade-offs
"""
        
        response = self.client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=2000,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return {
            "content": [{
                "type": "text",
                "text": response.content[0].text
            }]
        }

    async def start(self, host: str = "localhost", port: int = 8080):
        """Start the MCP server."""
        server = await asyncio.start_server(
            self._handle_connection, host, port
        )
        print(f"MCP Server running on {host}:{port}")
        await server.serve_forever()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming MCP protocol connections."""
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                
                request = json.loads(data)
                response = await self._handle_request(request)
                
                writer.write(json.dumps(response).encode() + b"\n")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP protocol requests."""
        method = request.get("method")
        if method == "tools/list":
            return await self.list_tools()
        elif method == "tools/call":
            return await self.call_tool(
                request["params"]["name"],
                request["params"]["arguments"]
            )
        raise ValueError(f"Unknown method: {method}")
