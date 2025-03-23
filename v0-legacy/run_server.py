"""
Start the MCP server.
"""
import asyncio
from spark_mcp.server import SparkMCPServer

async def main():
    server = SparkMCPServer()
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())
