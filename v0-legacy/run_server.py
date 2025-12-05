"""
Start the MCP server.
"""
import asyncio
from spark_mcp.server import SparkMCPServer

# Test cases should be implemented in a separate test file
# Example test case:
# def test_server_startup():
#     server = SparkMCPServer()
#     await server.start()
#     assert server.is_running()

if __name__ == "__main__":
    asyncio.run(main())
