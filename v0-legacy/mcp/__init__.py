"""
MCP (Model Context Protocol) implementation for Spark optimization.
"""
try:
    from mcp.client.client import SparkMCPClient
    from mcp.server import SparkMCPServer
except ImportError as e:
    logger.error('ImportError in MCP module: %s', e)

__all__ = ['SparkMCPClient', 'SparkMCPServer']
