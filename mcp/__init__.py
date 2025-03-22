"""
MCP (Model Context Protocol) implementation for Spark optimization.
"""
from mcp.client.client import SparkMCPClient
from mcp.server import SparkMCPServer

__all__ = ['SparkMCPClient', 'SparkMCPServer']
