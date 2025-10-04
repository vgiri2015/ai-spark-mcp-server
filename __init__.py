import logging
logger = logging.getLogger(__name__)

logger.info('/ai-spark-mcp-server/v0-legacy/__init__.py: Importing SparkMCPClient and SparkMCPServer')

from .client import SparkMCPClient
from .server import SparkMCPServer

__all__ = ['SparkMCPClient', 'SparkMCPServer']