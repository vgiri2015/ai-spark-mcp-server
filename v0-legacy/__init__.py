import logging
from datetime import datetime

logger = logging.getLogger(__name__)

current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger.info(f"{current_time} - /ai-spark-mcp-server/v0-legacy/__init__.py: Importing SparkMCPClient and SparkMCPServer", extra={'module': 'init', 'action': 'import'})

from .client import SparkMCPClient
from .server import SparkMCPServer

__all__ = ['SparkMCPClient', 'SparkMCPServer']