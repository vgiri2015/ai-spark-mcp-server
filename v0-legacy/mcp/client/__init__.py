import logging

logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {'module_name': __name__})
logger.info({'event': 'module_initialization', 'module_name': __name__, 'status': 'starting'})

from .spark import SparkMCPClient

__all__ = ['SparkMCPClient']
