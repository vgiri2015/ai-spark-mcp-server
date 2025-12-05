import logging
import sys
from enum import Enum

# Configure logging to output to stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger("spark_mcp")

class MCPErrorCode(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    EXTERNAL_SERVICE_ERROR = "external_service_error"
    INTERNAL_ERROR = "internal_error"

class MCPError(Exception):
    def __init__(self, code: MCPErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)
