## What is Model Context Protocol (MCP)?

Model Context Protocol (MCP) is a standardized communication protocol designed for AI systems to interact with external tools, resources, and services. It provides a structured way for AI models to:
- Access and manipulate data
- Execute code
- Call external services
- Manage resources
- Handle asynchronous operations

## Why is MCP Required in the AI Field?

MCP addresses several critical challenges in AI development:

1. **Standardization**: Without a standard protocol, each AI system would need custom integrations with different tools and services, leading to fragmentation and compatibility issues.

2. **Context Management**: AI models need consistent ways to maintain context across interactions, handle state, and manage resources.

3. **Tool Integration**: AI systems need to interact with various tools and services. MCP provides a unified interface for these interactions.

4. **Error Handling**: MCP includes standardized error handling and response formats, making it easier to debug and maintain AI systems.

## How Does MCP Work?

MCP works through a client-server architecture:

1. **Server Side**:
   - Registers tools and resources that can be used by clients
   - Handles requests and manages state
   - Provides standardized responses

2. **Client Side**:
   - Connects to MCP servers
   - Makes tool and resource requests
   - Handles responses and errors

3. **Communication Flow**:
   ```
   Client -> Initialize Connection -> Server
   Client -> Request Tool/Resource -> Server
   Server -> Process Request -> Server
   Server -> Return Response -> Client
   ```

# Spark MCP Server

## Architecture Workflow

```
+----------------+     +------------------+     +------------------+
|                |     |                  |     |                  |
| test_client.py |     | spark_mcp_server |     |   Claude API    |
|                |     |                  |     |                  |
+-------+--------+     +--------+---------+     +--------+---------+
        |                       |                        |
        |                       |                        |
        |  1. Read Spark Code   |                        |
        |  from spark_code.py   |                        |
        |                       |                        |
        |  2. Send to Server    |                        |
        +----------------------->                         |
        |                       |  3. Request            |
        |                       |  Optimization          |
        |                       +----------------------->|
        |                       |                        |
        |                       |  4. Return             |
        |                       |  Optimized Code        |
        |                       <-----------------------+|
        |  5. Return Result     |                        |
        <-----------------------+                        |
        |                       |                        |
        |  6. Write to         |                        |
        |  optimized_spark_code.py                      |
        |                       |                        |
        v                       |                        |
```

## MCP Decorators Explained

### @mcp.tool()
```python
@mcp.tool()
def optimize_spark_code(spark_code: str, optimization_level: str = "medium") -> Dict[str, Any]:
```
The `@mcp.tool()` decorator registers a function as an MCP tool that can be called by clients. It:
- Handles parameter validation
- Provides automatic type checking
- Manages async execution
- Standardizes error handling
- Makes the tool discoverable via `list_tools()`

### @mcp.resource()
```python
@mcp.resource("spark://examples")
def get_spark_examples() -> Dict[str, List[Dict[str, str]]]:
```
The `@mcp.resource()` decorator registers a function as an MCP resource that provides static or dynamic data. It:
- Defines a URI scheme for the resource
- Handles caching and invalidation
- Manages resource lifecycle
- Provides versioning support

## Innovative Use of MCP for AI-Powered Optimization

While MCP is traditionally used to connect AI models with multiple external tools, this project takes an innovative approach that transforms MCP into a specialized optimization service.

### Traditional MCP Usage vs Our Approach

```
Traditional:
AI Model -> MCP Server -> Multiple Tools (Git, DB, Files, etc.)

Our Approach:
Spark Code -> MCP Server -> Claude AI -> Optimized Code
```

### Key Innovation Points

1. **Specialized Service**
   - Focused solely on Spark code optimization
   - Deep integration with a single AI provider (Claude)
   - Purpose-built for performance optimization
   - Creates a reusable optimization service

2. **Intelligent Abstraction Layer**
   - Hides complexity of LLM interactions
   - Provides consistent optimization interface
   - Makes it easy to switch LLM providers
   - Handles authentication and rate limiting

3. **Domain-Specific Value Addition**
   - Implements multiple optimization levels
   - Adds Spark-specific knowledge and best practices
   - Handles code formatting and validation
   - Provides example management and documentation

4. **Architectural Benefits**
   - Clean separation of concerns
   - Standardized input/output formats
   - Easy to extend with new features
   - Can be deployed as a standalone service

This approach demonstrates how MCP can be adapted from a general-purpose tool integration protocol into a specialized, high-value service that combines AI capabilities with domain expertise.

## Why Use spark-mcp-server Instead of Direct LLM Calls?

1. **Standardized Interface**:
   - Provides a consistent API for code optimization
   - Handles request/response formatting
   - Manages authentication and rate limiting

2. **Enhanced Functionality**:
   - Adds fallback optimization strategies
   - Implements caching for similar requests
   - Provides example management
   - Handles code validation and formatting

3. **Better Error Handling**:
   - Graceful handling of API failures
   - Detailed error messages
   - Automatic retries

4. **Resource Management**:
   - Efficient use of API tokens
   - Connection pooling
   - Request throttling

5. **Extensibility**:
   - Easy to add new optimization levels
   - Can integrate multiple LLM providers
   - Supports custom optimization rules

6. **Security**:
   - API key management
   - Input validation
   - Output sanitization

7. **Monitoring and Logging**:
   - Request tracking
   - Performance metrics
   - Usage statistics


## Before and After MCP Examples

### Drawbacks of Direct LLM Integration (Without MCP)

Without using MCP Server, you would need to implement direct integration with Claude AI, which has several drawbacks:

1. **Complex Error Handling**:
```python
class SparkOptimizer:
    def optimize(self, code):
        try:
            response = claude_client.optimize(code)
            if response.error:
                # Handle API errors
            if response.rate_limited:
                # Handle rate limiting
            if response.timeout:
                # Handle timeouts
        except Exception as e:
            # Handle other errors
```

2. **No Standardization**:
```python
# Different implementations for different LLMs
class ClaudeOptimizer:
    def optimize(self, code): pass

class GPTOptimizer:
    def process_code(self, code): pass  # Different method name

class AnthropicOptimizer:
    def enhance(self, code): pass  # Another different name
```

3. **Limited Reusability**:
```python
# Tightly coupled implementation
class SparkOptimizer:
    def __init__(self):
        self.claude = Claude(api_key="...")  # Hard-coded dependency
        
    def optimize(self, code):
        return self.claude.direct_call(code)  # No abstraction
```

### Key Drawbacks:

1. **Development Overhead**:
   - Need to implement error handling from scratch
   - Must manage API keys and rate limiting manually
   - Have to build custom logging and monitoring

2. **Maintenance Burden**:
   - Changes in LLM API require code updates
   - No standardized way to add new features
   - Testing becomes more complex

3. **Limited Flexibility**:
   - Difficult to switch LLM providers
   - Hard to implement caching
   - Complex to add new optimization levels

4. **Security Risks**:
   - Direct exposure of API keys
   - No standardized input validation
   - Potential for prompt injection

### With MCP

```python
# Server defines tools and resources using standard decorators
@mcp.tool()
async def optimize_spark_code(code: str):
    # Implementation
    pass

# Client uses standard interface
async with ClientSession(stdio, write) as client:
    result = await client.call_tool("optimize_spark_code", {"code": code})
```

## MCP Server Code Explanation

Our MCP server (`spark_mcp_server.py`) implements:

1. **Tool Registration**:
   - `optimize_spark_code`: A tool for optimizing Spark code with different optimization levels
   - Uses `@mcp.tool()` decorator for registration

2. **Resource Registration**:
   - `spark://examples`: A resource providing example Spark code snippets
   - Uses `@mcp.resource()` decorator

3. **AI-Powered Optimization**:
   - Uses Claude (Anthropic's LLM) for intelligent code optimization
   - Provides expert-level Spark optimizations based on best practices
   - Falls back to rule-based optimization if Claude is unavailable

4. **Optimization Features**:
   - Basic: Adds `limit(10)` to `show()` operations
   - Medium: Uses `persist()` instead of `cache()`, adds broadcast hints
   - High: Adds repartitioning, bucketing, and optimizes join strategies

## MCP Client Code Explanation

Our MCP client (`test_client.py`) demonstrates:

1. **Server Connection**:
   ```python
   server_params = StdioServerParameters(
       command="python",
       args=["spark_mcp_server.py"]
   )
   ```

2. **Session Management**:
   ```python
   async with stdio_client(server_params) as (stdio, write):
       async with ClientSession(stdio, write) as client:
           await client.initialize()
   ```

3. **Tool Usage**:
   - Lists available tools
   - Calls `optimize_spark_code` with parameters
   - Handles responses

4. **Resource Access**:
   - Retrieves example Spark code using `read_resource`
   - Processes resource responses

## Benefits of Using MCP

1. **Standardization**: Common interface for all tools and resources
2. **Type Safety**: Built-in type checking and validation
3. **Async Support**: Native handling of asynchronous operations
4. **Error Handling**: Consistent error reporting and handling
5. **Extensibility**: Easy to add new tools and resources

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your Anthropic API key:
```bash
export ANTHROPIC_API_KEY=your_api_key_here
```

3. Start the MCP server:
```bash
python spark_mcp_server.py
```

## Running Tests

To run the test client:
```bash
python test_client.py
```

## Optimization Levels

- **Low**: Basic optimizations (adds limit to show operations)
- **Medium**: Intermediate optimizations (replaces cache with persist)
- **High**: Advanced optimizations (adds repartitioning before groupBy)
