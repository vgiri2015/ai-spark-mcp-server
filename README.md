# Spark MCP (Model Context Protocol) Optimizer

This project implements a Model Context Protocol (MCP) server and client for optimizing Apache Spark code using Claude AI. The system provides intelligent code optimization suggestions and performance analysis through a client-server architecture.

## Features

- **Intelligent Code Optimization**: Leverages Claude AI to analyze and optimize PySpark code
- **Performance Analysis**: Provides detailed analysis of performance differences between original and optimized code
- **MCP Architecture**: Implements the Model Context Protocol for standardized AI model interactions
- **Easy Integration**: Simple client interface for code optimization requests
- **Code Generation**: Automatically saves optimized code to separate files

## Installation

```bash
pip install -r requirements.txt
```

## Requirements

- Python 3.8+
- PySpark 3.2.0+
- Anthropic API Key (for Claude AI)

## Usage

1. Start the MCP server:
```bash
python run_server.py
```

2. Use the client to optimize Spark code:
```python
from spark_mcp.client import SparkMCPClient

async def main():
    # Connect to the MCP server
    client = SparkMCPClient()
    await client.connect()

    # Your Spark code to optimize
    spark_code = '''
    # Your PySpark code here
    '''

    # Get optimized code
    optimized_code = await client.optimize_spark_code(spark_code)
    
    # Analyze performance
    performance = await client.analyze_performance(spark_code, optimized_code)

    await client.close()
```

## Architecture

This project follows the Model Context Protocol architecture for standardized AI model interactions:

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│                  │      │   MCP Server     │      │    Resources     │
│   MCP Client     │      │  (SparkMCPServer)│      │                  │
│ (SparkMCPClient) │      │                  │      │ ┌──────────────┐ │
│                  │      │    ┌─────────┐   │      │ │  Claude AI   │ │
│   ┌─────────┐    │      │    │ Tools  │   │ <──>  │ │   Model      │ │
│   │ Tools   │    │      │    │Registry│   │       │ └──────────────┘ │
│   │Interface│    │ <──> │    └─────────┘   │      │                  │
│   └─────────┘    │      │    ┌─────────┐   │      │ ┌──────────────┐ │
│                  │      │    │Protocol │   │      │ │  PySpark     │ │
│                  │      │    │Handler  │   │      │ │  Runtime     │ │
│                  │      │    └─────────┘   │      │ └──────────────┘ │
└──────────────────┘      └──────────────────┘      └──────────────────┘

        │                         │                          │
        │                         │                          │
        v                         v                          v
┌──────────────┐          ┌──────────────┐           ┌──────────────┐
│  Available   │          │  Registered  │           │   External   │
│    Tools     │          │    Tools     │           │  Resources   │
├──────────────┤          ├──────────────┤           ├──────────────┤
│optimize_code │          │optimize_code │           │ Claude API   │
│analyze_perf  │          │analyze_perf  │           │ Spark Engine │
└──────────────┘          └──────────────┘           └──────────────┘
```

### Components

1. **MCP Client**
   - Provides tool interface for code optimization
   - Handles async communication with server
   - Manages file I/O for code generation

2. **MCP Server**
   - Implements MCP protocol handlers
   - Manages tool registry and execution
   - Coordinates between client and resources

3. **Resources**
   - Claude AI: Provides code optimization intelligence
   - PySpark Runtime: Executes and validates optimizations

### Protocol Flow

1. Client sends optimization request via MCP protocol
2. Server validates request and invokes appropriate tool
3. Tool utilizes Claude AI for optimization
4. Optimized code is returned via MCP response
5. Client saves and validates the optimized code

### Code Optimization Workflow

```
User Code                MCP Flow                 Optimization            Output
   │                         │                          │                    │
   │                         │                          │                    │
   ▼                         │                          │                    │
┌─────────┐          ┌──────────────┐                   │                    │
│ PySpark │          │              │                   │                    │
│  Code   │─────────▶│  MCP Client  │                   │                    │
└─────────┘          │              │                   │                    │
                     └──────┬───────┘                   │                    │
                            │                           │                    │
                            ▼                           │                    │
                     ┌──────────────┐         ┌─────────────────┐            │
                     │              │         │                 │            │
                     │  MCP Server  │────────▶│    Claude AI    │            │
                     │              │         │     Analysis    │            │
                     └──────┬───────┘         └────────┬────────┘            │
                            │                          │                     │
                            │                          ▼                     │
                            │                 ┌─────────────────┐            │
                            │                 │   Optimization  │            │
                            │                 │   Suggestions   │            │ 
                            │                 └────────┬────────┘            │
                            ▼                          │                     │
                     ┌──────────────┐                  │                     │
                     │   Apply &    │◀─────────────────┘                     │
                     │   Validate   │                                        │
                     └──────┬───────┘                                        │
                            │                                                │
                            ▼                                                ▼
                     ┌──────────────┐                              ┌──────────────┐
                     │  Optimized   │                              │ Performance  │
                     │    Code      │──────────────────────────────▶│  Analysis   │
                     └──────────────┘                              └──────────────┘
```

This workflow illustrates:
1. Input PySpark code submission
2. MCP protocol handling and routing
3. Claude AI analysis and optimization
4. Code transformation and validation
5. Performance analysis and reporting

## Project Structure

```
ai-mcp/
├── spark_mcp/
│   ├── __init__.py
│   ├── client.py      # MCP client implementation
│   └── server.py      # MCP server implementation
├── examples/
│   ├── optimize_code.py           # Example usage
│   └── optimized_spark_example.py # Generated optimized code
├── requirements.txt
└── run_server.py      # Server startup script
```

## Available Tools

1. **optimize_spark_code**
   - Optimizes PySpark code for better performance
   - Supports basic and advanced optimization levels
   - Automatically saves optimized code to examples/optimized_spark_example.py

2. **analyze_performance**
   - Analyzes performance differences between original and optimized code
   - Provides insights on:
     - Performance improvements
     - Resource utilization
     - Scalability considerations
     - Potential trade-offs

## Environment Variables

- `ANTHROPIC_API_KEY`: Your Anthropic API key for Claude AI

## Example Optimizations

The system implements various PySpark optimizations including:
- Broadcast joins for small-large table joins
- Efficient window function usage
- Strategic data caching
- Query plan optimizations
- Performance-oriented operation ordering

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT License
