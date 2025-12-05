FROM python:3.10-slim

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY v1 v1

# Install dependencies
RUN pip install .

# Set environment variables
ENV PYTHONPATH=/app/v1
ENV ANTHROPIC_API_KEY="" 

# Run the server
ENTRYPOINT ["python", "-m", "spark_mcp.server"]
