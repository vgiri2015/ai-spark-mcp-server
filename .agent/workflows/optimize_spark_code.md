---
description: How to optimize Spark code using the MCP server
---

1. **Prerequisites**: Ensure you have a valid optional `ANTHROPIC_API_KEY` set in your environment.
   ```bash
   export ANTHROPIC_API_KEY="your_api_key_here"
   ```

2. **Prepare Input**: Paste the PySpark code you want to optimize into `v1/input/spark_code_input.py`.
   ```bash
   # Example: ensure directory exists
   mkdir -p v1/input
   # Edit the file
   nano v1/input/spark_code_input.py
   ```

3. **Run Optimization**: Execute the client script. This will automatically start the MCP server, send the code for optimization, and save the results.
   ```bash
   cd v1
   # // turbo
   python run_client.py
   ```

4. **View Results**: Check the `output/` directory for the results.
   - `output/optimized_spark_code.py`: The optimized code.
   - `output/performance_analysis.md`: Analysis of the changes.

---

### Examples

#### Example 1: Basic Optimization

**Input** (`v1/input/spark_code_input.py`):
```python
# Inefficient join
df1 = spark.read.csv("large_file.csv")
df2 = spark.read.csv("small_file.csv")
joined = df1.join(df2, "id")
joined.show()
```

**Output** (`v1/output/optimized_spark_code.py`):
```python
from pyspark.sql.functions import broadcast

# Optimized with broadcast join
df1 = spark.read.csv("large_file.csv")
df2 = spark.read.csv("small_file.csv")
# Broadcast the small dataframe to avoid shuffling
joined = df1.join(broadcast(df2), "id")
joined.limit(10).show() 
```

#### Example 2: Caching and Filtering

**Input**:
```python
df = spark.table("events")
# Filter after caching
df.cache()
df = df.filter(df.type == "error")
df.count()
```

**Output**:
```python
from pyspark.storagelevel import StorageLevel

df = spark.table("events")
# Filter BEFORE caching to save memory
df = df.filter(df.type == "error")
# Use persist with appropriate storage level
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()
```
