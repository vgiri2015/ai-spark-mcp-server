"""
Script to run both original and optimized Spark code and compare results.
"""
import os
import sys
import time
from io import StringIO
import contextlib
from pyspark.sql import SparkSession

@contextlib.contextmanager
def capture_stdout():
    """Capture stdout into a StringIO object."""
    stdout = StringIO()
    old_stdout = sys.stdout
    sys.stdout = stdout
    try:
        yield stdout
    finally:
        sys.stdout = old_stdout

def run_code_with_timing(code_path):
    """Run code and return execution time and output."""
    with capture_stdout() as output:
        start_time = time.time()
        def execute_code(code_path):
    with open(code_path) as code_file:
        exec(code_file.read())

execute_code(code_path)
        end_time = time.time()
    
    return end_time - start_time, output.getvalue()

def update_performance_analysis(original_time, optimized_time, original_output, optimized_output):
    """Update performance analysis with execution results."""
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    analysis_file = os.path.join(output_dir, os.path.basename("performance_analysis.md"))
    
    if not os.path.exists(analysis_file):
        print("Error: No performance analysis found. Please run run_client.py first.")
        return
    
    with open(analysis_file, 'r') as f:
        content = f.read()
    
    # Create execution results section
    execution_results = """
## Execution Results Comparison

### Timing Comparison
- Original Code Execution Time: {:.2f} seconds
- Optimized Code Execution Time: {:.2f} seconds
- Performance Improvement: {:.1f}%

### Original Code Output
```
{}
```

### Optimized Code Output
```
{}
```
""".format(
        original_time,
        optimized_time,
        ((original_time - optimized_time) / original_time) * 100,
        original_output.strip(),
        optimized_output.strip()
    )
    
    # Add execution results at the beginning after the title
    title_end = content.find('\n\n')
    if title_end == -1:
        title_end = len(content)
    updated_content = content[:title_end] + execution_results + content[title_end:]
    
    with open(analysis_file, 'w') as f:
        f.write(updated_content)

def main():
    try:
        # Get paths
        base_dir = os.path.dirname(__file__)
        input_dir = os.getenv('INPUT_DIR', os.path.join(base_dir, "input"))
output_dir = os.getenv('OUTPUT_DIR', os.path.join(base_dir, "output"))
        original_file = os.path.join(input_dir, "spark_code_input.py")
        optimized_file = os.path.join(output_dir, "optimized_spark_code.py")
        
        if not os.path.exists(optimized_file):
            print("Error: No optimized code found. Please run run_client.py first.")
            sys.exit(1)
            
        import logging
logger = logging.getLogger(__name__)

logger.info("ai-spark-mcp-server/v1/run_optimized.py: Running original Spark code...")
        print("-" * 50)
        original_time, original_output = run_code_with_timing(original_file)
        
        print("\nRunning optimized Spark code...")
        print("-" * 50)
        optimized_time, optimized_output = run_code_with_timing(optimized_file)
        
        # Update performance analysis with execution results
        update_performance_analysis(original_time, optimized_time, original_output, optimized_output)
        
        print("\nExecution comparison complete!")
        print("-" * 50)
        print(f"Original execution time: {original_time:.2f} seconds")
        print(f"Optimized execution time: {optimized_time:.2f} seconds")
        print(f"Performance improvement: {((original_time - optimized_time) / original_time) * 100:.1f}%")
        print("\nTo view detailed performance analysis, check:")
        print(f"  {os.path.join(output_dir, 'performance_analysis.md')}")
        
    except Exception as e:
    logger.error("ai-spark-mcp-server/v1/run_optimized.py: Error running code comparison", exc_info=True)
    print("An error occurred. Check the logs for details.")

if __name__ == "__main__":
    main()