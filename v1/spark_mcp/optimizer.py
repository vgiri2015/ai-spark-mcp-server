import os
import ast
import anthropic
from .utils import logger

class SparkOptimizer:
    def __init__(self):
        # Initialize Claude client with API key
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning("ANTHROPIC_API_KEY is not set. Optimization will fail.")
        self.client = anthropic.Client(api_key=api_key)

    @staticmethod
    def get_optimization_prompt(code: str, level: str) -> str:
        return f"You are an expert Apache Spark optimizer. Given the following PySpark code, optimize it for {level} performance. Apply these optimizations based on the level: For all levels: add limit(10) to show() operations and add appropriate imports. For medium and high levels: replace cache() with persist(StorageLevel.MEMORY_AND_DISK) and add broadcast hints for joins. For high level: add repartition before groupBy operations, use appropriate bucketing and partitioning, and optimize join strategies. Here's the code to optimize: {code} Return only the optimized code without explanations. Include all necessary imports. Do not include markdown backticks."

    def optimize_code(self, code: str, level: str = "medium") -> str:
        """
        Optimizes Spark code using Claude for intelligent optimizations.
        """
        try:
            # Get optimization suggestions from Claude
            message = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1500,
                temperature=0,
                messages=[{
                    "role": "user",
                    "content": self.get_optimization_prompt(code, level)
                }]
            )
            
            # Extract optimized code from response
            optimized_code = message.content[0].text
            
            # Clean up the response
            optimized_code = optimized_code.strip()
            if optimized_code.startswith("```python"):
                optimized_code = optimized_code[9:]
            elif optimized_code.startswith("```"):
                optimized_code = optimized_code[3:]
            
            if optimized_code.endswith("```"):
                optimized_code = optimized_code[:-3]
            
            optimized_code = optimized_code.strip()

            # Validate syntax
            try:
                ast.parse(optimized_code)
            except SyntaxError as e:
                logger.error(f"Generated code has syntax error: {e}")
                # Return the flawed code with a comment, so the user can see what happened
                return f"# Error: Generated code had syntax error\n# {e}\n# Original generation:\n'''\n{optimized_code}\n'''"

            return optimized_code

        except Exception as e:
            logger.error(f"Error during optimization: {e}")
            raise
