from setuptools import setup, find_packages

setup(
    name="spark-mcp",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "anthropic>=0.7.0",
        "asyncio>=3.4.3"
    ],
)
