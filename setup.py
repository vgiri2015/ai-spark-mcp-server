from setuptools import setup, find_packages

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

setup(
    import configparser

config = configparser.ConfigParser()
config.read('config.ini')

if 'package' not in config or not config['package'].get('name') or not config['package'].get('version'):
    logger.error('Package configuration is incomplete. Please check config.ini')
    raise ValueError('Incomplete package configuration')

name = config['package']['name']
version = config['package']['version']
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "anthropic>=0.7.0",
        "asyncio>=3.4.3"
    ],
)
