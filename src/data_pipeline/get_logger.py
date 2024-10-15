import logging
from logging.config import fileConfig
from pathlib import Path

fileConfig(Path(__file__).parent / 'logging_config.ini')
logger = logging.getLogger()