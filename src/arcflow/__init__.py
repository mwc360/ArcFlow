"""
arcflow: A zone-agnostic data lakehouse framework for PySpark

Scalable, testable, and production-ready ELT framework designed for:
- Single-source zone processing (landing -> bronze -> silver -> gold)
- Stream and batch processing with easy toggle for development
- Microsoft Fabric Spark Job Definition deployment
"""

__version__ = "0.1.0"

from .models import FlowConfig, StageConfig
from .controller import Controller
from .pipelines.zone_pipeline import ZonePipeline
from .config import Defaults, get_config
from .lock import JobLock, JobLockError
from .yaml_loader import load_yaml_config, load_tables
from .utils import build_table_reference, parse_table_reference, get_table_identifier

__all__ = [
    'FlowConfig',
    'StageConfig',
    'Controller',
    'ZonePipeline',
    'Defaults',
    'get_config',
    'JobLock',
    'JobLockError',
    'load_yaml_config',
    'load_tables',
    'build_table_reference',
    'parse_table_reference',
    'get_table_identifier',
]
