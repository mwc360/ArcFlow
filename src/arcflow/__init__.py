"""
arcflow: A zone-agnostic data lakehouse framework for PySpark

Scalable, testable, and production-ready ELT framework designed for:
- Single-source zone processing (landing -> bronze -> silver -> gold)
- Multi-source dimensional modeling (facts, dimensions, bridges)
- Stream and batch processing with easy toggle for development
- Microsoft Fabric Spark Job Definition deployment
"""

__version__ = "0.1.0"

from .models import SourceConfig, DimensionConfig, ZoneConfig
from .controller import Controller
from .pipelines.zone_pipeline import ZonePipeline
from .pipelines.dimension_pipeline import DimensionPipeline
from .config import Defaults, get_config
from .utils import build_table_reference, parse_table_reference, get_table_identifier

__all__ = [
    'SourceConfig',
    'DimensionConfig', 
    'ZoneConfig',
    'Controller',
    'ZonePipeline',
    'DimensionPipeline',
    'Defaults',
    'get_config',
    'build_table_reference',
    'parse_table_reference',
    'get_table_identifier',
]
