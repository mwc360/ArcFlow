"""
Main entry point for ArcFlow ELT Framework

Usage as Spark Job Definition in Microsoft Fabric:
1. Package this project: `uv build`
2. Upload wheel to Fabric workspace
3. Create Spark Job Definition with this file as main
4. Configure table registry and zones
5. Run job
"""
import logging
import sys
from typing import Dict

from arcflow import SourceConfig, DimensionConfig, ZoneConfig, Controller
from arcflow.core.spark_session import create_spark_session
from arcflow.config import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def get_pipeline_config() -> dict:
    """
    Get global pipeline configuration
    
    Uses ArcFlowDefaults for standard paths. Override any setting here.
    
    Override these settings based on environment:
    - Development: streaming_enabled=False for faster testing
    - Production: streaming_enabled=True for continuous processing
    """
    # Start with defaults and override as needed
    config = get_config({
        'streaming_enabled': True,  # Set to False for batch testing
        # All paths use defaults from ArcFlowDefaults:
        # - landing_uri: 'Files/landing/'
        # - checkpoint_uri: 'Files/checkpoints/'
        # - archive_uri: 'Files/archive/'
        # Tables use catalog references (e.g., 'bronze.shipment' or 'lakehouse.bronze.shipment')
    })
    return config


def get_table_registry() -> Dict[str, SourceConfig]:
    """
    Define all source tables and their zone configurations
    
    This is your "table catalog" - add entries here for each table
    you want to process through the lakehouse zones.
    
    Returns:
        Dict mapping table names to SourceConfig instances
    """
    tables = {}
    
    # Example: OPC UA sensor data
    tables['sensor_data'] = SourceConfig(
        name='sensor_data',
        format='parquet',
        source_uri='Files/landing/opc_ua/sensor_data/',
        zones={
            'bronze': ZoneConfig(
                enabled=True,
                mode='append',
                description='Raw sensor readings'
            ),
            'silver': ZoneConfig(
                enabled=True,
                mode='upsert',
                merge_keys=['sensor_id', 'timestamp'],
                custom_transform='clean_sensor_data',  # Table-specific cleaning
                description='Deduplicated and validated sensor data'
            ),
            'gold': ZoneConfig(
                enabled=True,
                mode='append',
                custom_transform='aggregate_sensor_hourly',
                description='Hourly sensor aggregations'
            )
        }
    )
    
    # Example: Equipment maintenance logs
    tables['maintenance_logs'] = SourceConfig(
        name='maintenance_logs',
        format='json',
        source_uri='Files/landing/maintenance/',
        json_explode_arrays=False,
        zones={
            'bronze': ZoneConfig(
                enabled=True,
                mode='append'
            ),
            'silver': ZoneConfig(
                enabled=True,
                mode='upsert',
                merge_keys=['log_id'],
                custom_transform='enrich_maintenance'
            )
        }
    )
    
    # Example: Production schedule
    tables['production_schedule'] = SourceConfig(
        name='production_schedule',
        format='parquet',
        source_uri='Files/landing/production/',
        zones={
            'bronze': ZoneConfig(enabled=True, mode='append'),
            'silver': ZoneConfig(enabled=True, mode='upsert', merge_keys=['schedule_id'])
        }
    )
    
    return tables


def get_dimension_registry() -> Dict[str, DimensionConfig]:
    """
    Define dimensional tables built from multiple sources
    
    These are multi-source tables like facts, dimensions, bridges
    that combine data from multiple bronze/silver tables.
    
    Returns:
        Dict mapping dimension names to DimensionConfig instances
    """
    dimensions = {}
    
    # Example: Equipment fact table (combines sensors + maintenance + schedule)
    dimensions['fact_equipment_performance'] = DimensionConfig(
        name='fact_equipment_performance',
        dimension_type='fact',
        source_tables=[
            {'table': 'sensor_data', 'zone': 'silver', 'alias': 'sensors'},
            {'table': 'maintenance_logs', 'zone': 'silver', 'alias': 'maintenance'},
            {'table': 'production_schedule', 'zone': 'silver', 'alias': 'schedule'}
        ],
        builder_transform='build_equipment_fact',  # Custom builder function
        mode='upsert',
        merge_keys=['equipment_id', 'timestamp'],
        enabled=True,
        description='Equipment performance metrics with maintenance and schedule context'
    )
    
    # Example: Sensor dimension (SCD Type 2)
    dimensions['dim_sensor'] = DimensionConfig(
        name='dim_sensor',
        dimension_type='dimension',
        source_tables=[
            {'table': 'sensor_data', 'zone': 'silver'}
        ],
        builder_transform='build_sensor_dimension',
        mode='upsert',
        merge_keys=['sensor_id', 'valid_from'],
        enabled=True,
        description='Sensor master data with SCD Type 2 history'
    )
    
    return dimensions


def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Starting ArcFlow ELT Framework")
    logger.info("=" * 80)
    
    try:
        # 1. Create Spark session with Delta optimizations
        logger.info("Creating Spark session...")
        spark = create_spark_session(app_name="ArcFlowELT")
        logger.info(f"Spark version: {spark.version}")
        
        # 2. Load configuration
        logger.info("Loading configuration...")
        config = get_pipeline_config()
        table_registry = get_table_registry()
        dimension_registry = get_dimension_registry()
        
        logger.info(f"Loaded {len(table_registry)} tables and {len(dimension_registry)} dimensions")
        
        # 3. Initialize orchestrator
        logger.info("Initializing orchestrator...")
        orchestrator = Controller(
            spark=spark,
            config=config,
            table_registry=table_registry,
            dimension_registry=dimension_registry
        )
        
        # 4. Run full pipeline
        logger.info("Starting full ELT pipeline...")
        orchestrator.run_full_pipeline(
            zones=['bronze', 'silver', 'gold'],
            include_dimensions=True
        )
        
        logger.info("=" * 80)
        logger.info("ArcFlow ELT Framework completed successfully")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully...")
        if 'orchestrator' in locals():
            orchestrator.stop_all()
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
