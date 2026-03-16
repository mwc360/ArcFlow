"""
ArcFlow Spark Job Definition - Main Entry Point

This file is the entry point for Microsoft Fabric Spark Job Definition.

DEPLOYMENT INSTRUCTIONS:
========================

1. Build the wheel:
   $ uv build
   
2. In Fabric Workspace:
   - Go to Environment settings
   - Upload: dist/arcflow-0.1.0-py3-none-any.whl
   - Add to environment libraries
   
3. Create Spark Job Definition:
   - Main file: Upload this file (main.py)
   - Reference files: Upload pipeline_config.py
   - Environment: Select environment with arcflow wheel
   - Reference Lakehouse: Select the Lakehouse where data should be written
   
4. Configure (in pipeline_config.py):
   - Define your tables
   - Define dimensions
   - Register DataFrame transformation functions with the `@register_zone_transfomer` decorator
   
5. Run the Spark Job Definition

CONFIGURATION:
==============
- Edit pipeline_config.py to define tables and transformations
- All default paths are in arcflow.config.ArcFlowDefaults
- Override config below in get_pipeline_config() as needed

"""
from arcflow import Controller
from lakegen.generators.mcmillan_industrial_group import McMillanDataGen
from pyspark.sql import SparkSession
import argparse
from pipeline_config import tables

import logging
import sys

def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-connection-string", type=str, required=False, help="Kafka connection string for CDC source (e.g. Azure Event Hubs or Fabric Eventstream)")
    p.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    return p.parse_args(argv)

def configure_logging(debug: bool) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)

def create_spark(app_name: str, debug: bool) -> SparkSession:
    spark = (
        SparkSession
            .builder
            .appName(app_name)
            .config('spark.databricks.delta.autoCompact.enabled', True)
            .config('spark.databricks.delta.autoCompact.onCheckpointOnly.enabled', True)
            .config('spark.microsoft.delta.targetFileSize.adaptive.enabled', True)
            .config('spark.microsoft.delta.optimize.fileLevelTarget.enabled', True)
            .config('spark.microsoft.delta.snapshot.driverMode.enabled', True)
            .config('spark.databricks.delta.properties.defaults.enableDeletionVectors', True)
            .config('spark.databricks.delta.optimizeWrite.enabled', True) # OW enabled since it's streaming micro batches
            .config('spark.native.enabled', True)
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO" if debug else "ERROR")
    return spark

def main(argv: list[str]) -> None:
    # parse input arguments
    args = parse_args(argv)

    # configure logging
    logger = configure_logging(args.debug)

    # assign SparkSession as variable
    spark = create_spark("myApp", args.debug)

    logger.info("=" * 80)
    logger.info("Starting LakeGen: McMillanDataGen")
    logger.info("=" * 80)

    target_folder_uri=f"/lakehouse/default/Files/landing/"

    logger.info(target_folder_uri)
    data_gen = McMillanDataGen(
        target_folder_uri=target_folder_uri,
        output_type_map={
            "shipment": "json",
            "shipment_scan_event": "json",
            "route": "parquet",
            "servicelevel": "parquet",
            "facility": "parquet",
            "exceptiontype": "parquet",
            "item": "parquet",
            "customer": "parquet",
            "order": "parquet",
        },
        max_events_per_second=10000,
        concurrenct_threads=1
    )
    data_gen.start(verbose=False)

    logger.info("=" * 80)
    logger.info("Starting ArcFlow ELT Framework")
    logger.info("=" * 80)

    # Configure pipeline
    config = {
        'streaming_enabled': True,
        'checkpoint_uri': "Files/checkpoints",
        'archive_uri': "Files/archive",
        'landing_uri': "Files/landing",
        'trigger_interval': '1 seconds', # default if not set at table level
        'await_termination': True #await_termination needed to keep Spark job from reaching terminal state
    }

    # Step 2: Initialize controller
    logger.info("Initializing ArcFlow Controller...")
    controller = Controller(
        spark=spark,
        config=config,
        table_registry=tables
    )
    logger.info("✓ Controller initialized")

    # Step 3: Run full pipeline
    logger.info("Starting full ELT pipeline...")
    controller.run_full_pipeline(zones=['bronze', 'silver'])

if __name__ == "__main__":
    main(sys.argv[1:])
