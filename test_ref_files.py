from pyspark.sql import SparkSession
from pyspark import SparkFiles
import sys
import logging
import argparse
import yaml

# import your python packge
def load_table_registry(path: str) -> dict:
    with open(path, "r") as f:
        table_registry = yaml.safe_load(f)
    return table_registry

def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--zone", type=lambda s: s.lower(), required=True)
    p.add_argument("--load-group", type=int, default=0)
    p.add_argument("--compression", choices=["snappy", "zstd"], default="snappy")
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
    logger.info(f"Starting load group {args.load_group} for zone {args.zone}...")
    logger.info("=" * 80)

    import os
    from pyspark import SparkFiles

    logger.info("CWD:", os.getcwd())
    logger.info("SparkFiles root:", SparkFiles.getRootDirectory())
    logger.info("Files in SparkFiles root:", os.listdir(SparkFiles.getRootDirectory()))
    logger.info(sys.path.append(os.getcwd()))
    logger.info("Files in cwd root:", os.listdir(sys.path.append(os.getcwd())))
    try:
        print("Resolved path:", SparkFiles.get("table_registry.yaml"))
    except Exception as e:
        print("SparkFiles.get failed:", e)


    # main executable code
    path = SparkFiles.get("table_registry.yaml")
    tables_registry = load_table_registry(path)
    for t in tables_registry['tables']:
        logger.info(t['name'])
        print(t['name'])

    logger.info("=" * 80)
    logger.info(f"Completed load group {args.load_group} for zone {args.zone}...")
    logger.info("=" * 80)

if __name__ == "__main__":
    main(sys.argv[1:])