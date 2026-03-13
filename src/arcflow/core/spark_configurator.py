"""
Spark Best-Practice Configurator

Automatically applies recommended Spark configurations to an existing SparkSession.
Used by Controller on startup when autoset_spark_configs=True (default).

Configs are only applied if not already set — user-supplied values are never overwritten.
Per-key overrides can be passed via the `overrides` dict.
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkConfigurator:
    """
    Applies best-practice Spark configurations to a live SparkSession.

    Configs are skipped if already set on the session (unless passed in `overrides`).
    Configs that are read-only at runtime are silently skipped.

    Usage:
        result = SparkConfigurator.apply(spark)
        # result = {'applied': [...], 'skipped': [...], 'failed': [...]}

    To disable auto-apply:
        config = {'autoset_spark_configs': False}

    To override specific keys:
        config = {'spark_config_overrides': {'spark.sql.shuffle.partitions': '200'}}
    """

    BEST_PRACTICE_CONFIGS: Dict[str, str] = {
        # Microsoft Features
        "spark.microsoft.delta.targetFileSize.adaptive.enabled": "true",
        "spark.microsoft.delta.optimize.fast.enabled": "true",
        "spark.microsoft.delta.optimize.fileLevelTarget.enabled": "true",
        "spark.microsoft.delta.stats.collect.extended": "false",
        "spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation": "false",
        "spark.microsoft.delta.snapshot.driverMode.enabled": "true",
        "spark.microsoft.delta.parallelSnapshotLoading.enabled": "true",
        "spark.sql.parquet.native.writer.directWriteEnabled": "true",
        "spark.sql.parquet.vorder.enabled": None,
        "spark.native.enabled": "true",
        # OSS Features
        "spark.databricks.delta.optimizeWrite.enabled": None,
        "spark.databricks.delta.optimizeWrite.binSize": "128m",
        "spark.databricks.delta.properties.defaults.enableDeletionVectors": "true",
        "spark.databricks.delta.optimize.maxFileSize": "128m",
        "spark.sql.parquet.compression.codec": "zstd",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    }

    # Applied separately — not available in all environments
    _ROCKSDB_CONFIG = {
        "spark.sql.streaming.stateStore.providerClass": (
            "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
        )
    }

    # TODO: write jobs of a specific use case will apply configs automatically to the specific job
    _USE_CASE_CONFIGS = {
        "query_optimized": {
            "spark.microsoft.delta.stats.collect.extended": "true",
            "spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation": "true",
        },
        "streaming": {
            "spark.sql.streaming.stateStore.providerClass": (
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
            ),
            "spark.databricks.delta.optimizeWrite.enabled": "true"
        }
    }

    @classmethod
    def apply(
        cls,
        spark: SparkSession,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, list]:
        """
        Apply best-practice configs to the SparkSession.

        Args:
            spark: Active SparkSession to configure.
            overrides: Optional dict of config key -> value that will always be
                       written (overrides both defaults and existing session values).
                       Set a value to None to unset that config key on the session.

        Returns:
            Dict with keys 'applied', 'skipped', 'failed', 'unset' — each a list of config keys.
        """
        raw_overrides = overrides or {}
        # Separate unset requests (None) from value overrides
        unset_keys = {str(k) for k, v in raw_overrides.items() if v is None}
        overrides = {str(k): str(v) for k, v in raw_overrides.items() if v is not None}

        applied, skipped, failed, unset = [], [], [], []

        # Unset any keys explicitly set to None in overrides
        for key in unset_keys:
            try:
                spark.conf.unset(key)
                logger.debug(f"[SparkConfigurator] unset {key}")
                unset.append(key)
            except Exception as e:
                logger.debug(f"[SparkConfigurator] failed to unset {key}: {e}")
                failed.append(key)

        # Remove unset keys and None-valued defaults so they aren't re-applied
        configs_to_set = {
            k: v for k, v in {**cls.BEST_PRACTICE_CONFIGS, **overrides}.items()
            if k not in unset_keys and v is not None
        }

        for key, value in configs_to_set.items():
            is_override = key in overrides
            try:
                current = cls._get_conf(spark, key)
                if current is not None and not is_override:
                    logger.debug(f"[SparkConfigurator] skipped {key} (already set to '{current}')")
                    skipped.append(key)
                    continue
                spark.conf.set(key, value)
                logger.debug(f"[SparkConfigurator] set {key} = {value}")
                applied.append(key)
            except Exception as e:
                logger.debug(f"[SparkConfigurator] failed to set {key}: {e}")
                failed.append(key)

        # RocksDB state store — best-effort only
        for key, value in cls._ROCKSDB_CONFIG.items():
            try:
                current = cls._get_conf(spark, key)
                if current is None:
                    spark.conf.set(key, value)
                    applied.append(key)
                    logger.debug(f"[SparkConfigurator] set {key} = {value}")
                else:
                    skipped.append(key)
            except Exception:
                pass  # Not available in this environment — silently skip

        logger.info(
            f"[SparkConfigurator] applied={len(applied)}, "
            f"skipped={len(skipped)}, unset={len(unset)}, failed={len(failed)}"
        )
        if failed:
            logger.warning(f"[SparkConfigurator] failed keys: {failed}")

        return {"applied": applied, "skipped": skipped, "unset": unset, "failed": failed}

    @staticmethod
    def _get_conf(spark: SparkSession, key: str) -> Optional[str]:
        """Return the current value of a Spark config key, or None if not set."""
        try:
            return spark.conf.get(key)
        except Exception:
            return None


__all__ = ["SparkConfigurator"]
