"""
Global configuration defaults for ArcFlow

Centralized configuration for default paths, settings, and constants.
Override these values in your pipeline config dict.
"""
from typing import Dict, Any


class Defaults:
    """
    Default configuration values for ArcFlow framework
    
    These can be overridden by passing values in the config dict
    when initializing pipelines or orchestrator.
    """
    
    # =========================================================================
    # Path Configuration
    # =========================================================================
    
    # Base paths for file storage
    LANDING_URI: str = "Files/landing/"
    ARCHIVE_URI: str = "Files/archive/"
    CHECKPOINT_URI: str = "Files/checkpoints/"
    
    # =========================================================================
    # Streaming Configuration
    # =========================================================================
    
    STREAMING_ENABLED: bool = True
    MAX_FILES_PER_TRIGGER: int = 1000
    TRIGGER_INTERVAL: str = "60 seconds"
    AWAIT_TERMINATION: bool = False  # False for notebooks, True for production Spark jobs
    EVENT_DRIVEN_CHAINING: bool = True  # Cascade downstream zones via StreamingQueryListener
    FAIL_FAST: bool = True  # True: raise on first stream failure. False: log and continue.
    
    # =========================================================================
    # Delta Lake Configuration
    # =========================================================================
    
    OPTIMIZE_WRITE: bool = True
    AUTO_COMPACT: bool = True
    
    # =========================================================================
    # Retry Configuration
    # =========================================================================
    
    MAX_RETRIES: int = 3
    RETRY_DELAY_SECONDS: int = 60
    
    # =========================================================================
    # Spark Configuration
    # =========================================================================
    
    SPARK_SHUFFLE_PARTITIONS: int = 200
    SPARK_SQL_ADAPTIVE_ENABLED: bool = True
    AUTOSET_SPARK_CONFIGS: bool = True
    
    # =========================================================================
    # Job Lock Configuration
    # =========================================================================
    
    JOB_ID: str = None                          # User-provided job identifier (required to enable lock)
    JOB_LOCK_ENABLED: bool = False              # Opt-in singleton lock
    JOB_LOCK_PATH: str = "Files/locks/"         # Base directory for lock files
    JOB_LOCK_TIMEOUT_SECONDS: int = 3600        # Wait timeout before failing (1 hour)
    JOB_LOCK_POLL_INTERVAL: int = 30            # Seconds between retry checks
    
    @classmethod
    def get_default_config(cls) -> Dict[str, Any]:
        """
        Get complete default configuration as dict
        
        Returns:
            Dict with all default settings
        """
        return {
            # Paths
            'landing_uri': cls.LANDING_URI,
            'archive_uri': cls.ARCHIVE_URI,
            'checkpoint_uri': cls.CHECKPOINT_URI,
            
            # Streaming
            'streaming_enabled': cls.STREAMING_ENABLED,
            'max_files_per_trigger': cls.MAX_FILES_PER_TRIGGER,
            'trigger_interval': cls.TRIGGER_INTERVAL,
            'await_termination': cls.AWAIT_TERMINATION,
            'event_driven_chaining': cls.EVENT_DRIVEN_CHAINING,
            'fail_fast': cls.FAIL_FAST,
            
            # Delta Lake
            'optimize_write': cls.OPTIMIZE_WRITE,
            'auto_compact': cls.AUTO_COMPACT,
            
            # Retry
            'max_retries': cls.MAX_RETRIES,
            'retry_delay_seconds': cls.RETRY_DELAY_SECONDS,
            
            # Spark
            'spark.sql.shuffle.partitions': cls.SPARK_SHUFFLE_PARTITIONS,
            'spark.sql.adaptive.enabled': cls.SPARK_SQL_ADAPTIVE_ENABLED,
            
            # Job Lock
            'job_id': cls.JOB_ID,
            'job_lock_enabled': cls.JOB_LOCK_ENABLED,
            'job_lock_path': cls.JOB_LOCK_PATH,
            'job_lock_timeout_seconds': cls.JOB_LOCK_TIMEOUT_SECONDS,
            'job_lock_poll_interval': cls.JOB_LOCK_POLL_INTERVAL,
        }
    
    @classmethod
    def merge_with_defaults(cls, user_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge user config with defaults
        
        Args:
            user_config: User-provided configuration dict
            
        Returns:
            Merged configuration with defaults filled in
        """
        config = cls.get_default_config()
        config.update(user_config)
        return config


# Convenience function for getting merged config
def get_config(user_config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Get configuration with defaults
    
    Args:
        user_config: Optional user configuration to override defaults
        
    Returns:
        Complete configuration dict
        
    Example:
        >>> config = get_config({'streaming_enabled': False})
        >>> config['landing_uri']
        'Files/landing/'
    """
    if user_config is None:
        user_config = {}
    return Defaults.merge_with_defaults(user_config)


# Export for convenience
__all__ = [
    'Defaults',
    'get_config',
]
