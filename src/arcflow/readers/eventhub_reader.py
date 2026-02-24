"""
Native Azure Event Hubs reader for arcflow framework

Uses the azure-eventhubs-spark connector (not Kafka protocol).
JSON message bodies are deserialized using the schema defined in FlowConfig.

Prerequisites
-------------
The azure-eventhubs-spark connector JAR must be available to Spark:
    com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
"""
import json
import re
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from .base_reader import BaseReader


class EventHubReader(BaseReader):
    """
    Streaming (or batch) reader for Azure Event Hubs (native connector).

    Set ``source_uri`` to the full Event Hubs connection string:
        Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<hub>

    The connection string is parsed and encrypted automatically.
    Additional Event Hubs options can be passed via ``reader_options``.
    """

    # Event Hubs metadata columns to keep alongside the deserialized payload
    _METADATA_COLUMNS = {
        "offset":            "_eh_offset",
        "sequenceNumber":    "_eh_sequence_number",
        "enqueuedTime":      "_eh_enqueued_time",
        "publisher":         "_eh_publisher",
        "partitionKey":      "_eh_partition_key",
        "properties":        "_eh_properties",
        "systemProperties":  "_eh_system_properties",
    }

    # Starting position: read all available events from the beginning
    _DEFAULT_STARTING_POSITION = {
        "offset": "-1",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True,
    }

    def _parse_connection_string(self, connection_string: str) -> Dict[str, str]:
        """
        Parse an Azure Event Hubs connection string.

        Expected format:
            Endpoint=sb://<namespace>.servicebus.windows.net/;
            SharedAccessKeyName=<key_name>;SharedAccessKey=<key_value>;
            EntityPath=<hub_name>

        Returns:
            Dict with keys: connection_string (normalized), entity_path
        """
        parts = {}
        for part in connection_string.split(';'):
            if '=' in part:
                key, value = part.split('=', 1)
                parts[key] = value

        endpoint = parts.get('Endpoint', '')
        if not re.search(r'sb://[^/]+', endpoint):
            raise ValueError(
                "Invalid Event Hubs connection string: missing or malformed Endpoint"
            )

        key_name = parts.get('SharedAccessKeyName', '')
        key_value = parts.get('SharedAccessKey', '')
        if not key_name or not key_value:
            raise ValueError(
                "Invalid Event Hubs connection string: "
                "missing SharedAccessKeyName or SharedAccessKey"
            )

        entity_path = parts.get('EntityPath', '')
        if not entity_path:
            raise ValueError(
                "Invalid Event Hubs connection string: missing EntityPath"
            )

        return {
            'connection_string': connection_string,
            'entity_path': entity_path,
        }

    def _encrypt_connection_string(self, connection_string: str) -> str:
        """Encrypt the connection string using the EventHubsUtils JVM helper."""
        return (
            self.spark.sparkContext._jvm
            .org.apache.spark.eventhubs.EventHubsUtils
            .encrypt(connection_string)
        )

    def _build_eh_df(self, table_config):
        """Build the raw Event Hubs DataFrameReader/DataStreamReader (before load)."""
        if not table_config.source_uri:
            raise ValueError(
                f"FlowConfig '{table_config.name}' must set source_uri "
                "to the Event Hubs connection string when format='eventhub'."
            )

        parsed = self._parse_connection_string(table_config.source_uri)
        encrypted = self._encrypt_connection_string(parsed['connection_string'])

        starting_position = self._DEFAULT_STARTING_POSITION.copy()

        eh_conf = {
            'eventhubs.connectionString': encrypted,
            'eventhubs.startingPosition': json.dumps(starting_position),
        }

        # Apply any caller-supplied overrides / extras
        for key, value in table_config.reader_options.items():
            eh_conf[key] = value

        reader = self.get_reader()
        return reader.format('eventhubs').options(**eh_conf)

    def read_raw(self, table_config, limit: int = 5) -> DataFrame:
        """
        Read raw Event Hubs messages as strings for debugging.

        Delegates to ``read(raw=True)`` in batch mode. Returns a DataFrame with columns:
            payload                – the raw message body as a UTF-8 string
            _eh_offset             – partition offset
            _eh_sequence_number    – sequence number within partition
            _eh_enqueued_time      – enqueue timestamp
            _eh_publisher          – publisher tag (nullable)
            _eh_partition_key      – partition key (nullable)
            _eh_properties         – user-defined properties map
            _eh_system_properties  – system properties map

        Use this to inspect the actual message format before configuring a schema.

        Example::

            reader = EventHubReader(spark, is_streaming=False, config={})
            reader.read_raw(table_config).show(truncate=False)
        """
        orig = self.is_streaming
        self.is_streaming = False
        try:
            return self.read(table_config, raw=True).limit(limit)
        finally:
            self.is_streaming = orig

    def read(self, table_config, raw: bool = False, max_records: int = None) -> DataFrame:
        """
        Read from an Azure Event Hubs topic.

        Args:
            table_config: FlowConfig instance (format='eventhub', source_uri=<connection string>)
            raw: If True, return the raw body as a string column named ``payload`` without
                 applying schema deserialization. Useful for initial stream discovery before a
                 schema is defined. All metadata columns are included in both modes.
            max_records: If set, limits the total number of events read per trigger using
                         ``eventhubs.maxEventsPerTrigger``. Useful for test_input/test_output
                         to avoid draining the entire backlog. Has no effect in production
                         streaming (where no limit is desired).

        Returns:
            DataFrame with columns:

            - ``raw=False`` (default): deserialized JSON fields from ``table_config.schema``
              plus all Event Hubs metadata columns.
            - ``raw=True``: ``payload`` (string) plus all Event Hubs metadata columns.
              No schema required.
        """
        df = self._build_eh_df(table_config)

        if max_records is not None:
            df = df.option('eventhubs.maxEventsPerTrigger', max_records)

        try:
            df = df.load()
        except Exception as e:
            if 'eventhubs' in str(e).lower() and 'data source' in str(e).lower():
                raise RuntimeError(
                    "Event Hubs data source not found. The azure-eventhubs-spark connector JAR "
                    "must be available to Spark before reading from Event Hubs.\n\n"
                    "    com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22\n\n"
                    "  Local / dev — pass in spark_configs when creating your session:\n"
                    "    'spark.jars.packages': "
                    "'com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22'"
                ) from e
            raise

        if raw:
            return df.withColumn(
                'body', col('body').cast('string')
            )

        # Deserialize JSON body bytes into typed columns
        parsed = df.withColumn(
            'body', from_json(col('body').cast('string'), table_config.schema)
        )

        return parsed
