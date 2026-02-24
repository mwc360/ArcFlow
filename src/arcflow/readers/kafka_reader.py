"""
Kafka / Azure Event Hubs reader for arcflow framework

Parses an Event Hubs connection string and configures the PySpark Kafka
connector automatically. JSON-encoded messages are deserialized using
the schema defined in FlowConfig.

Prerequisites
-------------
The ``spark-sql-kafka`` connector JAR must be available to Spark:
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
"""
import re
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from .base_reader import BaseReader


class KafkaReader(BaseReader):
    """
    Streaming (or batch) reader for Kafka / Azure Event Hubs.

    Set ``source_uri`` to the full Kafka JAAS config string or Azure Event Hubs connection string:
        Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<topic>

    The topic, bootstrap server, and SASL credentials are parsed automatically.
    Additional Kafka options can be passed via ``reader_options``.
    """

    # Kafka metadata columns to keep alongside the deserialized payload
    _METADATA_COLUMNS = {
        "key":           "_kafka_key",
        "topic":         "_kafka_topic",
        "partition":     "_kafka_partition",
        "offset":        "_kafka_offset",
        "timestamp":     "_kafka_timestamp",
        "timestampType": "_kafka_timestamp_type",
        "headers":       "_kafka_headers",
    }

    def _parse_connection_string(self, connection_string: str) -> Dict[str, str]:
        """
        Parse an Azure Event Hubs connection string into Kafka connection params.

        Expected format:
            Endpoint=sb://<namespace>.servicebus.windows.net/;
            SharedAccessKeyName=<key_name>;SharedAccessKey=<key_value>;
            EntityPath=<topic>

        Returns:
            Dict with keys: bootstrap_servers, topic, jaas_config
        """
        parts = {}
        for part in connection_string.split(';'):
            if '=' in part:
                key, value = part.split('=', 1)
                parts[key] = value

        endpoint = parts.get('Endpoint', '')
        match = re.search(r'sb://([^/]+)', endpoint)
        if not match:
            raise ValueError(
                "Invalid Event Hubs connection string: missing or malformed Endpoint"
            )

        bootstrap_servers = f"{match.group(1)}:9093"

        key_name = parts.get('SharedAccessKeyName', '')
        key_value = parts.get('SharedAccessKey', '')
        if not key_name or not key_value:
            raise ValueError(
                "Invalid Event Hubs connection string: "
                "missing SharedAccessKeyName or SharedAccessKey"
            )

        topic = parts.get('EntityPath', '')
        if not topic:
            raise ValueError(
                "Invalid Event Hubs connection string: missing EntityPath (topic name)"
            )

        jaas_config = (
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="$ConnectionString" '
            f'password="{connection_string}";'
        )

        return {
            'bootstrap_servers': bootstrap_servers,
            'topic': topic,
            'jaas_config': jaas_config,
        }

    def _resolve_kafka_params(self, source_uri: str) -> Dict[str, str]:
        """
        Resolve Kafka connection params from either a raw Event Hubs connection
        string or a pre-built SASL JAAS config string.

        JAAS config format:
            org.apache.kafka.common.security.plain.PlainLoginModule required
            username="$ConnectionString" password="Endpoint=sb://...";

        In the JAAS case the embedded password is extracted and parsed to
        obtain the bootstrap server and topic; the provided JAAS string is
        used verbatim as ``kafka.sasl.jaas.config``.
        """
        source_uri = source_uri.strip()

        if source_uri.startswith('org.apache.kafka.common.security.plain.PlainLoginModule'):
            # Extract the connection string from the password field of the JAAS config
            password_match = re.search(r'password="([^"]+)"', source_uri)
            if not password_match:
                raise ValueError(
                    "Could not extract password from JAAS config. "
                    "Expected: password=\"<Event Hubs connection string>\""
                )
            conn_str = password_match.group(1)
            parsed = self._parse_connection_string(conn_str)
            # Use the provided JAAS config verbatim
            parsed['jaas_config'] = source_uri if source_uri.endswith(';') else source_uri + ';'
            return parsed

        # Plain Event Hubs connection string
        return self._parse_connection_string(source_uri)

    def _build_kafka_df(self, table_config):
        """Build the raw Kafka DataFrameReader/DataStreamReader (before load)."""
        if not table_config.source_uri:
            raise ValueError(
                f"FlowConfig '{table_config.name}' must set source_uri "
                "to the Event Hubs connection string when format='kafka'."
            )

        conn = self._resolve_kafka_params(table_config.source_uri)
        reader = self.get_reader()

        df = (
            reader.format('kafka')
            .option('kafka.bootstrap.servers', conn['bootstrap_servers'])
            .option('subscribe', conn['topic'])
            .option('kafka.security.protocol', 'SASL_SSL')
            .option('kafka.sasl.mechanism', 'PLAIN')
            .option('kafka.sasl.jaas.config', conn['jaas_config'])
            .option('startingOffsets', 'earliest')
            .option('failOnDataLoss', 'false')
        )

        for key, value in table_config.reader_options.items():
            df = df.option(key, value)

        return df

    def read_raw(self, table_config, limit: int = 5) -> "DataFrame":
        """
        Read raw Kafka messages as strings for debugging.

        Delegates to ``read(raw=True)`` in batch mode. Returns a DataFrame with columns:
            payload            – the raw message payload as a UTF-8 string
            _kafka_key         – message key as a string
            _kafka_topic       – topic name
            _kafka_partition   – partition number
            _kafka_offset      – message offset
            _kafka_timestamp   – enqueue timestamp
            _kafka_timestamp_type – 0=CreateTime, 1=LogAppendTime
            _kafka_headers     – array of header structs

        Use this to inspect the actual message format before configuring a schema.

        Example::

            reader = KafkaReader(spark, is_streaming=False, config={})
            reader.read_raw(table_config).show(truncate=False)
        """
        orig = self.is_streaming
        self.is_streaming = False
        try:
            return self.read(table_config, raw=True).limit(limit)
        finally:
            self.is_streaming = orig

    def read(self, table_config, raw: bool = False, max_records: int = None) -> "DataFrame":
        """
        Read from a Kafka / Event Hubs topic.

        Args:
            table_config: FlowConfig instance (format='kafka', source_uri=<connection string>)
            raw: If True, return the raw payload as a string column named ``payload`` without
                 applying schema deserialization. Useful for initial stream discovery before a
                 schema is defined. All metadata columns are included in both modes.
            max_records: If set, limits the total number of records read per trigger using
                         ``maxOffsetsPerTrigger``. Useful for test_input/test_output to avoid
                         draining the entire backlog. Has no effect in production streaming
                         (where no limit is desired).

        Returns:
            DataFrame with columns:

            - ``raw=False`` (default): deserialized JSON fields from ``table_config.schema``
              plus all Kafka metadata columns.
            - ``raw=True``: ``payload`` (string) plus all Kafka metadata columns.
              No schema required.
        """
        df = self._build_kafka_df(table_config)

        if max_records is not None:
            df = df.option('maxOffsetsPerTrigger', max_records)

        try:
            df = df.load()
        except Exception as e:
            if 'kafka' in str(e).lower() and 'data source' in str(e).lower():
                raise RuntimeError(
                    "Kafka data source not found. The spark-sql-kafka connector JAR must be "
                    "available to Spark before reading from Kafka / Event Hubs.\n\n"
                    "    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0\n\n"
                    "  Local / dev — pass in spark_configs when creating your session:\n"
                    "    'spark.jars.packages': "
                    "'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'"
                ) from e
            raise

        if raw:
            return df.withColumn(
                'value', col('value').cast('string')
            )

        # Deserialize JSON value bytes into typed columns
        parsed = df.withColumn(
                'value', from_json(col('value').cast('string'), table_config.schema)
            )

        return parsed
