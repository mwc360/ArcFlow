"""
Stream endpoint validator for ArcFlow

Validates Kafka and Azure Event Hubs connection strings (format + network reachability)
*before* attempting to start a Spark stream. No Spark session required.

Usage
-----
    from arcflow.utils.endpoint_validator import validate_endpoint

    result = validate_endpoint(table_config)
    if not result.valid:
        raise ValueError(result.error)

    # Or validate a raw connection string directly:
    from arcflow.utils.endpoint_validator import StreamEndpointValidator
    result = StreamEndpointValidator.validate_kafka("Endpoint=sb://...")
    result = StreamEndpointValidator.validate_eventhub("Endpoint=sb://...")
"""
import re
import socket
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ValidationResult:
    """Result of an endpoint validation check."""
    valid: bool                          # False if any check failed
    format: str                          # 'kafka' or 'eventhub'
    endpoint: str                        # host:port that was probed
    topic: str                           # entity/topic name
    reachable: bool = False              # TCP probe succeeded
    latency_ms: Optional[float] = None  # Round-trip TCP latency
    error: Optional[str] = None         # Human-readable failure reason

    def __str__(self) -> str:
        if self.valid:
            return (
                f"[OK] {self.format} endpoint reachable "
                f"({self.endpoint}, topic={self.topic}, latency={self.latency_ms:.1f}ms)"
            )
        return f"[FAIL] {self.format} validation failed — {self.error}"


class StreamEndpointValidator:
    """
    Validates Kafka / Azure Event Hubs endpoints before starting a Spark stream.

    Two-phase check:
    1. **Format validation** — parse the connection string and verify all required
       fields are present (Endpoint, SharedAccessKeyName, SharedAccessKey, EntityPath).
    2. **Reachability probe** — open a TCP socket to the broker host:port and
       measure round-trip latency. Fails fast on DNS or network errors.

    Kafka / Event Hubs-via-Kafka uses port **9093** (SASL_SSL).
    Native Event Hubs uses port **443** (AMQP over TLS).
    """

    # Ports used for TCP probe
    _KAFKA_PORT = 9093
    _EVENTHUB_PORT = 443

    # Default socket timeout in seconds
    DEFAULT_TIMEOUT: float = 5.0

    # ---------------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------------

    @classmethod
    def validate_kafka(
        cls,
        source_uri: str,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> ValidationResult:
        """
        Validate a Kafka / Event Hubs-via-Kafka connection string.

        Accepts either:
        - A plain Event Hubs connection string:
            ``Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<topic>``
        - A SASL JAAS config string (password field must contain a valid connection string):
            ``org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://...";``

        Args:
            source_uri: Connection string or JAAS config string.
            timeout: TCP probe timeout in seconds.

        Returns:
            ValidationResult
        """
        try:
            host, topic = cls._parse_kafka(source_uri)
        except ValueError as exc:
            return ValidationResult(
                valid=False, format='kafka', endpoint='', topic='', error=str(exc)
            )

        endpoint = f"{host}:{cls._KAFKA_PORT}"
        reachable, latency_ms, tcp_error = cls._tcp_probe(host, cls._KAFKA_PORT, timeout)

        return ValidationResult(
            valid=reachable,
            format='kafka',
            endpoint=endpoint,
            topic=topic,
            reachable=reachable,
            latency_ms=latency_ms,
            error=tcp_error,
        )

    @classmethod
    def validate_eventhub(
        cls,
        source_uri: str,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> ValidationResult:
        """
        Validate a native Azure Event Hubs connection string.

        Expected format:
            ``Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<hub>``

        Args:
            source_uri: Event Hubs connection string.
            timeout: TCP probe timeout in seconds.

        Returns:
            ValidationResult
        """
        try:
            host, topic = cls._parse_eventhub(source_uri)
        except ValueError as exc:
            return ValidationResult(
                valid=False, format='eventhub', endpoint='', topic='', error=str(exc)
            )

        endpoint = f"{host}:{cls._EVENTHUB_PORT}"
        reachable, latency_ms, tcp_error = cls._tcp_probe(host, cls._EVENTHUB_PORT, timeout)

        return ValidationResult(
            valid=reachable,
            format='eventhub',
            endpoint=endpoint,
            topic=topic,
            reachable=reachable,
            latency_ms=latency_ms,
            error=tcp_error,
        )

    @classmethod
    def validate(cls, table_config, timeout: float = DEFAULT_TIMEOUT) -> ValidationResult:
        """
        Validate the endpoint defined in a FlowConfig.

        Dispatches to validate_kafka or validate_eventhub based on
        ``table_config.format``.

        Args:
            table_config: FlowConfig instance (format='kafka' or 'eventhub').
            timeout: TCP probe timeout in seconds.

        Returns:
            ValidationResult

        Raises:
            ValueError: If ``table_config.format`` is not 'kafka' or 'eventhub'.

        Example::

            result = StreamEndpointValidator.validate(table_config)
            if not result.valid:
                raise RuntimeError(f"Endpoint check failed: {result.error}")
        """
        if not table_config.source_uri:
            return ValidationResult(
                valid=False,
                format=table_config.format,
                endpoint='',
                topic='',
                error=f"FlowConfig '{table_config.name}' has no source_uri set.",
            )

        if table_config.format == 'kafka':
            return cls.validate_kafka(table_config.source_uri, timeout=timeout)
        elif table_config.format == 'eventhub':
            return cls.validate_eventhub(table_config.source_uri, timeout=timeout)
        else:
            raise ValueError(
                f"validate() only supports format='kafka' or 'eventhub', "
                f"got '{table_config.format}'."
            )

    # ---------------------------------------------------------------------------
    # Parsing helpers (no Spark, no network)
    # ---------------------------------------------------------------------------

    @classmethod
    def _parse_connection_string(cls, connection_string: str):
        """Parse key=value pairs from a semicolon-delimited connection string."""
        parts = {}
        for part in connection_string.split(';'):
            if '=' in part:
                key, value = part.split('=', 1)
                parts[key.strip()] = value.strip()
        return parts

    @classmethod
    def _parse_kafka(cls, source_uri: str):
        """
        Extract (host, topic) for a Kafka/EventHub-via-Kafka connection.
        Handles both plain connection strings and SASL JAAS config strings.
        Returns (host, topic) tuple.
        """
        source_uri = source_uri.strip()
        if source_uri.startswith('org.apache.kafka.common.security.plain.PlainLoginModule'):
            password_match = re.search(r'password="([^"]+)"', source_uri)
            if not password_match:
                raise ValueError(
                    "Could not extract password from JAAS config. "
                    "Expected: password=\"<Event Hubs connection string>\""
                )
            source_uri = password_match.group(1)

        return cls._extract_host_and_topic(source_uri)

    @classmethod
    def _parse_eventhub(cls, source_uri: str):
        """
        Extract (host, topic) from a native Event Hubs connection string.
        Returns (host, topic) tuple.
        """
        return cls._extract_host_and_topic(source_uri.strip())

    @classmethod
    def _extract_host_and_topic(cls, connection_string: str):
        """
        Validate and extract (host, topic) from an Event Hubs connection string.
        Raises ValueError with a descriptive message on any missing/malformed field.
        """
        parts = cls._parse_connection_string(connection_string)

        endpoint = parts.get('Endpoint', '')
        match = re.search(r'sb://([^/]+)', endpoint)
        if not match:
            raise ValueError(
                "Invalid connection string: missing or malformed Endpoint. "
                "Expected format: Endpoint=sb://<namespace>.servicebus.windows.net/"
            )
        host = match.group(1)

        if not parts.get('SharedAccessKeyName') or not parts.get('SharedAccessKey'):
            raise ValueError(
                "Invalid connection string: "
                "missing SharedAccessKeyName or SharedAccessKey"
            )

        topic = parts.get('EntityPath', '')
        if not topic:
            raise ValueError(
                "Invalid connection string: missing EntityPath (topic/hub name)"
            )

        return host, topic

    # ---------------------------------------------------------------------------
    # TCP probe
    # ---------------------------------------------------------------------------

    @classmethod
    def _tcp_probe(cls, host: str, port: int, timeout: float):
        """
        Open a TCP connection to host:port and measure latency.

        Returns:
            (reachable: bool, latency_ms: float | None, error: str | None)
        """
        start = time.monotonic()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                latency_ms = (time.monotonic() - start) * 1000
                return True, latency_ms, None
        except socket.timeout:
            return False, None, (
                f"TCP probe timed out after {timeout}s connecting to {host}:{port}. "
                "Check firewall rules or VNet peering."
            )
        except socket.gaierror as exc:
            return False, None, (
                f"DNS resolution failed for '{host}': {exc}. "
                "Verify the namespace name in the connection string."
            )
        except OSError as exc:
            return False, None, (
                f"Could not connect to {host}:{port} — {exc}"
            )


# ---------------------------------------------------------------------------
# Module-level convenience function
# ---------------------------------------------------------------------------

def validate_endpoint(table_config, timeout: float = StreamEndpointValidator.DEFAULT_TIMEOUT) -> ValidationResult:
    """
    Validate the streaming endpoint defined in a FlowConfig before starting a stream.

    Shortcut for ``StreamEndpointValidator.validate(table_config)``.

    Args:
        table_config: FlowConfig with format='kafka' or 'eventhub'.
        timeout: TCP probe timeout in seconds (default: 5.0).

    Returns:
        ValidationResult — check ``.valid`` before proceeding.

    Example::

        from arcflow.utils.endpoint_validator import validate_endpoint

        result = validate_endpoint(my_table_config)
        print(result)
        if not result.valid:
            raise RuntimeError(f"Cannot start stream: {result.error}")
    """
    return StreamEndpointValidator.validate(table_config, timeout=timeout)
