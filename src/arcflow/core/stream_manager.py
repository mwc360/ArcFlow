"""
Stream manager for tracking and managing streaming queries
"""
import logging
from typing import List, Optional, Dict, Any, Union
from pyspark.sql.streaming import StreamingQuery


class StreamManager:
    """
    Manages lifecycle of streaming queries
    
    Provides centralized control for:
    - Query registration
    - Status monitoring
    - Graceful shutdown
    - Error handling
    """
    
    def __init__(self):
        self.queries: List[StreamingQuery] = []
        self._zone_queries: Dict[str, List[StreamingQuery]] = {}
        self.logger = logging.getLogger(__name__)
    
    def register(self, query: Optional[StreamingQuery], zone: Optional[str] = None):
        """
        Register a streaming query for management.

        Also prunes terminated queries to prevent unbounded memory growth
        in long-running streaming jobs.
        
        Args:
            query: StreamingQuery instance (None for batch queries)
            zone: Optional zone name for zone-aware tracking
        """
        if query is not None:
            self._prune_terminated()
            self.queries.append(query)
            if zone is not None:
                self._zone_queries.setdefault(zone, []).append(query)
            self.logger.info(f"Registered query: {query.name}"
                           + (f" (zone={zone})" if zone else ""))
    
    def has_queries(self) -> bool:
        """Check if any queries are registered"""
        return len(self.queries) > 0

    def _prune_terminated(self):
        """Remove terminated queries to free Py4J / JVM references."""
        before = len(self.queries)
        self.queries = [q for q in self.queries if q.isActive]
        for zone in list(self._zone_queries):
            self._zone_queries[zone] = [
                q for q in self._zone_queries[zone] if q.isActive
            ]
            if not self._zone_queries[zone]:
                del self._zone_queries[zone]
        pruned = before - len(self.queries)
        if pruned > 0:
            self.logger.info(f"Pruned {pruned} terminated queries ({len(self.queries)} active)")
    
    def await_all(self, timeout: Optional[int] = None):
        """
        Wait for all queries to complete
        
        Args:
            timeout: Optional timeout in seconds
        """
        if not self.has_queries():
            self.logger.info("No streaming queries to await")
            return
        
        self.logger.info(f"Awaiting {len(self.queries)} streaming queries...")
        
        for query in self.queries:
            try:
                if timeout:
                    self.logger.info(f"Waiting for {query.name} (timeout: {timeout}s)")
                    query.awaitTermination(timeout)
                else:
                    self.logger.info(f"Waiting for {query.name}")
                    query.awaitTermination()
            except Exception as e:
                self.logger.error(f"Query {query.name} failed: {e}")
                raise
    
    def stop_all(self):
        """Stop all active queries gracefully and clear internal state"""
        self.logger.info(f"Stopping {len(self.queries)} streaming queries...")
        
        for query in self.queries:
            try:
                if query.isActive:
                    self.logger.info(f"Stopping query: {query.name}")
                    query.stop()
            except Exception as e:
                self.logger.error(f"Error stopping {query.name}: {e}")
        
        self.queries.clear()
        self._zone_queries.clear()
    
    def get_status(self, as_dataframe: bool = False) -> Union[Dict[str, Dict[str, Any]], Any]:
        """
        Get status of all queries
        
        Args:
            as_dataframe: If True, return a pandas DataFrame with flattened
                metrics from recent_progress. Defaults to False (dict output).
        
        Returns:
            Dict mapping query name to status info, or a pandas DataFrame
        """
        status = {}
        for query in self.queries:
            try:
                progress = query.recentProgress[-1] if query.recentProgress else None
                status[query.name] = {
                    'active': query.isActive,
                    'id': query.id,
                    'runId': query.runId,
                    'recent_progress': progress,
                }
            except Exception as e:
                status[query.name] = {'error': str(e)}
        
        if not as_dataframe:
            return status

        return self._status_to_dataframe(status)

    @staticmethod
    def _status_to_dataframe(status: Dict[str, Dict[str, Any]]):
        """Convert status dict to a pandas DataFrame with flattened progress metrics."""
        import pandas as pd

        rows = []
        for query_name, info in status.items():
            if 'error' in info:
                rows.append({'query_name': query_name, 'error': info['error']})
                continue

            progress = info.get('recent_progress')
            row: Dict[str, Any] = {
                'query_name': query_name,
                'active': info.get('active'),
                'id': str(info.get('id', '')),
                'run_id': str(info.get('runId', '')),
            }

            if progress is not None:
                row['batch_id'] = getattr(progress, 'batchId', None) if not isinstance(progress, dict) else progress.get('batchId')
                row['num_input_rows'] = getattr(progress, 'numInputRows', None) if not isinstance(progress, dict) else progress.get('numInputRows')
                row['input_rows_per_second'] = getattr(progress, 'inputRowsPerSecond', None) if not isinstance(progress, dict) else progress.get('inputRowsPerSecond')
                row['processed_rows_per_second'] = getattr(progress, 'processedRowsPerSecond', None) if not isinstance(progress, dict) else progress.get('processedRowsPerSecond')
                row['timestamp'] = getattr(progress, 'timestamp', None) if not isinstance(progress, dict) else progress.get('timestamp')

                # Flatten durationMs
                duration = getattr(progress, 'durationMs', None) if not isinstance(progress, dict) else progress.get('durationMs')
                if isinstance(duration, dict):
                    for k, v in duration.items():
                        row[f'duration_ms_{k}'] = v
            else:
                row.update({
                    'batch_id': None, 'num_input_rows': None,
                    'input_rows_per_second': None, 'processed_rows_per_second': None,
                    'timestamp': None,
                })

            rows.append(row)

        return pd.DataFrame(rows)
    
    def get_active_queries(self) -> List[StreamingQuery]:
        """Get list of currently active queries"""
        return [q for q in self.queries if q.isActive]
    
    def get_inactive_queries(self) -> List[StreamingQuery]:
        """Get list of inactive/completed queries"""
        return [q for q in self.queries if not q.isActive]
    
    def get_zone_queries(self, zone: str) -> List[StreamingQuery]:
        """Get all queries registered for a specific zone"""
        return list(self._zone_queries.get(zone, []))
    
    def has_active_zone_queries(self, zone: str) -> bool:
        """Check if any queries for a zone are still active"""
        return any(q.isActive for q in self._zone_queries.get(zone, []))
