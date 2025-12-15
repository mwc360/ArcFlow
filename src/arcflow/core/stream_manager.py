"""
Stream manager for tracking and managing streaming queries
"""
import logging
from typing import List, Optional, Dict, Any
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
        self.logger = logging.getLogger(__name__)
    
    def register(self, query: Optional[StreamingQuery]):
        """
        Register a streaming query for management
        
        Args:
            query: StreamingQuery instance (None for batch queries)
        """
        if query is not None:
            self.queries.append(query)
            self.logger.info(f"Registered query: {query.name}")
    
    def has_queries(self) -> bool:
        """Check if any queries are registered"""
        return len(self.queries) > 0
    
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
        """Stop all active queries gracefully"""
        self.logger.info(f"Stopping {len(self.queries)} streaming queries...")
        
        for query in self.queries:
            try:
                if query.isActive:
                    self.logger.info(f"Stopping query: {query.name}")
                    query.stop()
            except Exception as e:
                self.logger.error(f"Error stopping {query.name}: {e}")
    
    def get_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status of all queries
        
        Returns:
            Dict mapping query name to status info
        """
        status = {}
        for query in self.queries:
            try:
                status[query.name] = {
                    'active': query.isActive,
                    'id': query.id,
                    'runId': query.runId,
                    'recent_progress': query.recentProgress[-1] if query.recentProgress else None
                }
            except Exception as e:
                status[query.name] = {'error': str(e)}
        
        return status
    
    def get_active_queries(self) -> List[StreamingQuery]:
        """Get list of currently active queries"""
        return [q for q in self.queries if q.isActive]
    
    def get_inactive_queries(self) -> List[StreamingQuery]:
        """Get list of inactive/completed queries"""
        return [q for q in self.queries if not q.isActive]
