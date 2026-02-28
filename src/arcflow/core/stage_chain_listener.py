"""
Event-driven stage chaining via Spark StreamingQueryListener

Listens for upstream query progress/termination events and spawns
downstream zone stages as availableNow queries. Provides efficient
cascading: bronze → silver → gold without polling.
"""
import logging
import threading
from typing import Callable, Dict, List, Optional, Set

from pyspark.sql.streaming import StreamingQueryListener


class StageChainListener(StreamingQueryListener):
    """
    Triggers downstream zone stages when upstream queries produce data.

    For processingTime upstream: triggers on onQueryProgress when numInputRows > 0.
    For availableNow upstream: triggers on onQueryTerminated after processing completes.

    Dedup: if a downstream zone is already running, sets a pending re-trigger
    flag so it re-spawns after the current run finishes.
    """

    def __init__(
        self,
        zone_chain: List[str],
        spawn_zone_fn: Callable[[str], List],
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            zone_chain: Ordered list of zones, e.g. ['bronze', 'silver', 'gold'].
            spawn_zone_fn: Callable(zone) -> List[StreamingQuery]. Provided by
                Controller to create and start downstream queries.
            logger: Optional logger instance.
        """
        super().__init__()
        self._zone_chain = zone_chain
        self._spawn_zone = spawn_zone_fn
        self.logger = logger or logging.getLogger(__name__)

        # zone -> next zone (derived from chain order)
        self._downstream: Dict[str, str] = {}
        for i in range(len(zone_chain) - 1):
            self._downstream[zone_chain[i]] = zone_chain[i + 1]

        # query_name -> zone
        self._query_zone: Dict[str, str] = {}
        # query_name -> trigger_mode
        self._query_trigger: Dict[str, str] = {}
        # query_id (str) -> query_name (for termination event matching)
        self._id_to_name: Dict[str, str] = {}

        # Track which zones have active downstream queries and pending re-triggers
        self._active_downstream: Set[str] = set()
        self._pending_retrigger: Set[str] = set()

        # Count of active queries per zone (for multi-table zones)
        self._zone_active_count: Dict[str, int] = {}
        # Track whether any query in the zone produced rows (for availableNow)
        self._zone_had_output: Set[str] = set()

        self._lock = threading.Lock()

    def register_query(self, query, zone: str, trigger_mode: str):
        """
        Register a streaming query for event tracking.

        Args:
            query: StreamingQuery instance (needs .name and .id).
            zone: Zone this query belongs to.
            trigger_mode: 'availableNow', 'processingTime', or 'continuous'.
        """
        with self._lock:
            self._query_zone[query.name] = zone
            self._query_trigger[query.name] = trigger_mode
            self._id_to_name[str(query.id)] = query.name
            self._zone_active_count[zone] = self._zone_active_count.get(zone, 0) + 1
            self.logger.debug(
                f"StageChainListener: registered {query.name} "
                f"(zone={zone}, trigger={trigger_mode}, id={query.id})"
            )

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        """
        Called after each micro-batch.

        For processingTime/continuous: triggers downstream immediately if rows produced.
        For availableNow: marks zone as having output (downstream triggered on termination).
        """
        progress = event.progress
        query_name = progress.name
        if query_name is None:
            return

        with self._lock:
            zone = self._query_zone.get(query_name)
            trigger_mode = self._query_trigger.get(query_name)

        if zone is None or trigger_mode is None:
            return

        num_input_rows = progress.numInputRows
        if num_input_rows is None or num_input_rows <= 0:
            return

        if trigger_mode == 'availableNow':
            with self._lock:
                self._zone_had_output.add(zone)
            return

        # processingTime or continuous: trigger downstream now
        downstream_zone = self._downstream.get(zone)
        if downstream_zone is not None:
            self._try_spawn_downstream(downstream_zone)

    def onQueryTerminated(self, event):
        """
        Called when a streaming query stops.

        Waits for all queries in a zone to terminate, then triggers downstream
        if data was produced (for availableNow) or checks pending re-triggers
        (for spawned downstream queries).
        """
        query_id = str(event.id)

        with self._lock:
            query_name = self._id_to_name.get(query_id)
            if query_name is None:
                return

            zone = self._query_zone.get(query_name)
            if zone is None:
                return

            # Decrement active count for this zone
            self._zone_active_count[zone] = max(
                0, self._zone_active_count.get(zone, 1) - 1
            )
            active_remaining = self._zone_active_count[zone]

        # Wait until ALL queries in the zone have terminated
        if active_remaining > 0:
            self.logger.debug(
                f"StageChainListener: {query_name} terminated, "
                f"{active_remaining} still active in {zone}"
            )
            return

        self.logger.info(f"StageChainListener: all queries in {zone} terminated")

        # Determine next action outside the lock to avoid deadlocks with _try_spawn
        action = None  # 'spawn_downstream', 'retrigger', or None
        target_zone = None

        with self._lock:
            had_output = zone in self._zone_had_output
            self._zone_had_output.discard(zone)

            if zone in self._active_downstream:
                # This zone was a spawned downstream — clear its active flag
                self._active_downstream.discard(zone)

                if zone in self._pending_retrigger:
                    self._pending_retrigger.discard(zone)
                    action = 'retrigger'
                    target_zone = zone
                elif had_output:
                    next_zone = self._downstream.get(zone)
                    if next_zone:
                        action = 'spawn_downstream'
                        target_zone = next_zone
            elif had_output:
                # This was an input zone (first zone or any non-spawned zone)
                next_zone = self._downstream.get(zone)
                if next_zone:
                    action = 'spawn_downstream'
                    target_zone = next_zone

        if action == 'retrigger':
            self.logger.info(f"StageChainListener: re-triggering {target_zone}")
            self._try_spawn_downstream(target_zone)
        elif action == 'spawn_downstream':
            self._try_spawn_downstream(target_zone)

    def _try_spawn_downstream(self, zone: str):
        """Spawn downstream zone queries with dedup protection."""
        with self._lock:
            if zone in self._active_downstream:
                self._pending_retrigger.add(zone)
                self.logger.info(
                    f"StageChainListener: {zone} already active, "
                    f"marking pending retrigger"
                )
                return
            self._active_downstream.add(zone)

        self.logger.info(f"StageChainListener: spawning {zone} as availableNow")
        try:
            queries = self._spawn_zone(zone)
            # register_query acquires _lock internally; don't wrap in another lock
            for q in queries:
                self.register_query(q, zone, 'availableNow')
        except Exception as e:
            self.logger.error(f"StageChainListener: failed to spawn {zone}: {e}")
            with self._lock:
                self._active_downstream.discard(zone)
