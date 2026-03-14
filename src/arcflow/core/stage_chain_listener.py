"""
Event-driven stage chaining via Spark StreamingQueryListener

Listens for upstream query progress/termination events and spawns
downstream zone stages as availableNow queries.  Operates at per-table
granularity: when bronze_item produces data only silver_item is triggered,
not the entire silver zone.

On startup, a first-batch cascade ensures each table triggers its
downstream counterpart once (even with 0 rows) so the full chain runs
at least once — this replaces explicit recovery logic.
"""
import logging
import threading
from typing import Callable, Dict, List, Optional, Set, Tuple

from pyspark.sql.streaming import StreamingQueryListener


class StageChainListener(StreamingQueryListener):
    """
    Per-table event-driven stage chaining.

    For processingTime upstream: triggers downstream on onQueryProgress when
    numInputRows > 0 (or unconditionally on the first batch for cascade).
    For availableNow upstream: triggers downstream on onQueryTerminated.

    Dedup: if a downstream (zone, table) is already running, a pending
    re-trigger flag is set so it re-spawns after the current run finishes.
    """

    def __init__(
        self,
        zone_chain: List[str],
        spawn_table_fn: Callable[[str, str], Optional[object]],
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            zone_chain: Ordered list of zones, e.g. ['bronze', 'silver', 'gold'].
            spawn_table_fn: Callable(zone, table_name) -> Optional[StreamingQuery].
                Spawns a single table in the given zone as availableNow.
            logger: Optional logger instance.
        """
        super().__init__()
        self._zone_chain = zone_chain
        self._spawn_table = spawn_table_fn
        self.logger = logger or logging.getLogger(__name__)

        # zone -> next zone (derived from chain order)
        self._downstream: Dict[str, str] = {}
        for i in range(len(zone_chain) - 1):
            self._downstream[zone_chain[i]] = zone_chain[i + 1]

        # --- Per-query metadata ---
        # query_name -> zone
        self._query_zone: Dict[str, str] = {}
        # query_name -> trigger_mode
        self._query_trigger: Dict[str, str] = {}
        # query_id (str) -> query_name
        self._id_to_name: Dict[str, str] = {}
        # query_name -> table_name (FlowConfig.name)
        self._query_table: Dict[str, str] = {}

        # --- Per-table downstream tracking ---
        # (zone, table_name) pairs with an active downstream query
        self._active_downstream_tables: Set[Tuple[str, str]] = set()
        # (zone, table_name) pairs queued for re-trigger after current run
        self._pending_retrigger_tables: Set[Tuple[str, str]] = set()
        # (zone, table_name) pairs that produced rows in the current run
        self._table_had_output: Set[Tuple[str, str]] = set()

        # --- First-batch cascade ---
        # (zone, table_name) pairs that have already triggered their initial
        # downstream cascade.  Before a pair appears here, the first event
        # (progress or termination) always triggers downstream — even with
        # 0 rows — so the full chain runs at least once on startup.
        self._initial_cascade_done: Set[Tuple[str, str]] = set()

        self._lock = threading.Lock()

    # ── Registration ────────────────────────────────────────────────

    def register_query(
        self,
        query,
        zone: str,
        trigger_mode: str,
        table_name: str,
    ):
        """
        Register a streaming query for event tracking.

        Args:
            query: StreamingQuery instance (needs .name and .id).
            zone: Zone this query belongs to.
            trigger_mode: 'availableNow', 'processingTime', or 'continuous'.
            table_name: FlowConfig.name this query corresponds to.
        """
        with self._lock:
            self._query_zone[query.name] = zone
            self._query_trigger[query.name] = trigger_mode
            self._id_to_name[str(query.id)] = query.name
            self._query_table[query.name] = table_name
            self.logger.debug(
                f"StageChainListener: registered {query.name} "
                f"(zone={zone}, table={table_name}, trigger={trigger_mode}, "
                f"id={query.id})"
            )

    # ── Public helpers (for Controller) ─────────────────────────────

    def mark_table_active(self, zone: str, table_name: str) -> bool:
        """Mark a (zone, table) as active downstream.

        Returns False if already active (caller should skip spawning).
        """
        key = (zone, table_name)
        with self._lock:
            if key in self._active_downstream_tables:
                return False
            self._active_downstream_tables.add(key)
            return True

    def clear_table_active(self, zone: str, table_name: str):
        """Clear a (zone, table) active flag — e.g. on spawn failure."""
        key = (zone, table_name)
        with self._lock:
            self._active_downstream_tables.discard(key)

    # ── Listener callbacks ──────────────────────────────────────────

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        """
        Called after each micro-batch.

        For processingTime/continuous: triggers downstream table immediately
        if rows were produced (or unconditionally on the very first batch).
        For availableNow: marks the table as having output; downstream is
        triggered on termination.
        """
        progress = event.progress
        query_name = progress.name
        if query_name is None:
            return

        with self._lock:
            zone = self._query_zone.get(query_name)
            trigger_mode = self._query_trigger.get(query_name)
            table_name = self._query_table.get(query_name)

        if zone is None or trigger_mode is None or table_name is None:
            return

        num_input_rows = progress.numInputRows
        has_rows = num_input_rows is not None and num_input_rows > 0

        # First-batch cascade check
        key = (zone, table_name)
        with self._lock:
            needs_initial_cascade = key not in self._initial_cascade_done

        if trigger_mode == 'availableNow':
            with self._lock:
                if has_rows:
                    self._table_had_output.add(key)
                # Don't mark _initial_cascade_done here — availableNow defers
                # downstream triggering to onQueryTerminated where the cascade
                # (and had_output check) are handled together.
            return

        # processingTime or continuous: trigger downstream now
        downstream_zone = self._downstream.get(zone)
        if downstream_zone is None:
            # Last zone — mark initial cascade done so we don't re-check
            if needs_initial_cascade:
                with self._lock:
                    self._initial_cascade_done.add(key)
            return

        if has_rows or needs_initial_cascade:
            if needs_initial_cascade:
                with self._lock:
                    self._initial_cascade_done.add(key)
                self.logger.info(
                    f"StageChainListener: first-batch cascade "
                    f"{zone}.{table_name} → {downstream_zone}.{table_name}"
                )
            self._try_spawn_downstream_table(downstream_zone, table_name)

    def onQueryTerminated(self, event):
        """
        Called when a streaming query stops.

        Routes to the appropriate handler based on whether this was a
        spawned downstream table or an upstream (root) query.
        """
        query_id = str(event.id)

        with self._lock:
            query_name = self._id_to_name.get(query_id)
            if query_name is None:
                return

            zone = self._query_zone.get(query_name)
            if zone is None:
                return

            table_name = self._query_table.get(query_name)
            if table_name is None:
                return

            key = (zone, table_name)
            is_spawned_downstream = key in self._active_downstream_tables

        if is_spawned_downstream:
            self._handle_downstream_termination(zone, table_name)
        else:
            self._handle_upstream_termination(query_name, zone, table_name)

    # ── Internal handlers ───────────────────────────────────────────

    def _handle_downstream_termination(self, zone: str, table_name: str):
        """Handle termination of a spawned downstream table."""
        key = (zone, table_name)
        action = None
        target_zone = None

        with self._lock:
            self._active_downstream_tables.discard(key)
            had_output = key in self._table_had_output
            self._table_had_output.discard(key)

            if key in self._pending_retrigger_tables:
                # New upstream data arrived while we were running — re-process
                self._pending_retrigger_tables.discard(key)
                action = 'retrigger'
                target_zone = zone
            elif had_output:
                # We produced output — cascade to next zone
                next_zone = self._downstream.get(zone)
                if next_zone:
                    action = 'spawn_downstream'
                    target_zone = next_zone

        self.logger.info(
            f"StageChainListener: {zone}.{table_name} terminated"
            + (f" → {action} {target_zone}.{table_name}" if action else "")
        )

        if action == 'retrigger':
            self._try_spawn_downstream_table(target_zone, table_name)
        elif action == 'spawn_downstream':
            self._try_spawn_downstream_table(target_zone, table_name)

    def _handle_upstream_termination(
        self, query_name: str, zone: str, table_name: str,
    ):
        """Handle termination of an upstream (root) query.

        For availableNow upstream queries: if the table produced output (or
        this is the first-batch cascade), trigger the downstream table.
        processingTime queries normally don't terminate, but if they do
        (e.g. error / manual stop) we don't trigger downstream.
        """
        key = (zone, table_name)

        with self._lock:
            trigger_mode = self._query_trigger.get(query_name)
            had_output = key in self._table_had_output
            self._table_had_output.discard(key)
            needs_initial_cascade = key not in self._initial_cascade_done

        downstream_zone = self._downstream.get(zone)
        if downstream_zone is None:
            if needs_initial_cascade:
                with self._lock:
                    self._initial_cascade_done.add(key)
            return

        should_trigger = False
        if needs_initial_cascade:
            with self._lock:
                self._initial_cascade_done.add(key)
            self.logger.info(
                f"StageChainListener: first-batch cascade "
                f"{zone}.{table_name} → {downstream_zone}.{table_name}"
            )
            should_trigger = True
        elif trigger_mode == 'availableNow' and had_output:
            should_trigger = True

        if should_trigger:
            self._try_spawn_downstream_table(downstream_zone, table_name)

    # ── Spawn with dedup ────────────────────────────────────────────

    def _try_spawn_downstream_table(self, zone: str, table_name: str):
        """Spawn a single downstream table with dedup protection."""
        key = (zone, table_name)
        with self._lock:
            if key in self._active_downstream_tables:
                self._pending_retrigger_tables.add(key)
                self.logger.info(
                    f"StageChainListener: {zone}.{table_name} already active, "
                    f"marking pending retrigger"
                )
                return
            self._active_downstream_tables.add(key)

        self.logger.info(
            f"StageChainListener: spawning {zone}.{table_name} as availableNow"
        )
        try:
            query = self._spawn_table(zone, table_name)
            if query:
                self.register_query(query, zone, 'availableNow', table_name)
            else:
                # spawn returned None (table not enabled, etc.)
                with self._lock:
                    self._active_downstream_tables.discard(key)
        except Exception as e:
            self.logger.error(
                f"StageChainListener: failed to spawn "
                f"{zone}.{table_name}: {e}"
            )
            with self._lock:
                self._active_downstream_tables.discard(key)
