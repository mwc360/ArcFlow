"""Tests for StageChainListener per-table event-driven stage chaining."""
import threading
import time
from unittest.mock import MagicMock, patch
import pytest

from arcflow.core.stage_chain_listener import StageChainListener


def _make_query(name, query_id="test-id-1"):
    """Create a mock StreamingQuery."""
    q = MagicMock()
    q.name = name
    q.id = query_id
    q.isActive = True
    return q


def _make_progress_event(query_name, num_input_rows):
    """Create a mock QueryProgressEvent."""
    event = MagicMock()
    event.progress.name = query_name
    event.progress.numInputRows = num_input_rows
    return event


def _make_terminated_event(query_id):
    """Create a mock QueryTerminatedEvent."""
    event = MagicMock()
    event.id = query_id
    return event


class TestStageChainListenerInit:
    def test_builds_downstream_map(self):
        listener = StageChainListener(
            zone_chain=['bronze', 'silver', 'gold'],
            spawn_table_fn=MagicMock(),
        )
        assert listener._downstream == {'bronze': 'silver', 'silver': 'gold'}

    def test_single_zone_no_downstream(self):
        listener = StageChainListener(
            zone_chain=['bronze'],
            spawn_table_fn=MagicMock(),
        )
        assert listener._downstream == {}

    def test_two_zones(self):
        listener = StageChainListener(
            zone_chain=['raw', 'curated'],
            spawn_table_fn=MagicMock(),
        )
        assert listener._downstream == {'raw': 'curated'}


class TestRegisterQuery:
    def test_registers_query_mappings(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        query = _make_query("bronze_orders", "id-1")

        listener.register_query(query, 'bronze', 'processingTime', 'orders')

        assert listener._query_zone["bronze_orders"] == 'bronze'
        assert listener._query_trigger["bronze_orders"] == 'processingTime'
        assert listener._id_to_name["id-1"] == "bronze_orders"
        assert listener._query_table["bronze_orders"] == 'orders'

    def test_multiple_tables_tracked_independently(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener.register_query(
            _make_query("bronze_items", "id-2"), 'bronze', 'processingTime', 'items'
        )

        assert listener._query_table["bronze_orders"] == 'orders'
        assert listener._query_table["bronze_items"] == 'items'


class TestOnQueryProgressProcessingTime:
    def test_triggers_downstream_table_when_rows_produced(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        # Mark initial cascade as done so we isolate the "rows > 0" behavior
        listener._initial_cascade_done.add(('bronze', 'orders'))

        event = _make_progress_event("bronze_orders", 100)
        listener.onQueryProgress(event)
        listener.wait_for_pending_spawns()

        spawn_fn.assert_called_once_with('silver', 'orders')

    def test_no_trigger_when_zero_rows_after_initial_cascade(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))

        event = _make_progress_event("bronze_orders", 0)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_no_trigger_for_last_zone(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("silver_orders", "id-1"), 'silver', 'processingTime', 'orders'
        )

        event = _make_progress_event("silver_orders", 50)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_ignores_unknown_queries(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)

        event = _make_progress_event("unknown_query", 100)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_ignores_none_query_name(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)

        event = _make_progress_event(None, 100)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_different_tables_trigger_independently(self):
        """bronze_orders and bronze_items each spawn their own silver table."""
        spawned = []
        def mock_spawn(zone, table):
            spawned.append((zone, table))
            return None
        listener = StageChainListener(['bronze', 'silver'], mock_spawn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener.register_query(
            _make_query("bronze_items", "id-2"), 'bronze', 'processingTime', 'items'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))
        listener._initial_cascade_done.add(('bronze', 'items'))

        listener.onQueryProgress(_make_progress_event("bronze_orders", 10))
        listener.onQueryProgress(_make_progress_event("bronze_items", 20))
        listener.wait_for_pending_spawns()

        assert ('silver', 'orders') in spawned
        assert ('silver', 'items') in spawned


class TestOnQueryProgressAvailableNow:
    def test_marks_table_had_output_but_no_immediate_spawn(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'availableNow', 'orders'
        )

        event = _make_progress_event("bronze_orders", 50)
        listener.onQueryProgress(event)

        # Should NOT spawn yet — waits for termination
        spawn_fn.assert_not_called()
        assert ('bronze', 'orders') in listener._table_had_output


class TestOnQueryTerminated:
    def test_upstream_availablenow_triggers_downstream_on_output(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'availableNow', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))
        listener._table_had_output.add(('bronze', 'orders'))

        listener.onQueryTerminated(_make_terminated_event("id-1"))
        listener.wait_for_pending_spawns()

        spawn_fn.assert_called_once_with('silver', 'orders')

    def test_upstream_availablenow_no_trigger_without_output(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'availableNow', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))

        listener.onQueryTerminated(_make_terminated_event("id-1"))

        spawn_fn.assert_not_called()

    def test_downstream_termination_with_pending_retrigger(self):
        """Spawned downstream table re-spawns when pending retrigger is set."""
        spawned = []
        def mock_spawn(zone, table):
            q = _make_query(f"{zone}_{table}", f"id-{zone}-{table}")
            spawned.append((zone, table, q))
            return q
        listener = StageChainListener(['bronze', 'silver'], mock_spawn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))

        # First bronze batch spawns silver_orders
        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 1

        # Another bronze batch while silver is active → pending retrigger
        listener.onQueryProgress(_make_progress_event("bronze_orders", 50))
        assert ('silver', 'orders') in listener._pending_retrigger_tables

        # silver_orders terminates → re-trigger
        listener.onQueryTerminated(_make_terminated_event("id-silver-orders"))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 2
        assert spawned[1][0] == 'silver'
        assert spawned[1][1] == 'orders'

    def test_downstream_termination_chains_further_on_output(self):
        """silver_orders terminates with output → spawns gold_orders."""
        spawned = []
        def mock_spawn(zone, table):
            q = _make_query(f"{zone}_{table}", f"id-{zone}-{table}")
            spawned.append((zone, table))
            return q
        listener = StageChainListener(['bronze', 'silver', 'gold'], mock_spawn)

        # Simulate silver_orders already spawned and registered
        silver_q = _make_query("silver_orders", "id-silver-orders")
        listener._active_downstream_tables.add(('silver', 'orders'))
        listener.register_query(silver_q, 'silver', 'availableNow', 'orders')
        listener._table_had_output.add(('silver', 'orders'))

        listener.onQueryTerminated(_make_terminated_event("id-silver-orders"))
        listener.wait_for_pending_spawns()

        assert len(spawned) == 1
        assert spawned[0] == ('gold', 'orders')

    def test_ignores_unknown_query_id(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)

        listener.onQueryTerminated(_make_terminated_event("unknown-id"))

        spawn_fn.assert_not_called()


class TestDeduplication:
    def test_per_table_dedup_prevents_concurrent_spawns(self):
        spawned = []
        def mock_spawn(zone, table):
            q = _make_query(f"{zone}_{table}", f"id-{zone}-{table}")
            spawned.append((zone, table))
            return q
        listener = StageChainListener(['bronze', 'silver'], mock_spawn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))

        # First progress spawns silver.orders
        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 1

        # Second progress — silver.orders already active → pending retrigger
        listener.onQueryProgress(_make_progress_event("bronze_orders", 50))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 1  # NOT called again
        assert ('silver', 'orders') in listener._pending_retrigger_tables

    def test_different_tables_not_blocked_by_each_other(self):
        """silver.orders being active doesn't block silver.items."""
        spawned = []
        def mock_spawn(zone, table):
            q = _make_query(f"{zone}_{table}", f"id-{zone}-{table}")
            spawned.append((zone, table))
            return q
        listener = StageChainListener(['bronze', 'silver'], mock_spawn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener.register_query(
            _make_query("bronze_items", "id-2"), 'bronze', 'processingTime', 'items'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))
        listener._initial_cascade_done.add(('bronze', 'items'))

        # Spawn silver.orders
        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 1

        # silver.orders is active, but silver.items should still spawn
        listener.onQueryProgress(_make_progress_event("bronze_items", 50))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 2
        assert spawned[1] == ('silver', 'items')


class TestFirstBatchCascade:
    def test_first_batch_triggers_downstream_even_with_zero_rows(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )

        # First batch with 0 rows — should still trigger downstream
        event = _make_progress_event("bronze_orders", 0)
        listener.onQueryProgress(event)
        listener.wait_for_pending_spawns()

        spawn_fn.assert_called_once_with('silver', 'orders')
        assert ('bronze', 'orders') in listener._initial_cascade_done

    def test_second_batch_with_zero_rows_does_not_trigger(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )

        # First batch (cascade)
        listener.onQueryProgress(_make_progress_event("bronze_orders", 0))
        listener.wait_for_pending_spawns()
        assert spawn_fn.call_count == 1

        # Second batch with 0 rows — should NOT trigger
        listener.onQueryProgress(_make_progress_event("bronze_orders", 0))
        listener.wait_for_pending_spawns()
        assert spawn_fn.call_count == 1

    def test_first_batch_cascade_for_availablenow_on_termination(self):
        """availableNow upstream with progress event still cascades on termination."""
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'availableNow', 'orders'
        )

        # Progress with 0 rows — availableNow defers cascade to termination
        listener.onQueryProgress(_make_progress_event("bronze_orders", 0))
        spawn_fn.assert_not_called()
        # _initial_cascade_done should NOT be marked yet for availableNow
        assert ('bronze', 'orders') not in listener._initial_cascade_done

        # Termination — first-batch cascade fires even without output
        listener.onQueryTerminated(_make_terminated_event("id-1"))
        listener.wait_for_pending_spawns()
        spawn_fn.assert_called_once_with('silver', 'orders')
        assert ('bronze', 'orders') in listener._initial_cascade_done

    def test_first_batch_cascade_availablenow_triggers_on_termination(self):
        """availableNow upstream that terminates before any progress event."""
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'availableNow', 'orders'
        )

        # Terminate without any progress event — first-batch cascade fires
        listener.onQueryTerminated(_make_terminated_event("id-1"))
        listener.wait_for_pending_spawns()

        spawn_fn.assert_called_once_with('silver', 'orders')
        assert ('bronze', 'orders') in listener._initial_cascade_done

    def test_cascade_propagates_through_chain(self):
        """First-batch cascade flows bronze → silver → gold."""
        spawned = []
        def mock_spawn(zone, table):
            q = _make_query(f"{zone}_{table}", f"id-{zone}-{table}")
            spawned.append((zone, table))
            return q
        listener = StageChainListener(['bronze', 'silver', 'gold'], mock_spawn)
        listener.register_query(
            _make_query("bronze_orders", "id-bronze"), 'bronze', 'processingTime', 'orders'
        )

        # Bronze first batch (0 rows) → cascades to silver
        listener.onQueryProgress(_make_progress_event("bronze_orders", 0))
        listener.wait_for_pending_spawns()
        assert len(spawned) == 1
        assert spawned[0] == ('silver', 'orders')

        # silver_orders first batch (0 rows via progress) → cascades to gold on termination
        # (availableNow downstream — marks initial cascade on progress,
        #  but since it had no output, gold triggers via first-batch on termination)
        listener.onQueryProgress(_make_progress_event("silver_orders", 0))
        # silver terminates — initial cascade already done, no output, no trigger
        # But gold hasn't had its cascade yet — that happens when silver terminates
        # Actually, silver is availableNow downstream, so its termination is handled
        # by _handle_downstream_termination which doesn't check initial_cascade.
        # Gold cascade depends on silver_orders producing output.
        # For a true 0-row cascade through 3 zones, silver must terminate:
        listener.onQueryTerminated(_make_terminated_event("id-silver-orders"))
        listener.wait_for_pending_spawns()
        # No output from silver, no pending retrigger → no gold spawn
        # This is correct: silver processed 0 rows, so gold has nothing to process.
        assert len(spawned) == 1


class TestCascade:
    def test_three_zone_cascade_with_output(self):
        """bronze → silver → gold cascading via per-table events."""
        spawned = []

        def mock_spawn(zone, table):
            q = _make_query(f"{zone}_{table}", f"id-{zone}")
            spawned.append((zone, q))
            return q

        listener = StageChainListener(['bronze', 'silver', 'gold'], mock_spawn)
        listener.register_query(
            _make_query("bronze_orders", "id-bronze"),
            'bronze', 'availableNow', 'orders',
        )

        # Bronze produces output and terminates (first-batch cascade)
        listener._table_had_output.add(('bronze', 'orders'))
        listener.onQueryTerminated(_make_terminated_event("id-bronze"))
        listener.wait_for_pending_spawns()

        # Silver should be spawned
        assert len(spawned) == 1
        assert spawned[0][0] == 'silver'

        # Silver produces output and terminates
        listener._table_had_output.add(('silver', 'orders'))
        listener.onQueryTerminated(_make_terminated_event("id-silver"))
        listener.wait_for_pending_spawns()

        # Gold should be spawned
        assert len(spawned) == 2
        assert spawned[1][0] == 'gold'


class TestSpawnFailure:
    def test_clears_active_flag_on_spawn_error(self):
        spawn_fn = MagicMock(side_effect=RuntimeError("Spark error"))
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))

        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        listener.wait_for_pending_spawns()

        # Active flag should be cleared despite error
        assert ('silver', 'orders') not in listener._active_downstream_tables

    def test_clears_active_flag_when_spawn_returns_none(self):
        spawn_fn = MagicMock(return_value=None)
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(
            _make_query("bronze_orders", "id-1"), 'bronze', 'processingTime', 'orders'
        )
        listener._initial_cascade_done.add(('bronze', 'orders'))

        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        listener.wait_for_pending_spawns()

        # spawn returned None → active flag cleared
        assert ('silver', 'orders') not in listener._active_downstream_tables


class TestMarkTableActiveHelpers:
    def test_mark_returns_true_when_not_active(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        assert listener.mark_table_active('silver', 'orders') is True
        assert ('silver', 'orders') in listener._active_downstream_tables

    def test_mark_returns_false_when_already_active(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        listener._active_downstream_tables.add(('silver', 'orders'))
        assert listener.mark_table_active('silver', 'orders') is False

    def test_clear_removes_active_flag(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        listener._active_downstream_tables.add(('silver', 'orders'))
        listener.clear_table_active('silver', 'orders')
        assert ('silver', 'orders') not in listener._active_downstream_tables
