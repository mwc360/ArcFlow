"""Tests for StageChainListener event-driven stage chaining."""
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
            spawn_zone_fn=MagicMock(),
        )
        assert listener._downstream == {'bronze': 'silver', 'silver': 'gold'}

    def test_single_zone_no_downstream(self):
        listener = StageChainListener(
            zone_chain=['bronze'],
            spawn_zone_fn=MagicMock(),
        )
        assert listener._downstream == {}

    def test_two_zones(self):
        listener = StageChainListener(
            zone_chain=['raw', 'curated'],
            spawn_zone_fn=MagicMock(),
        )
        assert listener._downstream == {'raw': 'curated'}


class TestRegisterQuery:
    def test_registers_query_mappings(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        query = _make_query("bronze_orders", "id-1")

        listener.register_query(query, 'bronze', 'processingTime')

        assert listener._query_zone["bronze_orders"] == 'bronze'
        assert listener._query_trigger["bronze_orders"] == 'processingTime'
        assert listener._id_to_name["id-1"] == "bronze_orders"
        assert listener._zone_active_count['bronze'] == 1

    def test_increments_zone_active_count(self):
        listener = StageChainListener(['bronze', 'silver'], MagicMock())
        listener.register_query(_make_query("q1", "id-1"), 'bronze', 'availableNow')
        listener.register_query(_make_query("q2", "id-2"), 'bronze', 'availableNow')

        assert listener._zone_active_count['bronze'] == 2


class TestOnQueryProgressProcessingTime:
    def test_triggers_downstream_when_rows_produced(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'processingTime')

        event = _make_progress_event("bronze_orders", 100)
        listener.onQueryProgress(event)

        spawn_fn.assert_called_once_with('silver')

    def test_no_trigger_when_zero_rows(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'processingTime')

        event = _make_progress_event("bronze_orders", 0)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_no_trigger_for_last_zone(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("silver_orders", "id-1"), 'silver', 'processingTime')

        event = _make_progress_event("silver_orders", 50)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_ignores_unknown_queries(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)

        event = _make_progress_event("unknown_query", 100)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()

    def test_ignores_none_query_name(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)

        event = _make_progress_event(None, 100)
        listener.onQueryProgress(event)

        spawn_fn.assert_not_called()


class TestOnQueryProgressAvailableNow:
    def test_marks_zone_had_output_but_no_immediate_spawn(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'availableNow')

        event = _make_progress_event("bronze_orders", 50)
        listener.onQueryProgress(event)

        # Should NOT spawn yet — waits for termination
        spawn_fn.assert_not_called()
        assert 'bronze' in listener._zone_had_output


class TestOnQueryTerminated:
    def test_triggers_downstream_on_availablenow_termination(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'availableNow')

        # Mark zone had output (normally done by onQueryProgress)
        listener._zone_had_output.add('bronze')

        event = _make_terminated_event("id-1")
        listener.onQueryTerminated(event)

        spawn_fn.assert_called_once_with('silver')

    def test_no_trigger_when_no_output(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'availableNow')

        # Don't mark zone had output
        event = _make_terminated_event("id-1")
        listener.onQueryTerminated(event)

        spawn_fn.assert_not_called()

    def test_waits_for_all_zone_queries_before_trigger(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'availableNow')
        listener.register_query(_make_query("bronze_customers", "id-2"), 'bronze', 'availableNow')
        listener._zone_had_output.add('bronze')

        # First query terminates — should NOT trigger yet
        listener.onQueryTerminated(_make_terminated_event("id-1"))
        spawn_fn.assert_not_called()

        # Second query terminates — NOW trigger
        listener.onQueryTerminated(_make_terminated_event("id-2"))
        spawn_fn.assert_called_once_with('silver')

    def test_ignores_unknown_query_id(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)

        event = _make_terminated_event("unknown-id")
        listener.onQueryTerminated(event)

        spawn_fn.assert_not_called()


class TestDeduplication:
    def test_dedup_prevents_concurrent_spawns(self):
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'processingTime')

        # First progress event spawns silver
        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        assert spawn_fn.call_count == 1

        # Second progress event while silver is active → pending retrigger
        listener.onQueryProgress(_make_progress_event("bronze_orders", 50))
        assert spawn_fn.call_count == 1  # NOT called again
        assert 'silver' in listener._pending_retrigger

    def test_retrigger_on_downstream_completion(self):
        """When a downstream zone completes with a pending retrigger, re-spawn it."""
        spawn_fn = MagicMock(return_value=[])
        listener = StageChainListener(['bronze', 'silver', 'gold'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'processingTime')

        # Spawn silver
        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))
        assert spawn_fn.call_count == 1

        # Register the spawned silver query (normally done by _try_spawn_downstream)
        silver_query = _make_query("silver_orders", "id-silver")
        listener.register_query(silver_query, 'silver', 'availableNow')

        # Another bronze batch while silver is running → pending retrigger
        listener.onQueryProgress(_make_progress_event("bronze_orders", 50))
        assert 'silver' in listener._pending_retrigger

        # Silver completes (had output)
        listener._zone_had_output.add('silver')
        listener.onQueryTerminated(_make_terminated_event("id-silver"))

        # Should retrigger silver (pending), AND also potentially spawn gold
        # The retrigger takes priority
        assert spawn_fn.call_count >= 2


class TestCascade:
    def test_three_zone_cascade(self):
        """bronze → silver → gold cascading via termination events."""
        spawned = []

        def mock_spawn(zone):
            q = _make_query(f"{zone}_orders", f"id-{zone}")
            spawned.append((zone, q))
            return [q]

        listener = StageChainListener(['bronze', 'silver', 'gold'], mock_spawn)
        listener.register_query(_make_query("bronze_orders", "id-bronze"), 'bronze', 'availableNow')

        # Bronze produces output and terminates
        listener._zone_had_output.add('bronze')
        listener.onQueryTerminated(_make_terminated_event("id-bronze"))

        # Silver should be spawned
        assert len(spawned) == 1
        assert spawned[0][0] == 'silver'

        # Silver produces output and terminates
        listener._zone_had_output.add('silver')
        listener.onQueryTerminated(_make_terminated_event("id-silver"))

        # Gold should be spawned
        assert len(spawned) == 2
        assert spawned[1][0] == 'gold'


class TestSpawnFailure:
    def test_clears_active_flag_on_spawn_error(self):
        spawn_fn = MagicMock(side_effect=RuntimeError("Spark error"))
        listener = StageChainListener(['bronze', 'silver'], spawn_fn)
        listener.register_query(_make_query("bronze_orders", "id-1"), 'bronze', 'processingTime')

        listener.onQueryProgress(_make_progress_event("bronze_orders", 100))

        # Active flag should be cleared despite error
        assert 'silver' not in listener._active_downstream
