"""Command smoke: `bulk scancount --persegment`.

Exercises the --persegment flag against a real Glue environment. Creates a
transient table, seeds it with items distributed across segments, then runs
scancount with --persegment and verifies:

1. The command succeeds (Glue job reaches SUCCEEDED)
2. Per-segment output lines appear in stdout (format: "N: COUNT")
3. The sum of per-segment counts equals the total item count
"""
from __future__ import annotations

import re

import pytest

from tests.e2e.helpers.assertions import assert_glue_succeeded, table_item_count
from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.connector.conftest import PerfRow


# Per-segment output lines: "<segment_number>: <count>" or "Segment <n>: <count>"
_SEGMENT_LINE = re.compile(r"(?:Segment\s+)?(\d+)\s*:\s*([\d,]+)")

# Total line (existing behavior)
_TOTAL_LINE = re.compile(r"Total records counted:\s*([\d,]+)")


def _parse_segment_counts(stdout: str) -> dict[int, int]:
    """Parse per-segment output lines from scancount --persegment stdout."""
    counts = {}
    for match in _SEGMENT_LINE.finditer(stdout):
        seg_id = int(match.group(1))
        count = int(match.group(2).replace(",", ""))
        counts[seg_id] = count
    return counts


def _parse_total(stdout: str) -> int | None:
    match = _TOTAL_LINE.search(stdout)
    if match:
        return int(match.group(1).replace(",", ""))
    return None


@pytest.mark.e2e
class TestScancountPerSegmentSmoke:
    """Real-AWS smoke test for scancount --persegment (issue #92)."""

    def test_persegment_outputs_per_segment_counts(self, e2e_config, cmd_perf_collector):
        """Run scancount --persegment on a seeded table; verify per-segment output."""
        with transient_table(e2e_config.aws_region, label="scancount-ps") as table:
            # Seed the table with enough items to span multiple segments
            seed = run_command(
                "fill",
                table=table,
                extra_args=["--numitems", "200", "--generator", "default"],
            )
            assert_glue_succeeded("scancount-persegment setup (fill)", seed, e2e_config.aws_region)

            # Verify seed worked
            seeded_count = table_item_count(e2e_config.aws_region, table)
            assert seeded_count > 0, "Table must have items for scancount test"

            # Run scancount with --persegment flag
            result = run_command(
                "scancount",
                table=table,
                extra_args=["--persegment", "--segments", "5"],
            )
            perf = assert_glue_succeeded("scancount --persegment", result, e2e_config.aws_region)

            # Parse per-segment output
            segment_counts = _parse_segment_counts(result.stdout)
            assert len(segment_counts) > 0, (
                f"--persegment must produce per-segment output lines. "
                f"Got stdout:\n{result.stdout[-2000:]}"
            )

            # Verify each segment has a non-negative count
            for seg_id, count in segment_counts.items():
                assert count >= 0, f"Segment {seg_id} has negative count: {count}"

            # Sum of per-segment counts should equal total
            segment_sum = sum(segment_counts.values())
            total = _parse_total(result.stdout)
            if total is not None:
                assert segment_sum == total, (
                    f"Sum of per-segment counts ({segment_sum}) must equal "
                    f"reported total ({total})"
                )

            # Segment sum should match the actual table item count
            assert segment_sum == seeded_count, (
                f"Sum of per-segment counts ({segment_sum}) must equal "
                f"actual table item count ({seeded_count})"
            )

            cmd_perf_collector.add(PerfRow(
                command="scancount --persegment",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=segment_sum,
            ))

    def test_persegment_without_flag_shows_only_total(self, e2e_config, cmd_perf_collector):
        """Without --persegment, scancount still shows only the total (backward compat)."""
        with transient_table(e2e_config.aws_region, label="scancount-nops") as table:
            seed = run_command(
                "fill",
                table=table,
                extra_args=["--numitems", "50", "--generator", "default"],
            )
            assert_glue_succeeded("scancount-nopersegment setup (fill)", seed, e2e_config.aws_region)

            result = run_command("scancount", table=table)
            assert_glue_succeeded("scancount (no --persegment)", result, e2e_config.aws_region)

            total = _parse_total(result.stdout)
            assert total is not None, (
                f"scancount without --persegment must still print total. "
                f"Got:\n{result.stdout[-1000:]}"
            )

            # Should NOT have per-segment breakdown lines
            segment_counts = _parse_segment_counts(result.stdout)
            # Filter out the total line which might match the pattern
            # A real per-segment output would have multiple numbered lines
            assert len(segment_counts) <= 1, (
                f"scancount without --persegment should NOT show per-segment "
                f"breakdown (got {len(segment_counts)} segment lines)"
            )
