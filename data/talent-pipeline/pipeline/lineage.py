"""Lineage records: what each step read, wrote and rejected, per dataset.

Every layer returns a list of ``DatasetMetric`` objects. ``job.py`` collects
them for the run summary (and, later, persists them as a run manifest).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetMetric:
    """Row counts for one dataset moving through one layer."""

    layer: str          # bronze | silver | gold
    dataset: str        # e.g. employees
    source: str         # where it was read from
    target: str         # where it was written to
    rows_in: int
    rows_out: int
    rows_rejected: int = 0  # quarantined (Silver) - always 0 in Bronze

    @property
    def rows_dropped(self) -> int:
        """Rows neither written nor quarantined (e.g. removed duplicates)."""
        return self.rows_in - self.rows_out - self.rows_rejected

    def as_dict(self) -> dict[str, Any]:
        """Plain dict, ready for JSON serialisation."""
        return {**asdict(self), "rows_dropped": self.rows_dropped}
