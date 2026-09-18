"""Shared partitions definition for the ingestion assets.

Both ``waga_weather_ingestion`` and ``waga_generation_ingestion`` must use
the *same* :class:`~dagster.DailyPartitionsDefinition` instance — Dagster
schedules built via ``build_schedule_from_partitioned_job`` require every
partitioned asset in the job to share one partitions definition.

The daily-run home-server schedule fires at 06:15 **America/Phoenix**
(server timezone; Phoenix is UTC-7 year-round, no DST), so the partitions
definition is timezone-aware in Phoenix as well: the partition that
``build_schedule_from_partitioned_job`` requests for "yesterday" always
lines up with the schedule's own tick.

The ingestion assets themselves read ``context.partition_key`` as a plain
date string (``YYYY-MM-DD``), so this timezone change does not shift what
data a given partition covers — it only changes which partition the
06:15-Phoenix schedule tick resolves to "yesterday".
"""

from __future__ import annotations

from dagster import DailyPartitionsDefinition

INGESTION_PARTITIONS = DailyPartitionsDefinition(
    start_date="2023-01-01",
    timezone="America/Phoenix",
)
