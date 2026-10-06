"""Log vận hành (P10): mỗi task ghi sự kiện RUNNING khi bắt đầu và SUCCESS/FAILED khi kết thúc.

Bảng log là APPEND-ONLY (không MERGE/UPDATE): nhiều notebook chạy song song (runMultiple) cùng ghi
mà không xung đột Delta. Trạng thái hiện tại = sự kiện mới nhất theo task_id (latest_task_status).
Task còn RUNNING quá lâu (không có sự kiện kết thúc) = notebook chết giữa chừng → alert.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from companya_de.env import RunContext

TASK_LOG_TABLE = "meta.log_task_run"
PIPELINE_LOG_TABLE = "meta.log_pipeline_run"

METRIC_FIELDS = (
    "rows_read", "rows_inserted", "rows_updated", "rows_deleted", "rows_rejected", "rows_deduped",
    "watermark_from", "watermark_to", "delta_version",
)
_STRING_METRICS = {"watermark_from", "watermark_to"}


@dataclass(frozen=True)
class Task:
    task_id: str
    run_id: str
    load_date: dt.date
    layer: str
    step: str
    source_system: str | None
    entity: str | None
    target_table: str | None
    started_at: dt.datetime


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _append(spark: SparkSession, table: str, row: dict) -> None:
    schema = spark.table(table).schema
    values = [row.get(f.name) for f in schema.fields]
    spark.createDataFrame([values], schema=schema).write.format("delta").mode("append").saveAsTable(table)


def _error_text(error: BaseException | str | None) -> str | None:
    if error is None:
        return None
    text = f"{type(error).__name__}: {error}" if isinstance(error, BaseException) else str(error)
    return text[:4000]


def _clean_metrics(metrics: dict | None) -> dict:
    metrics = dict(metrics or {})
    unknown = set(metrics) - set(METRIC_FIELDS)
    if unknown:
        raise ValueError(f"Unknown metric(s) {sorted(unknown)}; allowed: {METRIC_FIELDS}")
    return {k: (None if v is None else str(v) if k in _STRING_METRICS else int(v)) for k, v in metrics.items()}


def task_start(
    spark: SparkSession,
    ctx: RunContext,
    *,
    layer: str,
    step: str,
    source_system: str | None = None,
    entity: str | None = None,
    target_table: str | None = None,
) -> Task:
    task = Task(
        task_id=uuid.uuid4().hex, run_id=ctx.run_id, load_date=ctx.load_date, layer=layer, step=step,
        source_system=source_system, entity=entity, target_table=target_table, started_at=_now(),
    )
    _append(spark, TASK_LOG_TABLE, {**task.__dict__, "status": "RUNNING", "event_at": task.started_at})
    return task


def task_end(
    spark: SparkSession,
    task: Task,
    status: str,
    *,
    metrics: dict | None = None,
    error: BaseException | str | None = None,
) -> None:
    if status not in {"SUCCESS", "FAILED", "SKIPPED"}:
        raise ValueError(f"Invalid final status '{status}'")
    ended_at = _now()
    row = {
        **task.__dict__,
        **_clean_metrics(metrics),
        "status": status,
        "error_message": _error_text(error),
        "ended_at": ended_at,
        "event_at": ended_at,
    }
    _append(spark, TASK_LOG_TABLE, row)


@contextmanager
def tracked_task(spark: SparkSession, ctx: RunContext, **task_fields) -> Iterator[dict]:
    """Bọc phần thân notebook: luôn ghi kết thúc (P8). Thân điền số liệu vào dict được yield.

        with tracked_task(spark, ctx, layer="slv", step="slv_orders") as metrics:
            result = merge_delta(...)
            metrics.update(result.as_metrics(), rows_read=n)
    """
    task = task_start(spark, ctx, **task_fields)
    metrics: dict = {}
    try:
        yield metrics
    except BaseException as exc:
        task_end(spark, task, "FAILED", metrics=metrics, error=exc)
        raise
    task_end(spark, task, "SUCCESS", metrics=metrics)


def latest_task_status(spark: SparkSession, run_id: str | None = None) -> DataFrame:
    """Một dòng / task_id = sự kiện mới nhất."""
    df = spark.table(TASK_LOG_TABLE)
    if run_id:
        df = df.filter(F.col("run_id") == run_id)
    w = Window.partitionBy("task_id").orderBy(F.col("event_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def pipeline_event(spark: SparkSession, ctx: RunContext, pipeline: str, status: str, message: str | None = None):
    _append(spark, PIPELINE_LOG_TABLE, {
        "run_id": ctx.run_id, "pipeline": pipeline, "load_date": ctx.load_date,
        "status": status, "message": message, "event_at": _now(),
    })
