import pytest
from pyspark.sql import functions as F

from companya_de.runlog import TASK_LOG_TABLE, latest_task_status, pipeline_event, task_end, task_start, tracked_task


def _events(spark, task_id):
    return spark.table(TASK_LOG_TABLE).filter(F.col("task_id") == task_id).orderBy("event_at").collect()


def test_tracked_task_success_logs_start_and_end(migrated, ctx):
    spark = migrated
    with tracked_task(spark, ctx, layer="slv", step="test_success", entity="orders") as metrics:
        metrics.update(rows_read=10, rows_inserted=7, watermark_to="2026-01-02")

    task_id = spark.table(TASK_LOG_TABLE).filter("step = 'test_success'").first().task_id
    start, end = _events(spark, task_id)
    assert (start.status, end.status) == ("RUNNING", "SUCCESS")
    assert (end.rows_read, end.rows_inserted, end.watermark_to) == (10, 7, "2026-01-02")
    assert end.ended_at >= start.started_at


def test_tracked_task_failure_logs_and_reraises(migrated, ctx):
    spark = migrated
    with pytest.raises(ZeroDivisionError), tracked_task(spark, ctx, layer="slv", step="test_failure"):
        _ = 1 / 0

    end = latest_task_status(spark, ctx.run_id).filter("step = 'test_failure'").first()
    assert end.status == "FAILED"
    assert end.error_message.startswith("ZeroDivisionError")


def test_unknown_metric_is_rejected(migrated, ctx):
    task = task_start(migrated, ctx, layer="slv", step="test_metric")
    with pytest.raises(ValueError, match="Unknown metric"):
        task_end(migrated, task, "SUCCESS", metrics={"rows_redd": 1})


def test_latest_task_status_one_row_per_task(migrated, ctx):
    spark = migrated
    for step in ("test_latest_a", "test_latest_b"):
        with tracked_task(spark, ctx, layer="gld", step=step):
            pass
    latest = latest_task_status(spark, ctx.run_id).filter(F.col("step").startswith("test_latest"))
    assert latest.count() == 2
    assert {r.status for r in latest.collect()} == {"SUCCESS"}


def test_pipeline_event(migrated, ctx):
    pipeline_event(migrated, ctx, "pl_test", "STARTED")
    row = migrated.table("meta.log_pipeline_run").filter("pipeline = 'pl_test'").first()
    assert (row.run_id, row.load_date.isoformat()) == (ctx.run_id, "2026-01-02")
