import datetime as dt

import pytest

from companya_de.io import DuplicateKeyError, merge_delta, sql_literal, write_batch

SCHEMA = "id int, part string, v string, _batch_id string"


def _rows(spark, table):
    return sorted(tuple(r) for r in spark.table(table).collect())


# --- write_batch -----------------------------------------------------------


def test_write_batch_rerun_does_not_duplicate(spark, table_name):
    df = spark.createDataFrame([(1, "a", "x", "b1"), (2, "a", "y", "b1")], SCHEMA)
    write_batch(df, table_name)
    write_batch(df, table_name)
    assert spark.table(table_name).count() == 2


def test_write_batch_only_replaces_its_own_batch(spark, table_name):
    write_batch(spark.createDataFrame([(1, "a", "x", "b1"), (2, "a", "y", "b1")], SCHEMA), table_name)
    write_batch(spark.createDataFrame([(3, "a", "z", "b2")], SCHEMA), table_name)
    # chạy lại b1 với nội dung khác → chỉ b1 bị thay, b2 giữ nguyên
    write_batch(spark.createDataFrame([(9, "a", "new", "b1")], SCHEMA), table_name)
    assert _rows(spark, table_name) == [(3, "a", "z", "b2"), (9, "a", "new", "b1")]


def test_write_batch_reports_rows_written(spark, table_name):
    result = write_batch(spark.createDataFrame([(1, "a", "x", "b1"), (2, "a", "y", "b1")], SCHEMA), table_name)
    assert result.rows_inserted == 2


def test_write_batch_empty_is_noop(spark, table_name):
    assert write_batch(spark.createDataFrame([], SCHEMA), table_name) is None


# --- merge_delta -----------------------------------------------------------


def test_merge_is_idempotent(spark, table_name):
    df = spark.createDataFrame([(1, "a", "x", "b1"), (2, "b", "y", "b1")], SCHEMA)
    merge_delta(df, table_name, keys=["id"])
    merge_delta(df, table_name, keys=["id"])
    assert _rows(spark, table_name) == [(1, "a", "x", "b1"), (2, "b", "y", "b1")]


def test_merge_upserts_and_reports_metrics(spark, table_name):
    merge_delta(spark.createDataFrame([(1, "a", "x", "b1")], SCHEMA), table_name, keys=["id"])
    result = merge_delta(
        spark.createDataFrame([(1, "a", "X", "b2"), (2, "a", "y", "b2")], SCHEMA), table_name, keys=["id"]
    )
    assert (result.operation, result.rows_inserted, result.rows_updated) == ("MERGE", 1, 1)
    assert result.as_metrics()["delta_version"] == result.version


def test_merge_update_condition_skips_unchanged(spark, table_name):
    merge_delta(spark.createDataFrame([(1, "a", "x", "b1"), (2, "a", "y", "b1")], SCHEMA), table_name, keys=["id"])
    result = merge_delta(
        spark.createDataFrame([(1, "a", "x", "b2"), (2, "a", "CHANGED", "b2")], SCHEMA),
        table_name, keys=["id"], update_condition="s.v <> t.v",
    )
    assert result.rows_updated == 1


def test_merge_rejects_duplicate_source_keys(spark, table_name):
    df = spark.createDataFrame([(1, "a", "x", "b1"), (1, "a", "y", "b1")], SCHEMA)
    with pytest.raises(DuplicateKeyError):
        merge_delta(df, table_name, keys=["id"])


def test_merge_delete_not_matched_respects_prune_scope(spark, table_name):
    initial = spark.createDataFrame(
        [(1, "a", "x", "b1"), (2, "a", "y", "b1"), (3, "b", "z", "b1")], SCHEMA
    )
    merge_delta(initial, table_name, keys=["id"], partition_by=["part"])

    # nguồn chỉ chứa partition 'a' và đã bỏ id=2 → xoá id=2, KHÔNG được xoá id=3 (partition 'b')
    merge_delta(
        spark.createDataFrame([(1, "a", "x", "b2")], SCHEMA), table_name, keys=["id"],
        prune_column="part", delete_not_matched_by_source=True,
    )
    assert [r[0] for r in _rows(spark, table_name)] == [1, 3]


def test_merge_with_empty_pruned_source_is_noop(spark, table_name):
    merge_delta(spark.createDataFrame([(1, "a", "x", "b1")], SCHEMA), table_name, keys=["id"])
    merge_delta(spark.createDataFrame([], SCHEMA), table_name, keys=["id"], prune_column="part",
                delete_not_matched_by_source=True)
    assert spark.table(table_name).count() == 1


# --- sql_literal ------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (5, "5"),
        ("it's", "'it\\'s'"),
        (dt.date(2026, 1, 2), "DATE'2026-01-02'"),
        (dt.datetime(2026, 1, 2, 3, 4, 5), "TIMESTAMP'2026-01-02 03:04:05'"),
    ],
)
def test_sql_literal(value, expected):
    assert sql_literal(value) == expected


def test_sql_literal_roundtrip_in_spark(spark):
    assert spark.sql(f"SELECT {sql_literal(chr(39) + 'x')} AS v").first().v == "'x"
