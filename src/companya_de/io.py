"""Đọc/ghi bảng Delta theo tên `schema.table` — mọi cách ghi đều idempotent (P6).

- write_batch:  ghi đè đúng các batch có trong DataFrame (replaceWhere theo _batch_id) → Bronze
- merge_delta:  MERGE theo business key, tuỳ chọn giới hạn partition (pruning) và xoá dòng không còn ở nguồn
- last_write_result: số dòng insert/update/delete + version lấy từ Delta history (không đếm lại)
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


class DuplicateKeyError(ValueError):
    """Nguồn có nhiều dòng cùng key → MERGE không xác định; phải dedup trước."""


@dataclass(frozen=True)
class WriteResult:
    operation: str
    version: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int

    def as_metrics(self) -> dict:
        """Dạng dùng thẳng cho runlog task metrics."""
        return {
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "delta_version": self.version,
        }


def table_exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table)


def last_write_result(spark: SparkSession, table: str) -> WriteResult:
    row = DeltaTable.forName(spark, table).history(1).select("version", "operation", "operationMetrics").first()
    metrics = row.operationMetrics or {}

    def metric(*names: str) -> int:
        return sum(int(metrics.get(n, 0)) for n in names)

    if row.operation == "MERGE":
        return WriteResult(
            row.operation, row.version,
            rows_inserted=metric("numTargetRowsInserted"),
            rows_updated=metric("numTargetRowsUpdated"),
            rows_deleted=metric("numTargetRowsDeleted"),
        )
    return WriteResult(
        row.operation, row.version,
        rows_inserted=metric("numOutputRows"),
        rows_updated=0,
        rows_deleted=metric("numDeletedRows"),
    )


def sql_literal(value) -> str:
    """Giá trị Python → literal Spark SQL (dùng cho điều kiện replaceWhere / pruning)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, dt.datetime):
        return f"TIMESTAMP'{value.isoformat(sep=' ')}'"
    if isinstance(value, dt.date):
        return f"DATE'{value.isoformat()}'"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _in_predicate(column: str, values: list, alias: str | None = None) -> str:
    ref = f"{alias}.`{column}`" if alias else f"`{column}`"
    return f"{ref} IN ({', '.join(sql_literal(v) for v in values)})"


def _distinct_values(df: DataFrame, column: str) -> list:
    return sorted(r[0] for r in df.select(column).distinct().collect())


def assert_unique(df: DataFrame, keys: list[str]) -> None:
    dup = df.groupBy(*keys).count().filter("count > 1").limit(1).collect()
    if dup:
        raise DuplicateKeyError(f"Duplicate source keys {keys}: e.g. {dup[0].asDict()}")


def write_batch(
    df: DataFrame,
    table: str,
    *,
    batch_column: str = "_batch_id",
    partition_by: list[str] | None = None,
    merge_schema: bool = False,
) -> WriteResult | None:
    """Ghi đè đúng các batch trong df; batch khác giữ nguyên. Chạy lại cùng batch → không nhân đôi."""
    spark = df.sparkSession
    batch_ids = _distinct_values(df, batch_column)
    if not batch_ids:
        return None

    writer = df.write.format("delta").option("mergeSchema", str(merge_schema).lower())
    if not table_exists(spark, table):
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.mode("append").saveAsTable(table)
    else:
        writer.mode("overwrite").option("replaceWhere", _in_predicate(batch_column, batch_ids)).saveAsTable(table)
    return last_write_result(spark, table)


def merge_delta(
    df: DataFrame,
    table: str,
    keys: list[str],
    *,
    prune_column: str | None = None,
    update_condition: str | None = None,
    delete_not_matched_by_source: bool = False,
    partition_by: list[str] | None = None,
    check_unique: bool = True,
) -> WriteResult:
    """MERGE df vào table theo keys.

    prune_column:   chỉ chạm các partition (giá trị) có trong df → không quét/ghi lại cả bảng (P12)
    update_condition: vd "s._record_hash <> t._record_hash" → chỉ update khi thật sự đổi
    delete_not_matched_by_source: xoá dòng của target không còn trong df (trong phạm vi prune nếu có)
    """
    spark = df.sparkSession
    if check_unique:
        assert_unique(df, keys)

    if not table_exists(spark, table):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.mode("append").saveAsTable(table)
        return last_write_result(spark, table)

    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    scope = None
    if prune_column:
        values = _distinct_values(df, prune_column)
        if not values:  # nguồn rỗng → không partition nào cần chạm
            return last_write_result(spark, table)
        scope = _in_predicate(prune_column, values, alias="t")
        condition = f"{condition} AND {scope}"

    builder = (
        DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
    )
    if delete_not_matched_by_source:
        # Có prune → chỉ xoá trong các partition đang xử lý, KHÔNG xoá partition khác
        builder = builder.whenNotMatchedBySourceDelete(condition=scope)
    builder.execute()
    return last_write_result(spark, table)
