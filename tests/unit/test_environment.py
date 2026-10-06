"""Kiểm tra môi trường test khớp Fabric Runtime 2.0 — nền cho mọi test khác."""

from delta.tables import DeltaTable


def test_spark_version_matches_fabric_runtime(spark):
    assert spark.version.startswith("4.1")


def test_ansi_mode_on_like_fabric(spark):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"


def test_try_functions_return_null_on_dirty_values(spark):
    row = spark.sql(
        "SELECT try_to_timestamp('7/14/2024 5:41', 'M/d/yyyy H:mm') AS ok_ts, "
        "try_to_timestamp('21/12/2025', 'M/d/yyyy') AS bad_ts, "
        "try_cast('five percent' AS DECIMAL(18,2)) AS bad_num"
    ).first()
    assert row.ok_ts is not None
    assert row.bad_ts is None
    assert row.bad_num is None


def test_delta_merge_roundtrip(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string").write.format("delta").save(path)
    source = spark.createDataFrame([(2, "B"), (3, "c")], "id int, v string")
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(source.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    rows = sorted(tuple(r) for r in spark.read.format("delta").load(path).collect())
    assert rows == [(1, "a"), (2, "B"), (3, "c")]
