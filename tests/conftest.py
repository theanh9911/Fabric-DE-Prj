import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark(warehouse_dir: str) -> SparkSession:
    """Spark local cấu hình gần Fabric Runtime 2.0: Delta, ANSI (mặc định Spark 4), UTC."""
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("companya-de-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = build_spark(str(tmp_path_factory.mktemp("warehouse")))
    yield session
    session.stop()


@pytest.fixture(scope="session")
def migrated(spark):
    """Catalog local đã áp dụng toàn bộ migration — giống lh_platform sau nb_setup_migrate."""
    from companya_de.migrate import apply_migrations

    apply_migrations(spark)
    return spark


@pytest.fixture
def ctx():
    from companya_de.config import PlatformSettings
    from companya_de.env import new_run_context

    return new_run_context("2026-01-02", "testrun", settings=PlatformSettings("lh_platform", "Asia/Ho_Chi_Minh"))


@pytest.fixture
def table_name(spark):
    """Tên bảng duy nhất cho mỗi test trong schema `test`."""
    import uuid

    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    return f"test.t_{uuid.uuid4().hex[:8]}"
