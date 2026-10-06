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
