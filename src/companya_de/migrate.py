"""Áp dụng migration SQL đóng gói trong package (P11) — kiểu Flyway.

- File: companya_de/migrations/V###__<name>.sql, áp dụng theo thứ tự version
- Lịch sử: meta.schema_migrations (version, name, checksum, applied_at)
- File đã áp dụng mà nội dung đổi → lỗi (migration bất biến; muốn đổi thì thêm version mới)
- Câu lệnh tách theo ';'. Comment chỉ dùng dòng riêng bắt đầu bằng '--'.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib.resources
import re
from dataclasses import dataclass

from pyspark.sql import SparkSession

HISTORY_TABLE = "meta.schema_migrations"
_FILE_NAME = re.compile(r"^V(\d{3})__([a-z0-9_]+)\.sql$")


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()

    def statements(self) -> list[str]:
        code = "\n".join(line for line in self.sql.splitlines() if not line.strip().startswith("--"))
        return [s.strip() for s in code.split(";") if s.strip()]


def discover(package: str = "companya_de.migrations") -> list[Migration]:
    migrations = []
    for entry in importlib.resources.files(package).iterdir():
        match = _FILE_NAME.match(entry.name)
        if match:
            migrations.append(Migration(int(match.group(1)), match.group(2), entry.read_text(encoding="utf-8")))
        elif entry.name.endswith(".sql"):
            raise MigrationError(f"Bad migration file name '{entry.name}' (expected V###__name.sql)")
    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError(f"Duplicated migration versions: {sorted(versions)}")
    return sorted(migrations, key=lambda m: m.version)


def _ensure_history(spark: SparkSession) -> None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS meta")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} ("
        "version INT NOT NULL, name STRING NOT NULL, checksum STRING NOT NULL, applied_at TIMESTAMP NOT NULL"
        ") USING DELTA COMMENT 'Lịch sử migration'"
    )


def applied_migrations(spark: SparkSession) -> dict[int, str]:
    _ensure_history(spark)
    return {r.version: r.checksum for r in spark.table(HISTORY_TABLE).select("version", "checksum").collect()}


def apply_migrations(spark: SparkSession, migrations: list[Migration] | None = None) -> list[int]:
    """Áp dụng các migration chưa có. Trả về danh sách version vừa áp dụng."""
    migrations = discover() if migrations is None else migrations
    applied = applied_migrations(spark)

    for m in migrations:
        if m.version in applied and applied[m.version] != m.checksum:
            raise MigrationError(f"V{m.version:03d}__{m.name} was modified after being applied — add a new version")

    newly_applied = []
    for m in migrations:
        if m.version in applied:
            continue
        for statement in m.statements():
            spark.sql(statement)
        spark.createDataFrame(
            [(m.version, m.name, m.checksum, dt.datetime.now(dt.UTC))],
            schema=spark.table(HISTORY_TABLE).schema,
        ).write.format("delta").mode("append").saveAsTable(HISTORY_TABLE)
        newly_applied.append(m.version)
    return newly_applied
