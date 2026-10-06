"""Config as code: đọc, kiểm tra và đồng bộ YAML trong package vào bảng meta.cfg_*."""

from __future__ import annotations

import datetime as dt
import importlib.resources
import re
from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

SOURCE_TYPES = {"db", "file"}
LOAD_TYPES = {"incremental", "full_snapshot"}
WATERMARK_TYPES = {"column", "file_modified"}

CFG_SOURCE_ENTITY_TABLE = "meta.cfg_source_entity"

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(ValueError):
    """Config không hợp lệ — liệt kê toàn bộ lỗi một lần."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("Invalid config:\n  - " + "\n  - ".join(errors))


def _read_yaml(file_name: str) -> dict:
    text = importlib.resources.files(__package__).joinpath(file_name).read_text(encoding="utf-8")
    return yaml.safe_load(text) or {}


# ---------------------------------------------------------------------------
# Platform settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlatformSettings:
    lakehouse: str
    business_timezone: str


def load_settings() -> PlatformSettings:
    doc = _read_yaml("platform.yml")
    settings = PlatformSettings(lakehouse=doc["lakehouse"], business_timezone=doc["business_timezone"])
    errors = _check_timezone(settings.business_timezone, "platform.business_timezone")
    if errors:
        raise ConfigError(errors)
    return settings


# ---------------------------------------------------------------------------
# Source entities
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceEntity:
    source_system: str
    entity: str
    source_type: str
    source_object: str
    load_type: str
    business_keys: tuple[str, ...]
    source_timezone: str = "UTC"
    watermark_type: str | None = None
    watermark_column: str | None = None
    lookback_days: int = 0
    is_active: bool = True
    load_order: int = 100

    @property
    def key(self) -> tuple[str, str]:
        return (self.source_system, self.entity)

    @property
    def bronze_table(self) -> str:
        """Tên bảng Bronze suy ra từ quy ước — không khai báo trong YAML (P4)."""
        return f"brz.brz_{self.source_system}_{self.entity}"

    def validate(self) -> list[str]:
        where = f"{self.source_system}.{self.entity}"
        errors = []
        for name in ("source_system", "entity"):
            if not _IDENTIFIER.match(getattr(self, name)):
                errors.append(f"{where}: {name} must be snake_case")
        if self.source_type not in SOURCE_TYPES:
            errors.append(f"{where}: source_type must be one of {sorted(SOURCE_TYPES)}")
        if self.load_type not in LOAD_TYPES:
            errors.append(f"{where}: load_type must be one of {sorted(LOAD_TYPES)}")
        if self.watermark_type is not None and self.watermark_type not in WATERMARK_TYPES:
            errors.append(f"{where}: watermark_type must be one of {sorted(WATERMARK_TYPES)}")
        if not self.business_keys:
            errors.append(f"{where}: business_keys must not be empty")
        if self.lookback_days < 0:
            errors.append(f"{where}: lookback_days must be >= 0")

        if self.source_type == "file" and self.watermark_type != "file_modified":
            errors.append(f"{where}: file sources need watermark_type 'file_modified'")
        if self.source_type == "db" and self.load_type == "incremental":
            if self.watermark_type != "column" or not self.watermark_column:
                errors.append(f"{where}: db incremental needs watermark_type 'column' + watermark_column")
        if self.source_type == "db" and self.load_type == "full_snapshot" and self.watermark_type:
            errors.append(f"{where}: db full_snapshot must not declare a watermark")
        if self.watermark_type != "column" and (self.watermark_column or self.lookback_days):
            errors.append(f"{where}: watermark_column/lookback_days only apply to watermark_type 'column'")

        errors += _check_timezone(self.source_timezone, f"{where}.source_timezone")
        return errors


def parse_source_entities(doc: dict) -> list[SourceEntity]:
    """YAML (lồng theo nguồn) → danh sách phẳng, đã kiểm tra. Lỗi gom lại báo một lần."""
    entities: list[SourceEntity] = []
    errors: list[str] = []
    for source_system, source in (doc.get("sources") or {}).items():
        shared = {k: v for k, v in source.items() if k != "entities"}
        for entity, spec in (source.get("entities") or {}).items():
            fields = {**shared, **spec}
            fields["business_keys"] = tuple(fields.get("business_keys") or ())
            try:
                item = SourceEntity(source_system=source_system, entity=entity, **fields)
            except TypeError as exc:  # khoá lạ / thiếu khoá bắt buộc
                errors.append(f"{source_system}.{entity}: {exc}")
                continue
            errors += item.validate()
            entities.append(item)

    seen: set[tuple[str, str]] = set()
    for item in entities:
        if item.key in seen:
            errors.append(f"{item.source_system}.{item.entity}: duplicated")
        seen.add(item.key)

    if errors:
        raise ConfigError(errors)
    return sorted(entities, key=lambda e: (e.load_order, e.source_system, e.entity))


def load_source_entities() -> list[SourceEntity]:
    return parse_source_entities(_read_yaml("source_entity.yml"))


def sync_source_entities(spark, entities: list[SourceEntity], table: str = CFG_SOURCE_ENTITY_TABLE):
    """Đồng bộ config vào bảng: thêm / sửa / XOÁ dòng không còn trong YAML (Git là nguồn sự thật)."""
    from companya_de.io import merge_delta  # import trễ: config không phụ thuộc Spark khi chỉ validate

    synced_at = dt.datetime.now(dt.UTC)
    rows = [
        (
            e.source_system, e.entity, e.source_type, e.source_object, e.load_type,
            e.watermark_type, e.watermark_column, e.lookback_days, e.source_timezone,
            list(e.business_keys), e.bronze_table, e.is_active, e.load_order, synced_at,
        )
        for e in entities
    ]
    df = spark.createDataFrame(rows, schema=spark.table(table).schema)
    return merge_delta(
        df, table, keys=["source_system", "entity"], check_unique=False, delete_not_matched_by_source=True
    )


def _check_timezone(name: str, where: str) -> list[str]:
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return [f"{where}: unknown timezone '{name}'"]
    return []
