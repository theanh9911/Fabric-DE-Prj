import pytest

from companya_de.config import (
    CFG_SOURCE_ENTITY_TABLE,
    ConfigError,
    load_settings,
    load_source_entities,
    parse_source_entities,
    sync_source_entities,
)


def _doc(**entity):
    spec = {"source_object": "dbo.x", "load_type": "full_snapshot", "business_keys": ["id"], **entity}
    return {"sources": {"erp": {"source_type": "db", "entities": {"x": spec}}}}


def test_packaged_config_is_valid():
    entities = load_source_entities()
    keys = {e.key for e in entities}
    assert ("wholesale", "orders") in keys
    assert ("retail", "orders") in keys
    assert ("reference", "categories") in keys
    assert len(keys) == len(entities)


def test_settings_load():
    assert load_settings().lakehouse == "lh_platform"


def test_entities_sorted_by_load_order():
    orders = [e.load_order for e in load_source_entities()]
    assert orders == sorted(orders)


def test_bronze_table_derived_from_convention():
    e = next(e for e in load_source_entities() if e.key == ("retail", "orders"))
    assert e.bronze_table == "brz.brz_retail_orders"


def test_source_level_fields_are_inherited():
    e = next(e for e in load_source_entities() if e.key == ("wholesale", "orders"))
    assert e.source_type == "db" and e.source_timezone == "UTC"


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"load_type": "incremental"}, "db incremental needs"),
        ({"watermark_type": "file_modified"}, "db full_snapshot must not declare"),
        ({"business_keys": []}, "business_keys must not be empty"),
        ({"source_timezone": "Mars/Base"}, "unknown timezone"),
        ({"load_type": "sometimes"}, "load_type must be one of"),
        ({"lookback_days": 3}, "only apply to watermark_type 'column'"),
        ({"unknown_field": 1}, "unexpected keyword"),
    ],
)
def test_invalid_entity_rejected(override, message):
    with pytest.raises(ConfigError) as exc:
        parse_source_entities(_doc(**override))
    assert any(message in e for e in exc.value.errors)


def test_file_source_requires_file_modified_watermark():
    doc = {"sources": {"drop": {"source_type": "file", "entities": {"x": {
        "source_object": "inbound/x", "load_type": "incremental", "business_keys": ["id"],
    }}}}}
    with pytest.raises(ConfigError, match="file sources need"):
        parse_source_entities(doc)


def test_all_errors_reported_at_once():
    doc = _doc(load_type="sometimes", business_keys=[], source_timezone="Mars/Base")
    with pytest.raises(ConfigError) as exc:
        parse_source_entities(doc)
    assert len(exc.value.errors) >= 3


def test_sync_is_idempotent_and_removes_deleted_entities(migrated):
    spark = migrated
    entities = load_source_entities()

    sync_source_entities(spark, entities)
    sync_source_entities(spark, entities)
    assert spark.table(CFG_SOURCE_ENTITY_TABLE).count() == len(entities)

    remaining = [e for e in entities if e.key != ("reference", "categories")]
    sync_source_entities(spark, remaining)
    keys = {(r.source_system, r.entity) for r in spark.table(CFG_SOURCE_ENTITY_TABLE).collect()}
    assert ("reference", "categories") not in keys
    assert len(keys) == len(remaining)

    sync_source_entities(spark, entities)  # trả lại trạng thái cho test khác
