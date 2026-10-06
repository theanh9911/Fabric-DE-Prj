import pytest

from companya_de.migrate import Migration, MigrationError, apply_migrations, discover


def test_discover_finds_ordered_migrations():
    versions = [m.version for m in discover()]
    assert versions == sorted(versions) and versions[0] == 1


def test_statements_ignore_comment_lines_with_semicolons():
    m = Migration(99, "x", "-- note; with semicolon\nCREATE SCHEMA a;\n-- another;\nCREATE SCHEMA b;")
    assert m.statements() == ["CREATE SCHEMA a", "CREATE SCHEMA b"]


def test_layer_schemas_and_meta_tables_exist(migrated):
    spark = migrated
    schemas = {r[0] for r in spark.sql("SHOW SCHEMAS").collect()}
    assert {"brz", "slv", "gld", "meta"} <= schemas
    for table in ("cfg_source_entity", "state_watermark", "state_file_manifest",
                  "log_pipeline_run", "log_task_run", "schema_registry", "schema_migrations"):
        assert spark.catalog.tableExists(f"meta.{table}"), table


def test_reapply_is_noop(migrated):
    assert apply_migrations(migrated) == []


def test_modified_applied_migration_is_rejected(migrated):
    tampered = [Migration(m.version, m.name, m.sql + "\n-- edited") for m in discover()]
    with pytest.raises(MigrationError, match="modified after being applied"):
        apply_migrations(migrated, tampered)


def test_new_migration_is_applied_once(migrated):
    extra = [*discover(), Migration(999, "test_extra", "CREATE SCHEMA IF NOT EXISTS test_extra;")]
    assert apply_migrations(migrated, extra) == [999]
    assert apply_migrations(migrated, extra) == []
