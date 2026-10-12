# Platform implementation status

This page maps the implementation in `fabric/platform/` to the current source of
truth, [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Code in the repository

| Stage | Fabric items | Implemented behavior |
|---|---|---|
| Bootstrap | `nb_setup_ddl`, `nb_config`, `nb_common` | `slv`, `gld`, `meta`; three metadata tables; configuration validated from Git |
| Ingest | `pl_master_daily`, `pl_backfill`, `pl_daily_core`, `nb_ingest_plan`, `nb_ingest` | master and serial backfill call `nb_process` as notebook activities; source windows, SQL Database and drop-zone reads, snapshot checksum, immutable per-delivery Landing and manifest |
| Silver | `nb_slv_load` | current state per source, append-only order events and snapshot observations, technical quarantine, audit/watermark |
| Gold | `nb_gld_dims`, `nb_gld_fact` | date/customer/product/salesman dimensions, SCD2 from snapshot history, current recognized-sales fact and monthly aggregate |
| Quality/ops | `nb_dq`, `nb_ops_recon`, `nb_process`, `nb_sla_watch` | critical DQ checks, fact-to-aggregate and delivery accounting, normal/rerun selection, missing daily delivery warnings |
| Backfill | `pl_backfill` | serial daily calls to `pl_master_daily` over the requested date range |
| Maintenance | `pl_maintenance`, `nb_maintenance` | OPTIMIZE existing Silver/Gold Delta tables; no VACUUM or Landing deletion is automated |
| Watchdog | `pl_sla_watch` | checks daily source deliveries; assign a Fabric schedule and alert route in the workspace |

No Bronze schema, `cfg_*` tables, or `state_*` tables are created. `meta` has
`ingestion_batch`, `table_run`, and `check_result` plus `meta.v_watermark`.

## Deploy sequence

1. Connect the Fabric Dev workspace to this folder/branch and sync the items.
2. Confirm the source workspace and lakehouse names in `pl_master_daily` parameters.
3. Grant the executing identity read access to the source SQL Database and drop-zone Lakehouse, and write access to the platform Lakehouse.
4. Run `nb_setup_ddl`, then run `nb_config` to validate configuration.
5. Run `pl_master_daily` with a simulator date and a single small entity first; then run all sources and replay the same date with `p_mode=rerun`.
6. Inspect Landing manifests, `meta.ingestion_batch`, Silver/Gold counts, quarantine, `meta.check_result`, and pipeline monitoring before scheduling.

The checked-in pipeline activities use the Fabric Git workspace placeholder
`00000000-0000-0000-0000-000000000000`; Git sync must bind notebook references to
the target workspace. The SQL Database and Lakehouse are resolved by name through
the Fabric REST API at runtime, so the workspace identity must be permitted to
list and read those source items.

## Current implementation boundary

- Runtime-dependent Fabric behavior (Notebook activity output shape, REST API
  permissions, OneLake binary copy semantics, CDF/table property support, and
  semantic-model framing) still needs a deploy smoke run. The repository can be
  syntax-checked locally but cannot prove these workspace behaviors.
- The platform reads CSV drop-zone files via Spark for row counts and copies the
  original bytes to Landing. File modified-time filtering uses `modifyTime` when
  the OneLake listing exposes it; if absent, the runner includes the file and
  relies on idempotent Silver merges.
- Gold currently builds the implemented Company A sales model. Business owners
  still need to confirm tax treatment and return handling; the current fact
  recognizes `Delivered` only and computes tax as gross × parsed rate.
- `sm_sales`, `rpt_sales`, `rpt_platform_health`, Teams/email alert bindings,
  and `rpt_platform_health` are not yet Fabric items in the repository. The
  notebooks expose data needed by those items; semantic model/report definitions
  need to be authored/bound after Gold is deployed and inspected.
- Gold is selected by Silver table-version comparison rather than CDF
  `availableNow`. This avoids depending on unresolved K9 behavior while keeping
  missed Silver versions recoverable after a Gold failure.
