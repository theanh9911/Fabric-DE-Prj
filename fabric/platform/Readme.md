# Data Platform Fabric items

This folder is the deployable source for the Company A Data Platform workspace.
The current architecture is `docs/ARCHITECTURE.md`; Landing is the raw layer and
there is no Bronze Delta table.

## Main flow

`pl_master_daily` runs `nb_process` directly. `pl_backfill` runs the same notebook
sequentially for each requested date. `pl_daily_core` remains a standalone runner.
Processing writes source-specific Silver tables,
the shared order event and snapshot history, Gold dimensions/fact/aggregate,
quarantine, and DQ/reconciliation results.

## Notebooks

- `nb_setup_ddl`: schemas and three metadata tables.
- `nb_config`: versioned `SOURCES` and `TABLES` configuration.
- `nb_common`: Delta, landing, audit, check, and quarantine helpers.
- `nb_ingest_plan` / `nb_ingest`: source windows, SQL/file delivery, immutable Landing and manifests.
- `nb_slv_load`: delivery replay, technical quarantine, current Silver and append-only event/snapshot history.
- `nb_gld_dims` / `nb_gld_fact`: dimensions, SCD2 history, sales fact and monthly aggregate.
- `nb_dq` / `nb_ops_recon`: critical checks and reconciliation.
- `nb_sla_watch`: missing daily delivery warning; `nb_maintenance`: Delta OPTIMIZE.
- `nb_process`: normal/rerun selection and processing order.

## Workspace binding

The source workspace, SQL Database and drop-zone Lakehouse are pipeline parameters
(`CompanyA-Source`, `sqldb_erp_wholesale`, `lh_retail_drop` by default). The
executing identity needs read access to those items and write access to
`lh_platform`. Pipeline activity workspace IDs are Git placeholders and must be
bound by Fabric Git integration in the target workspace.

Semantic model/report items are not yet present in this folder. Add and bind them
after the Gold schema has been deployed and inspected in the target workspace.

## Notebook Lakehouse binding

Notebooks no longer use `%%configure -f` to choose the default Lakehouse. In the
Dev workspace, add `lh_platform` through each notebook's **Data items** pane and
set it as the default Lakehouse. Enable **Lakehouse Auto-Binding in Git** in the
notebook Git settings and commit the generated binding metadata to the branch.
Attach it to every notebook that reads or writes platform tables. A Spark session
is still required to run these notebooks; removing `%%configure -f` avoids its
forced session restart, not the underlying Spark compute.
