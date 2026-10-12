# `pl_master_daily` runbook

This guide describes `pl_master_daily` parameters and the current behavior of
`nb_process`. The same parameters are passed by `pl_daily_core`; `pl_backfill`
applies them once per date in its requested range.

For the source-to-table-to-notebook dependency matrix and exact execution order,
see [`DEPENDENCIES.md`](DEPENDENCIES.md).

## Parameters

| Parameter | Default | Meaning and guidance |
|---|---|---|
| `p_load_date` | empty | Batch date in `YYYY-MM-DD`. Empty on a normal run means yesterday in Bangkok time. Set explicitly for recovery. |
| `p_load_date_to` | empty | Inclusive end batch date. Empty means the same date as `p_load_date`. Bound the range for reruns. |
| `p_mode` | `normal` | `normal` runs planner/source ingest and processes not-yet-loaded Landing manifests. `rerun` skips ingest, replays matching manifests through Silver, and forces Gold. |
| `p_from_layer` | `ingest` | `ingest`, `slv`, or `gld`. `ingest` is end-to-end start; `slv` skips source ingest; `gld` skips ingest and Silver. |
| `p_entities` | `ALL` | `ALL` selects all active source entities. A comma-separated list (e.g. `retail.orders,wholesale.orders`) scopes ingest/Silver and Silver DQ to those sources. The master pipeline does not support table-scoped Gold requests; use `ALL` for a complete Gold/DQ/reconciliation run. |
| `p_full_reload` | `false` | Requests full reload behavior in Silver/Gold. Use only for a deliberate rebuild, not as a generic retry flag. |
| `p_source_workspace` | `CompanyA-Source` | Workspace containing source data; normally leave unchanged. |
| `p_erp_item` | `sqldb_erp_wholesale` | Source SQL item; normally leave unchanged. |
| `p_drop_lakehouse` | `lh_retail_drop` | Drop-zone Lakehouse; normally leave unchanged. |

Dates are batch/ingestion dates, not necessarily business dates in source rows.
Find the batch ID in `meta.table_run` or `meta.ingestion_batch` before replaying.
For example, batch ID `20261011` maps to `p_load_date=2026-10-11`.

## Common cases

### Normal daily run

```text
p_load_date     = blank (yesterday, Bangkok time)
p_load_date_to  = blank
p_mode          = normal
p_from_layer    = ingest
p_entities      = ALL
p_full_reload   = false
```

This plans/ingests current source windows, loads pending Landing manifests to
Silver, rebuilds Gold when source versions changed, Gold has never succeeded,
or any configured Gold target is missing, then runs full DQ and reconciliation.

### DQ fails because required Gold tables are missing

`Required Gold tables are missing: ...` means the Gold build did not create all
required outputs. The master now forces Gold if any configured output is
missing and checks the outputs before full DQ. If an output is still missing,
the run fails at `pipeline.gold` with the exact missing table list rather than
waiting for DQ to fail later.

For an already-ingested specific batch, replay it through Silver and force Gold:

```text
p_load_date     = <exact batch date, YYYY-MM-DD>
p_load_date_to  = <same date, or blank>
p_mode          = rerun
p_from_layer    = slv
p_entities      = ALL
p_full_reload   = false
```

`rerun` skips source ingest/planning, replays matching Landing manifests through
Silver, and forces Gold before DQ. Use the exact date; leaving dates blank can
match a much broader manifest range. If multiple batches are incomplete, set
the intended inclusive start/end dates.

If the intended rows and snapshot/event history already exist in Silver but
there are no Landing manifests to replay, rebuild Gold from Silver:

```text
p_load_date     = blank
p_load_date_to  = blank
p_mode          = rerun
p_from_layer    = gld
p_entities      = ALL
p_full_reload   = false
```

This skips Silver and forces Gold. It cannot recover rows that never reached
Silver; use the prior `slv` case for those.

### Ingest succeeded but Silver did not finish

Use `p_mode=rerun`, `p_from_layer=slv`, the exact batch date, and `p_entities=ALL`
(or exact source IDs). This avoids another source read and replays the batch's
Landing manifests through Silver, then Gold and DQ.

### Normal run stopped during ingest

Inspect audit events and manifests first. If a new source pull is needed, run
`p_mode=normal`, `p_from_layer=ingest`, with the exact batch date and intended
entities. This performs source planning/ingest again; already-loaded manifests
are excluded from the ordinary Silver pending set. If deliveries are already in
Landing and only downstream processing is incomplete, use the `slv` rerun case.

### Intentionally rebuild Gold

Use `p_mode=rerun`, `p_from_layer=gld`, `p_entities=ALL`. Set
`p_full_reload=true` only when table replacement is intended; leave it false for
a routine Gold repair.

### Partial entity run

Set `p_entities` to comma-separated source IDs such as
`retail.orders,wholesale.orders`. Ingest and Silver process only those sources;
DQ checks only their Silver tables, and reconciliation is explicitly marked
`SKIPPED` because it compares the complete Gold fact and aggregate. This is a
scoped operation, not proof that the complete Gold model is ready. Use
`p_entities=ALL` for an end-to-end readiness check. Its terminal audit event is
`PARTIAL`; a full run ends with `SUCCEEDED`.

`p_entities` accepts source entity IDs only. It does not support a request like
`gld.gld_d_customer` for table-scoped Gold processing. `p_mode=rerun` with
`p_from_layer=ingest` is rejected because rerun never reads source data; use
`normal/ingest` to acquire source data or `rerun/slv` to replay Landing.

## Mode/layer behavior

| `p_mode` | `p_from_layer` | Planner/ingest | Silver | Gold | Use |
|---|---|---|---|---|---|
| `normal` | `ingest` | Runs | Pending manifests | If versions changed / first Gold build | Daily run |
| `normal` | `slv` | Skipped | Pending manifests | If versions changed / first Gold build | Process existing deliveries; does not force Gold |
| `rerun` | `slv` | Skipped | Replays matching manifests | Forced | Recover batch through Silver and Gold |
| `rerun` | `gld` | Skipped | Skipped | Forced | Rebuild Gold from existing Silver |
| `normal` | `gld` | Skipped | Skipped | If versions changed / first Gold build | Usually not suitable for a forced repair |

The table reflects current code: `p_mode=rerun` forces Gold; setting only
`p_from_layer=slv` does not. Missing configured Gold targets also force a build,
even when Silver versions match the last successful Gold run.

## Verify and diagnose

```sql
%%sql
WITH latest AS (
  SELECT run_id FROM meta.table_run
  WHERE table_name = 'pipeline.run_process'
  ORDER BY event_at_utc DESC LIMIT 1
)
SELECT event_at_utc, layer, table_name, event, rows_in, rows_inserted,
       rows_updated, rows_deleted, rows_quarantined, error_message
FROM meta.table_run
WHERE run_id IN (SELECT run_id FROM latest)
ORDER BY event_at_utc, layer, table_name;
```

For a full run, expected terminal success events include `pipeline.silver`,
`pipeline.gold`, `pipeline.dq`, `pipeline.reconciliation`, and finally
`pipeline.run_process = SUCCEEDED`. A scoped source run ends with
`pipeline.run_process = PARTIAL`, `pipeline.dq.partial_silver`, and an explicit
`pipeline.reconciliation = SKIPPED`. For missing Gold, inspect whether
`pipeline.gold` is `SKIPPED` or `FAILED` and review the `gld.*` events. If the
pipeline/notebook was manually stopped or Spark terminated, `STARTED` audit
rows can remain without a terminal event; Fabric Monitoring/Spark logs contain
the cancellation or capacity error because a terminated notebook cannot write
its final audit event.

The ingest wrapper validates that every planned source has exactly one result
(`DELIVERED`, `NO_DATA`, `UNCHANGED`, or `SKIPPED`) and that every delivered
source has a manifest. A contract mismatch fails the ingest stage and logs the
affected entities as `INCOMPLETE` instead of leaving them at `STARTED`.
In current code, ingest `rows_in`/`rows_inserted` are source row counts; a
`SUCCEEDED` entity event represents a delivery. Older audit runs may have used
these metric columns for source-result/delivery counts instead, so compare runs
only when they use the same deployed notebook version.

For delivery status, query `meta.ingestion_batch` by batch ID and inspect
`status`, `loaded_run_id`, `rows_read`, `rows_changed`, and `rows_quarantined`.
`STARTED` by itself is inconclusive; use activity/Spark logs to distinguish
cancellation, capacity failure, and a notebook exception.

## Cautions

- Check for existing Landing deliveries before rerunning `normal`; it may read
  from the source again.
- `rerun` replays all matching manifests in the chosen date range. Bound that
  range for targeted recovery.
- A Gold-only run requires the Silver current tables and snapshot/event history
  to be present.
- `p_full_reload=true` is for deliberate rebuilds, not ordinary retries.
- Ensure the target Lakehouse is attached and notebook bindings are valid before
  running; otherwise audit writes may fail before useful events are recorded.
