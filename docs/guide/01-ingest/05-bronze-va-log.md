# 1.6–1.7 — `nb_brz_load`, `nb_ops_run_end`, nối vào pipeline

## Mục tiêu
Landing → Bronze; mọi lượt có dấu vết (`ingestion_batch`, `task_run`, `pipeline_run`); watermark chỉ tiến khi thành công; pipeline lỗi vẫn báo lỗi.

## Hiểu trước khi làm

### `nb_brz_load` (C viết)
Tham số: `p_load_date`, `p_run_id`, `p_mode`, `p_plan` (kế hoạch từ Lookup, dạng JSON).

```
normal:
  với mỗi entity trong p_plan:
    landing = Files/landing/<s>/<e>/load_date=<d>/batch=<p_run_id>/   (không có → 0 dòng)
    đọc: parquet (db, giữ kiểu) | CSV header, mọi cột string (file)
    + _source_system, _entity, _source_file, _load_date, _batch_id, _run_id, _ingested_at (, _snapshot_at)
    DELETE brz.<t> WHERE _batch_id = X → append (cột mới: ghi nhận + task_run.details_json)
    ingestion_batch: COMMITTED (db: 1 dòng; file: 1 dòng / file), window, rows
    task_run: brz / entity / rows / delta_version
  tất cả xong → watermark_state MERGE (db_incremental: max(updated_at); file: window_end)

rerun:
  đọc ingestion_batch của p_load_date (COMMITTED) → nạp lại đúng landing_path, giữ batch_id
  ingestion_batch: thêm dòng run_mode = rerun · watermark: không đổi

lỗi entity nào → ingestion_batch FAILED + task_run FAILED → notebook fail (watermark không tiến)
```

### `nb_ops_run_end` (C viết)
Tham số: `p_run_id`, `p_pipeline`, `p_load_date`, `p_mode`, `p_status`, `p_started_at`, `p_error`. Ghi 1 dòng `pipeline_run`. `p_status = FAILED` → ghi xong thì **raise lỗi** → pipeline FAILED (log không che lỗi).

### Vì sao 2 activity kết thúc
`run_end_fail` nối từ `nb_brz_load` bằng **cả ✗ Failed và ↷ Skipped** (2 mũi tên từ cùng activity = HOẶC) → bắt được cả lỗi ở Copy (ForEach lỗi → `nb_brz_load` bị Skipped) lẫn lỗi ở notebook.

## Làm

**C:** viết 2 notebook vào repo. **B:** đóng tab → Update all, rồi trong `pl_ingest`:

1. **`nb_brz_load`** — Activities → **Notebook**; nối ✓ từ `fe_source_entity`.
   - Settings → Notebook `nb_brz_load` · Base parameters:

     | Name | Type | Value (dynamic) |
     |---|---|---|
     | `p_load_date` | String | `@pipeline().parameters.p_load_date` |
     | `p_run_id` | String | `@pipeline().RunId` |
     | `p_mode` | String | `@pipeline().parameters.p_mode` |
     | `p_plan` | String | `@string(activity('lkp_source_entity').output.value)` |
   - General → Retry `1`.
2. **`run_end_ok`** — Notebook `nb_ops_run_end`; nối ✓ từ `nb_brz_load`. Base parameters: `p_status = SUCCEEDED`, `p_run_id = @pipeline().RunId`, `p_pipeline = @pipeline().Pipeline`, `p_load_date`, `p_mode` như trên, `p_started_at = @pipeline().TriggerTime`, `p_error` trống.
3. **`run_end_fail`** — Notebook `nb_ops_run_end`; nối từ `nb_brz_load` bằng ô **✗ (Failed)** **và** ô **↷ (Skipped)**. Base parameters như trên nhưng `p_status = FAILED`, `p_error = @concat('pl_ingest failed, run ', pipeline().RunId)`.
4. **Ctrl+S** → **Commit**.

## Kết quả mong đợi
- `{ }`: `run_end_fail.dependsOn` = `nb_brz_load` với `["Failed","Skipped"]`.
- Kiểm thật ở file 06.

## Spike cần kiểm ở bước này
- **K5:** lấy size / LastModified của file **nguồn** cho `ingestion_batch` (bản đầu ghi theo file landing; thử Copy *session log* / Get Metadata).
- **K6:** `%%configure -f` khi notebook chạy từ pipeline (standard session).
