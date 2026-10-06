# 1.6–1.7 — `nb_brz_load` và nối vào pipeline

## Mục tiêu
Landing → Bronze; mỗi entity / file có dòng audit trong `ingestion_batch`; watermark chỉ tiến khi thành công và **không bao giờ lùi**; notebook lỗi → pipeline FAILED.

## Hiểu trước khi làm

### Ai giữ log gì ([design/04 §1](../../design/04-control-tables.md))
- **Monitoring hub:** trạng thái pipeline/activity, thời lượng, lỗi, retry, Copy output (`rowsRead`, `filesWritten`).
- **`meta.ingestion_batch`:** dữ liệu nào đã được chấp nhận vào Bronze, cửa sổ, file/hash, trạng thái.
- **Delta history:** mỗi lần ghi bảng gắn `userMetadata = run_id` → biết version nào do run nào ghi (RESTORE).

### Định danh
- `ingestion_batch_id` — mỗi dòng log (mỗi lần thử) một id mới.
- `batch_id` = `<source>_<entity>_<yyyymmdd>_<8 ký tự RunId>` — sinh 1 lần ở lượt normal; **rerun giữ nguyên**. Bronze `_batch_id` = giá trị này.
- `landing_path` — thư mục landing của `batch_id`; rerun đọc lại đúng thư mục này.

### `nb_brz_load` (C viết)
Tham số: `p_load_date`, `p_run_id`, `p_mode`, `p_plan` (kế hoạch từ Lookup, JSON).

```
đầu notebook: userMetadata = p_run_id cho mọi lệnh ghi

normal:
  với mỗi entity trong p_plan (theo load_order):
    landing_path = Files/landing/<s>/<e>/load_date=<d>/batch=<p_run_id>/
    không có file → ingestion_batch NO_NEW_DATA
    db  : đọc parquet (giữ kiểu) → 1 dòng ingestion_batch
    file: đọc từng file bằng binaryFile → content_hash = sha2(nội dung)
          hash trùng file đã COMMITTED → SKIPPED_DUPLICATE (không vào Bronze)
          còn lại → parse CSV (header, mọi cột string) → 1 dòng ingestion_batch / file
    + _source_system, _entity, _source_file, _load_date, _batch_id, _run_id, _ingested_at (, _snapshot_at)
    DELETE brz.<t> WHERE _batch_id = X → append; cột mới → ingestion_batch.schema_changes
  mọi entity xong → watermark_state MERGE, KHÔNG LÙI:
    db_incremental: max(watermark hiện tại, max(updated_at) đã lấy)
    file          : max(watermark hiện tại, window_end)
    full snapshot : không đổi

rerun:
  ingestion_batch của p_load_date, status COMMITTED, run_mode normal|reprocess
  → nạp lại đúng landing_path, giữ batch_id → thêm dòng ingestion_batch run_mode = rerun
  → watermark không đổi

lỗi entity nào → ingestion_batch FAILED → raise → activity & pipeline FAILED (watermark không tiến)
```

Không có notebook "ghi kết thúc lượt chạy": pipeline lỗi ở bất kỳ activity nào thì Fabric tự ghi FAILED trong Monitoring hub.

## Làm

**C:** viết `nb_brz_load` vào repo. **B:** đóng tab → Update all, rồi trong `pl_ingest`:

1. Activities → **Notebook**; nối ✓ từ `fe_source_entity`.
2. General → Name `nb_brz_load` · Retry `1`.
3. Settings → Notebook `nb_brz_load` · Base parameters:

   | Name | Type | Value (dynamic) |
   |---|---|---|
   | `p_load_date` | String | `@pipeline().parameters.p_load_date` |
   | `p_run_id` | String | `@pipeline().RunId` |
   | `p_mode` | String | `@pipeline().parameters.p_mode` |
   | `p_plan` | String | `@string(activity('lkp_source_entity').output.value)` |
4. **Ctrl+S** → **Commit**.

## Kết quả mong đợi
- Pipeline: `set_run_start → lkp_source_entity → fe_source_entity → nb_brz_load`, mọi mũi tên Succeeded.
- Kiểm thật ở file 06.

## Spike ở bước này
- **K5:** metadata file **nguồn** (size, LastModified) — bản đầu ghi theo file landing; dedup không phụ thuộc vì dùng hash.
- **K6:** `%%configure -f` khi notebook chạy từ pipeline.
