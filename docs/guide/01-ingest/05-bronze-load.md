# 1.6–1.7 — `nb_brz_load` và nối vào pipeline

## Mục tiêu
Landing → Bronze; mỗi entity / file có dòng audit trong `ingestion_batch`; watermark chỉ tiến khi thành công và **không bao giờ lùi**; notebook lỗi → pipeline FAILED.

## Hiểu trước khi làm

### Ai giữ log gì ([design/04 §1](../../design/04-control-tables.md))
- **Monitoring hub:** trạng thái pipeline/activity, thời lượng, lỗi, retry, Copy output (`rowsRead`, `filesWritten`).
- **`meta.ingestion_batch`:** dữ liệu nào đã được chấp nhận vào Bronze, cửa sổ, file/hash, trạng thái.
- **Delta history:** mỗi lần ghi bảng gắn `run_id`, `batch_id`, operation vào `userMetadata` → tra version cụ thể (K8 cần xác minh trên Fabric Runtime).

### Định danh
- `ingestion_batch_id` — mỗi dòng log (mỗi lần thử) một id mới.
- DB `batch_id` = `<source>_<entity>_<yyyymmdd>_<run-token>`; file thêm token riêng theo từng file. Sinh 1 lần cho input; **rerun/recovery giữ nguyên**. Bronze `_batch_id` = giá trị batch đó.
- `landing_path` — thư mục landing của `batch_id`; rerun đọc lại đúng thư mục này.

### `nb_brz_load` (C viết)
Tham số: `p_load_date`, `p_run_id`, `p_mode`, `p_batch_id` (rerun/recovery), `p_plan` (kế hoạch từ Lookup, JSON).

```
đầu notebook: userMetadata gồm p_run_id, batch_id, operation cho mọi lệnh ghi

normal:
  với mỗi entity trong p_plan (theo load_order):
    landing_path lấy từ batch_id đã đăng ký; batch mới ghi path bất biến
    chuẩn bị đủ audit PREPARED cho mọi input của entity trước khi ghi Bronze
    không có file → ingestion_batch NO_NEW_DATA
    db  : đọc parquet (giữ kiểu) → 1 dòng ingestion_batch
    file: đọc từng file bằng binaryFile → content_hash = sha2(nội dung)
          hash trùng file khác trong cùng (source_system, entity) đã COMMITTED → SKIPPED_DUPLICATE (không vào Bronze)
          còn lại → parse CSV (header, mọi cột string) → 1 dòng ingestion_batch / file
    + _source_system, _entity, _source_file, _load_date, _batch_id, _run_id, _ingested_at (, _snapshot_at)
    Bronze ghi đè có điều kiện theo _batch_id bằng 1 commit Delta nguyên tử đã xác minh
    từng ingestion_batch → BRONZE_WRITTEN
  chỉ khi mọi input entity đã xử lý xong → watermark_state MERGE entity đó, KHÔNG LÙI:
    db_incremental: max(watermark hiện tại, max(updated_at) đã lấy)
    file          : max(watermark hiện tại, window_end)
    full snapshot : không đổi
  ingestion_batch BRONZE_WRITTEN → COMMITTED

rerun/recovery (p_batch_id tùy chọn; bỏ trống để lấy mọi batch của p_load_date):
  tra batch theo p_batch_id (nếu truyền) hoặc p_load_date; lấy đúng landing_path, giữ batch_id
  recovery cùng batch_id đi trước dedup hash: PREPARED → atomic Bronze write; BRONZE_WRITTEN → chỉ hoàn tất watermark/audit
  COMMITTED → chạy lại cùng input/code theo yêu cầu; watermark không lùi

lỗi entity nào → giữ nguyên stage, ghi error_message nếu có thể; raise → activity & pipeline FAILED trong Monitoring hub; entity đã commit giữ checkpoint, entity lỗi không tiến watermark
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
| `p_batch_id` | String | `@pipeline().parameters.p_batch_id` (tùy chọn; trống = rerun toàn bộ batch theo ngày) |
   | `p_plan` | String | `@string(activity('lkp_source_entity').output.value)` |
4. **Ctrl+S** → **Commit**.

## Kết quả mong đợi
- Pipeline: `set_run_start → lkp_source_entity → fe_source_entity → nb_brz_load`, mọi mũi tên Succeeded.
- Kiểm thật ở file 06.

## Spike ở bước này
- **K5:** metadata file **nguồn** (size, LastModified) — bản đầu ghi theo file landing; dedup không phụ thuộc vì dùng hash.
- **K6:** `%%configure -f` khi notebook chạy từ pipeline.
