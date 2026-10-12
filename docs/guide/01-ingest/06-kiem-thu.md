# 1.8 — Kiểm thử Bước 1

## Mục tiêu
Chứng minh ingest **đủ**, **rerun không đổi**, **incremental đúng**, **lỗi không làm mất dữ liệu**.

## Năm ca kiểm thử

| # | Làm | Kỳ vọng |
|---|---|---|
| **T1 normal** | Run `pl_ingest`: `p_load_date = 2026-01-01`, `p_mode = normal` | 9 bảng `brz.*` được tạo · `ingestion_batch`: 4 dòng db + 6 dòng file (2 orders, 3 master retail, 1 categories), tất cả COMMITTED · `watermark_state` 9 dòng · Monitoring hub: pipeline Succeeded |
| **T2 rerun** | Run `2026-01-01`, `p_mode = rerun` | Không có Copy chạy · count + checksum mọi `brz.*` **không đổi** · `ingestion_batch` thêm dòng `rerun` cùng `batch_id` · watermark không đổi |
| **T3 ngày mới** | Source: chạy `nb_00_sim_daily` (→ 2026-01-02). Run `2026-01-02`, `normal` | Vẫn 9 bảng `brz.*` (không tạo/xoá bảng) · orders mỗi nguồn tăng ≈ 62–66 dòng; ERP orders có thêm phần trùng do lookback 1 ngày (bình thường, Silver dedup) · master ERP thêm 1 snapshot · `orders_20260101.csv` bị quét lại do lookback → `SKIPPED_DUPLICATE` · entity file không có file mới → `NO_NEW_DATA` |
| **T3b không lùi** | Ngay sau T3, Run `2026-01-02` normal **lần nữa** (nguồn không đổi, chỉ đọc lại phần lookback) | ERP orders: `watermark_after = watermark_before` · file nào được copy lại do lookback → `SKIPPED_DUPLICATE`; entity không có file mới → `NO_NEW_DATA` |
| **T4 lỗi giữa lượt** | Tạo lỗi có kiểm soát ở một entity sau khi entity trước đã commit | Pipeline FAILED; entity trước giữ Bronze + audit + watermark đã commit; entity lỗi còn PREPARED/BRONZE_WRITTEN và watermark không tiến. Replay đúng `p_batch_id` không làm mất hoặc append trùng batch |
| **T5 lỗi ghi** | Gây lỗi giữa thao tác ghi Bronze và ghi audit/checkpoint, chỉ sau khi có recovery guard | Retry hoàn tất audit/checkpoint cho cùng `batch_id`, không mất dữ liệu và không thêm bản sao |

## Query kiểm (notebook `%%sql`, gắn `lh_platform`)

```sql
-- Bronze theo batch
SELECT _load_date, _batch_id, count(*) AS rows FROM brz.brz_wholesale_orders GROUP BY ALL ORDER BY 1;

-- Từng lần thử ingest
SELECT load_date, run_mode, source_system, entity, batch_id, source_file_name,
       window_start, window_end, watermark_before, watermark_after,
       content_hash, bronze_row_count, status
FROM meta.ingestion_batch ORDER BY started_at_utc;

SELECT * FROM meta.watermark_state ORDER BY source_system, entity;

-- T2: checksum trước/sau rerun — cột nghiệp vụ + batch
-- (_run_id, _ingested_at đổi theo lượt rerun là đúng: cho biết lần ghi gần nhất)
SELECT count(*) AS n, sum(xxhash64(_batch_id, _source_file, order_no, product_code, order_status, updated_at)) AS checksum
FROM brz.brz_retail_orders;

-- Version Bronze do run nào ghi (dùng khi cần RESTORE)
DESCRIBE HISTORY brz.brz_wholesale_orders;   -- kiểm userMetadata có run_id, batch_id, operation (K8)
```

Trạng thái lượt chạy, thời lượng, lỗi, Copy output (`rowsRead`, `filesWritten`): **Monitoring hub** → chọn lượt chạy `pl_ingest`.

## Đối soát Source ↔ Bronze sau T1

| Nguồn | So | Cách lấy phía nguồn |
|---|---|---|
| ERP orders | dòng | ws Source → `sqldb_erp_wholesale` → New query: `SELECT count(*) FROM dbo.orders WHERE updated_at < '2026-01-02'` = `brz_wholesale_orders` |
| ERP master | dòng | `SELECT count(*) FROM dbo.<entity>` = số dòng snapshot trong Bronze |
| File | **số file** | Copy `filesWritten` so với số file xử lý trong `ingestion_batch` (COMMITTED + SKIPPED_DUPLICATE; loại NO_NEW_DATA). Nếu chưa lấy/truyền được metric per-entity thì đây là kiểm tra thủ công Monitoring hub, chưa phải recon tự động. Số dòng chỉ đối chiếu sau parse |
| Dòng ERP tương lai | dòng | `SELECT count(*) FROM dbo.orders WHERE updated_at >= '2026-01-02'` → ghi nhận, phải **không** có trong Bronze |

## Xong Bước 1 khi
T1–T5 đạt → cập nhật trạng thái [README](README.md), [PLAN §5](../../PLAN.md#5-roadmap-end-to-end) → Commit.
