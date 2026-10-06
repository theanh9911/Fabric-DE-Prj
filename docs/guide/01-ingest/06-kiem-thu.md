# 1.8 — Kiểm thử Bước 1

## Mục tiêu
Chứng minh ingest **đủ**, **rerun không đổi**, **incremental đúng**, **lỗi không làm mất dữ liệu**.

## Ca kiểm thử

| # | Làm | Kỳ vọng |
|---|---|---|
| T1 normal | Run `pl_ingest`: `p_load_date = 2026-01-01`, `p_mode = normal` | 9 bảng `brz.*`; `ingestion_batch` COMMITTED đủ 9 entity (file: 1 dòng / file); `watermark_state` 9 dòng; `pipeline_run` SUCCEEDED |
| T2 rerun | Run `2026-01-01`, `p_mode = rerun` | Không có Copy; số dòng & checksum mọi `brz.*` **không đổi**; `ingestion_batch` thêm dòng `rerun`; watermark không đổi |
| T3 ngày mới | Source: chạy `nb_00_sim_daily` (→ 2026-01-02). Run `2026-01-02`, `normal` | `brz_*_orders` tăng ≈ 62–66 dòng / nguồn (+ dòng lookback ERP); master ERP thêm 1 snapshot; file master chỉ thêm nếu có snapshot mới |
| T4 lỗi | Tạm đổi `source_object` của 1 entity thành tên sai trong `nb_setup_config` → chạy lại config → Run `2026-01-02` `normal` → khôi phục config → Run lại | Lần lỗi: pipeline FAILED, `pipeline_run` FAILED, watermark entity lỗi **không tiến**. Lần sau: lấy đủ, không trùng |

## Query kiểm (notebook `%%sql`, gắn `lh_platform`)

```sql
-- Bronze theo batch
SELECT _load_date, _batch_id, count(*) AS rows FROM brz.brz_wholesale_orders GROUP BY ALL ORDER BY 1;

-- Cửa sổ đã đọc
SELECT load_date, run_mode, source_system, entity, window_start, window_end,
       watermark_before, watermark_after, source_file_path, bronze_row_count, status
FROM meta.ingestion_batch ORDER BY started_at_utc;

SELECT * FROM meta.watermark_state ORDER BY source_system, entity;
SELECT * FROM meta.pipeline_run ORDER BY started_at_utc;

-- T2: checksum trước/sau rerun — chỉ trên cột nghiệp vụ + batch
-- (_run_id, _ingested_at đổi theo lượt rerun là đúng: cho biết lần ghi gần nhất)
SELECT count(*) AS n, sum(xxhash64(_batch_id, _source_file, order_no, product_code, order_status, updated_at)) AS checksum
FROM brz.brz_retail_orders;
```

Phía nguồn (ws Source → `sqldb_erp_wholesale` → New query):
```sql
SELECT count(*) AS n, sum(CASE WHEN updated_at > SYSUTCDATETIME() THEN 1 ELSE 0 END) AS future_rows FROM dbo.orders;
```
→ `n − future_rows` phải bằng số dòng `brz_wholesale_orders` sau T1.

## Xong Bước 1 khi
T1–T4 đạt → cập nhật trạng thái ở [README](README.md), [PLAN §5](../../PLAN.md#5-roadmap-end-to-end) → Commit.
