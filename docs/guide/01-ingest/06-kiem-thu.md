# 1.6 — Kiểm thử Bước 1

## Mục tiêu
Chứng minh ingest **đủ** (không mất dòng), **idempotent** (chạy lại không đổi), **incremental** (ngày sau chỉ lấy phần mới).

## Trước khi bắt đầu
Xoá dữ liệu thử của các lần chạy trước để bắt đầu sạch: xoá thư mục `Files/landing/`, các bảng `brz.*`, và dòng trong `meta.state_watermark`, `meta.state_file_manifest` (Claude sẽ viết sẵn 1 cell reset trong `nb_ops_reset_ingest` khi tới bước này).

## Các ca kiểm thử

| # | Làm | Kỳ vọng |
|---|---|---|
| T1 | Run `pl_ingest` với `p_load_date = 2026-01-01` | 9 bảng `brz.*`; `brz_wholesale_orders` = số dòng `dbo.orders` ở nguồn; `brz_retail_orders` = tổng dòng 2 file csv; 9 dòng watermark |
| T2 | Run lại **ngay** với `2026-01-01` | Số dòng mọi bảng `brz.*` **không đổi**; watermark vẫn 9 dòng cho ngày đó |
| T3 | Chạy `nb_00_sim_daily` (Source → 2026-01-02), rồi Run `pl_ingest` với `2026-01-02` | Bronze **chỉ tăng** khoảng 62–66 dòng orders mỗi nguồn (+ dòng lookback ERP); master chỉ tăng nếu có file snapshot mới |
| T4 | Run lại `2026-01-02` | Không đổi so với sau T3 |

## Query đối chiếu (chạy trong notebook `%%sql` gắn `lh_platform`)

```sql
SELECT _source_system, _load_date, _batch_id, count(*) AS rows
FROM brz.brz_wholesale_orders
GROUP BY ALL ORDER BY _load_date;

SELECT * FROM meta.state_watermark ORDER BY load_date, source_system, entity;
```

Số dòng phía nguồn: trong ws Source → `sqldb_erp_wholesale` → New query → `SELECT count(*) FROM dbo.orders`.

## Xong Bước 1 khi
T1–T4 đều đạt → cập nhật trạng thái trong [README](README.md) và [PLAN §15](../../PLAN.md#15-roadmap-từng-bước).
