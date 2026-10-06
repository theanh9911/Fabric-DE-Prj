# 1.5b — Nhánh `db`: ERP → landing

## Mục tiêu
1 activity Copy phục vụ cả 2 chiến lược ERP: master lấy **toàn bảng**, orders lấy **theo cửa sổ đồng hồ nguồn**.

## Hiểu trước khi làm
Câu SQL sinh từ dòng kế hoạch của Lookup:

| Entity | Câu SQL sinh ra (`p_load_date = 2026-01-01`, lần đầu) |
|---|---|
| `wholesale.customers` (db_full_snapshot) | `SELECT * FROM dbo.customers` |
| `wholesale.orders` (db_incremental) | `SELECT * FROM dbo.orders WHERE updated_at > '1899-12-31 00:00:00' AND updated_at < '2026-01-02 00:00:00'` |

- `updated_at` và cận trên cùng **đồng hồ nguồn** (virtual clock) — không so với giờ thật.
- Cận trên loại các dòng `updated_at` năm 2027 → không đẩy watermark lên tương lai; số dòng này lộ ở recon.
- Ghi **parquet** (giữ kiểu ERP). Thư mục mới mỗi lượt (`batch=<RunId>`) → landing bất biến.

## Làm (B)
`fe_source_entity` ✏️ → `sw_source_type` → case **db** ✏️ → `cp_db_to_landing` (đã có từ 1.1):

1. **General:** Retry `2`.
2. **Source:** Connection `sqldb_erp_wholesale` (giữ) · Use query **Query** · **thay** biểu thức:
   ```
   @if(equals(item().load_strategy, 'db_full_snapshot'),
       concat('SELECT * FROM ', item().source_object),
       concat('SELECT * FROM ', item().source_object,
              ' WHERE ', item().watermark_column, ' > ''', item().window_start, '''',
              ' AND ', item().watermark_column, ' < ''', item().window_end, ''''))
   ```
3. **Destination** (giữ như 1.1): `lh_platform` · Root folder **Files** · thư mục
   ```
   @concat('landing/', item().source_system, '/', item().entity, '/load_date=', pipeline().parameters.p_load_date, '/batch=', pipeline().RunId)
   ```
   tên file `@concat(item().entity, '.parquet')` · **Parquet**.
4. **Ctrl+S** → **Commit**.

## Kết quả mong đợi (sau Run ở file 06)
- `landing/wholesale/{customers,products,sales_hierarchy,orders}/load_date=2026-01-01/batch=…/<entity>.parquet`.
- Output → `cp_db_to_landing` (orders) → 👓 → **Rows read** = số dòng `dbo.orders` có `updated_at < 2026-01-02` (≈ 41.940 trừ dòng 2027).
