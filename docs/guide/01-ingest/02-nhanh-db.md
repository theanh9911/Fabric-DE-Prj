# 1.2 — Nhánh `db`: ERP → landing ✅

## Mục tiêu
Với 4 entity `wholesale` (ERP), lấy dòng có `updated_at` trong khoảng `(watermark − lookback, run_start]` → ghi 1 file parquet vào landing.

## Hiểu trước khi làm
- Câu SQL **sinh động theo config**: cùng 1 activity Copy dùng cho cả 4 bảng ERP. Ví dụ `item()` = `wholesale.orders`, chưa có watermark, `run_start = 2026-10-06 08:58:00`:
  ```sql
  SELECT * FROM dbo.orders
  WHERE updated_at > DATEADD(day, -1, CAST('1900-01-01 00:00:00' AS datetime2))
    AND updated_at <= CAST('2026-10-06 08:58:00' AS datetime2)
  ```
- **Landing là bất biến:** mỗi lần chạy ghi vào thư mục `batch=<RunId>` mới, không ghi đè. Thư mục theo `load_date` giúp tìm & dọn theo ngày.
- **Parquet** giữ kiểu dữ liệu và nén tốt; Bronze sẽ đọc rồi ép về chuỗi.

## Làm
1. Vào `fe_source_entity` ✏️ → chọn `sw_source_type` → tab Activities → dòng **db** ✏️.
2. Xoá `wait_todo_db` → Activities → **Copy data ⌄ → Add copy data activity** → General → Name `cp_db_to_landing` · Retry `2`.
3. Tab **Source**:
   - Connection: **Browse all** → OneLake catalog → `sqldb_erp_wholesale` (loại *SQL database*, ở `CompanyA-Source`) → Connect.
   - Use query **Query** → *Add dynamic content*:
     ```
     @concat('SELECT * FROM ', item().source_object, ' WHERE ', item().watermark_column, ' > DATEADD(day, -', string(item().lookback_days), ', CAST(''', item().watermark_value, ''' AS datetime2)) AND ', item().watermark_column, ' <= CAST(''', variables('v_run_start'), ''' AS datetime2)')
     ```
4. Tab **Destination**:
   - Connection **Lakehouse anhnguyen** · Lakehouse `lh_platform` · Root folder **Files**.
   - File path — ô thư mục (dynamic):
     ```
     @concat('landing/', item().source_system, '/', item().entity, '/load_date=', pipeline().parameters.p_load_date, '/batch=', pipeline().RunId)
     ```
   - ô tên file (dynamic): `@concat(item().entity, '.parquet')`
   - File format **Parquet**.
5. **Ctrl+S** → Home → **Run** → `p_load_date = 2026-01-01`.
6. **Source control → Commit**.

## Kết quả mong đợi
- `lh_platform/Files/landing/wholesale/` có 4 thư mục: `customers`, `orders`, `products`, `sales_hierarchy` ✅ (đã chạy 2026-10-06).
- Tab Output → dòng `cp_db_to_landing` của orders → 👓 → **Rows written ≈ 41.940** (41.874 initial + 66 ngày 2026-01-01).
- Explorer không thấy file mới → bấm **…** cạnh Files → **Refresh**.

## Lưu ý
- Nút **Preview data** của Copy hỏi giá trị cho `item()`, `variables()` → **Cancel**, không cần dùng.
- Chưa có watermark → **mỗi lần Run đều lấy lại toàn bộ** vào batch mới. Đúng như thiết kế tạm thời; hết khi làm xong việc 5.
