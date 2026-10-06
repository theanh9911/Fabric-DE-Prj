# 1.5c — Nhánh `file`: drop zone → landing

## Mục tiêu
Chép **nguyên bản** các file có thời điểm sửa trong cửa sổ `[window_start, window_end)` từ `lh_retail_drop` sang landing.

## Hiểu trước khi làm
- **Binary copy**: không đọc nội dung → landing giữ đúng file gốc (kể cả file lỗi). Parse là việc của Bronze.
- **Lọc theo LastModified** (giờ thật lúc team nguồn ghi file), không theo tên file.
- `window_end = run_start − 5 phút`: bỏ qua file có thể đang ghi dở; lượt sau sẽ lấy.
- File đang có ở nguồn (ngày giả lập 2026-01-01):

  | Thư mục nguồn | File |
  |---|---|
  | `inbound/retail/orders/` | `orders_history_until_20251231.csv`, `orders_20260101.csv` |
  | `inbound/retail/{customers,products,sales_hierarchy}/` | `<entity>_20251231.csv` (snapshot) |
  | `inbound/reference/categories/` | `categories_20251231.csv` |

## Làm (B)
Vào `fe_source_entity` ✏️ → `sw_source_type` → case **file** ✏️ → xoá `wait_todo_file` → **Copy data → Add copy data activity**:

1. **General:** Name `cp_file_to_landing` · Retry `2`.
2. **Source:**
   - Connection **Lakehouse anhnguyen** → Lakehouse: **Browse all** → `lh_retail_drop` (Location = `CompanyA-Source`) → Connect.
   - Root folder **Files** · File path type **Wildcard file path**:
     - Wildcard folder path (dynamic): `@item().source_object`
     - Wildcard file name: `*`
   - **Filter by last modified:** Start time (dynamic) `@item().window_start` · End time (dynamic) `@item().window_end`.
   - Recursively: **bỏ tick** · File format **Binary**.
3. **Destination:** Connection **Lakehouse anhnguyen** · `lh_platform` · Root folder **Files** · thư mục (dynamic) — **giống nhánh db**:
   ```
   @concat('landing/', item().source_system, '/', item().entity, '/load_date=', pipeline().parameters.p_load_date, '/batch=', pipeline().RunId)
   ```
   Tên file: **để trống** (giữ tên gốc) · File format **Binary**.
4. **Ctrl+S** → **Commit**.

## Kết quả mong đợi (sau Run ở file 06)
- `landing/retail/orders/load_date=2026-01-01/batch=…/` có 2 file csv.
- `landing/retail/{customers,products,sales_hierarchy}/…` và `landing/reference/categories/…` mỗi nơi 1 file.

## Còn mở
- Định dạng thời gian `yyyy-MM-dd HH:mm:ss` có được bộ lọc LastModified chấp nhận không — nếu lỗi, đổi `CONVERT(…, 126)` trong Lookup (dạng ISO `yyyy-MM-ddTHH:mm:ss`).
