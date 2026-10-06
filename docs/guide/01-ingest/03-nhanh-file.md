# 1.3 — Nhánh `file`: drop zone → landing ▶

## Mục tiêu
Với 5 entity dạng file (4 `retail` + `reference.categories`), chép **nguyên trạng** các file mới (LastModified trong `(watermark, run_start]`) từ `lh_retail_drop` sang landing.

## Hiểu trước khi làm
- Nguồn file đang có (ngày giả lập 2026-01-01):

  | Thư mục nguồn (`lh_retail_drop/Files/…`) | File |
  |---|---|
  | `inbound/retail/orders/` | `orders_history_until_20251231.csv` (initial) · `orders_20260101.csv` |
  | `inbound/retail/{customers,products,sales_hierarchy}/` | `<entity>_20251231.csv` — **snapshot đầy đủ** |
  | `inbound/reference/categories/` | `categories_20251231.csv` |

- Copy kiểu **Binary**: không đọc nội dung, chép nguyên byte → landing giữ đúng file gốc (kể cả file lỗi). Đọc/parse là việc của Bronze.
- Lọc theo **LastModified** thay vì tên file → không phụ thuộc quy ước đặt tên của team nguồn.
- `lh_retail_drop` nằm ở **ws khác** → chọn qua *Browse all*.

## Làm
1. Vào `fe_source_entity` ✏️ → chọn `sw_source_type` → tab Activities → dòng **file** ✏️.
2. Xoá `wait_todo_file` → **Copy data ⌄ → Add copy data activity** → General → Name `cp_file_to_landing` · Retry `2`.
3. Tab **Source**:
   - Connection **Lakehouse anhnguyen** → Lakehouse: dropdown → **Browse all** → `lh_retail_drop` (cột Location = `CompanyA-Source`) → Connect.
   - Root folder **Files**.
   - File path type **Wildcard file path**:
     - Wildcard folder path (dynamic): `@item().source_object`
     - Wildcard file name: `*`
   - **Filter by last modified**:
     - Start time (UTC) (dynamic): `@item().watermark_value`
     - End time (UTC) (dynamic): `@variables('v_run_start')`
   - Recursively: **bỏ tick**.
   - File format **Binary**.
4. Tab **Destination**:
   - Connection **Lakehouse anhnguyen** · Lakehouse `lh_platform` · Root folder **Files**.
   - File path — ô thư mục (dynamic), **giống hệt nhánh db**:
     ```
     @concat('landing/', item().source_system, '/', item().entity, '/load_date=', pipeline().parameters.p_load_date, '/batch=', pipeline().RunId)
     ```
   - ô tên file: **để trống** (giữ tên gốc).
   - File format **Binary**.
5. **Ctrl+S** → **Run** → `p_load_date = 2026-01-01`.
6. **Source control → Commit**.

## Kết quả mong đợi
- `Files/landing/retail/` có 4 thư mục entity; `Files/landing/reference/categories/`.
- `landing/retail/orders/load_date=2026-01-01/batch=…/` có **2 file csv** như bảng trên.
- `landing/wholesale/` có **thêm 1 batch** (chưa có watermark → lấy lại) — bình thường.
- Tab Output: 9 activity Copy đều *Succeeded*.

## Nếu lỗi
Chụp tab Output (dòng lỗi → biểu tượng 💬 *Error*) và tab Source của `cp_file_to_landing`.
