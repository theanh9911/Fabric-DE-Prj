# 01 — Source contract

> "Hợp đồng" giữa team nguồn và Platform: Platform được phép giả định gì về từng entity. Ingest, Silver, DQ đều dựa vào đây. Ở dự án thật, đây là tài liệu phải chốt với team nguồn **trước** khi viết pipeline.

## 1. Hệ thống nguồn

| source_system | Hệ thống | Cách truy cập | Định dạng |
|---|---|---|---|
| `wholesale` | ERP — Fabric SQL Database `sqldb_erp_wholesale` | Connection (Entra ID), T-SQL query | Bảng quan hệ, có kiểu |
| `retail` | Hệ thống bán lẻ — gửi file | Drop zone `lh_retail_drop/Files/inbound/retail/<entity>/` | CSV, header, UTF-8, mọi giá trị là chữ |
| `reference` | Danh mục dùng chung | `…/inbound/reference/categories/` | CSV |

## 2. Từng entity

### orders (wholesale — ERP `dbo.orders`)
| Mục | Hợp đồng |
|---|---|
| Grain | 1 dòng = 1 sản phẩm trong 1 đơn **ở 1 trạng thái** (đổi trạng thái → dòng mới) |
| Khoá kỹ thuật | `order_line_id` (IDENTITY, chỉ trong ERP) |
| Khoá nghiệp vụ | `order_no + product_code` (dòng hiện hành); `+ order_status + updated_at` (sự kiện) — **xác minh ở EDA** |
| Cập nhật | Thêm dòng mới; dòng cũ không sửa (theo seed) |
| Xoá | Không hard delete trong vận hành bình thường |
| Cột thay đổi | `updated_at` NOT NULL, có index → dùng làm watermark |
| Thời gian | `updated_at` là **giờ giả lập** (virtual clock), không có múi giờ — **múi giờ: còn mở** |
| Tần suất | Liên tục; ~62 dòng / ngày giả lập |
| Lỗi đã biết | status sai chính tả · qty âm/lẻ · tax dạng chữ · orphan `CUS099`/`PRD999` · `updated_at` năm 2027 |

### customers / products / sales_hierarchy (wholesale — ERP)
| Mục | Hợp đồng |
|---|---|
| Grain | 1 dòng = 1 đối tượng, **trạng thái hiện tại** |
| Khoá | `customer_code` / `product_code` / `salesman_code` (PK) |
| Cập nhật | **Ghi đè tại chỗ** (MERGE) — ERP **không giữ lịch sử** |
| Xoá | Không có trong seed; hệ thật có thể xoá → chỉ phát hiện được bằng so snapshot |
| Cột thay đổi | `updated_at` **cho phép NULL** → không tin cậy làm watermark |
| Kích thước | vài chục dòng |
| → Chiến lược | **Full snapshot mỗi ngày** (đủ, phát hiện được xoá, có lịch sử cho SCD2) |

### orders (retail — file)
| Mục | Hợp đồng |
|---|---|
| File | Lần đầu `orders_history_until_YYYYMMDD.csv`; sau đó **1 file / ngày** `orders_YYYYMMDD.csv` (dòng có `updated_at` trong ngày) |
| Grain, khoá | như wholesale orders (không có `order_line_id`) |
| Gửi lại | Có thể gửi lại cùng tên (ghi đè) → phải nhận ra là **phiên bản mới của file** |
| Hoàn tất | File được ghi 1 lần (atomic rename trong simulator); hệ thật có thể đang ghi dở → cần biên an toàn |
| Thời gian | Không múi giờ → coi là **Asia/Ho_Chi_Minh** |

### customers / products / sales_hierarchy (retail) · categories (reference)
| Mục | Hợp đồng |
|---|---|
| File | `<entity>_YYYYMMDD.csv` — **snapshot đầy đủ**, chỉ gửi khi có thay đổi |
| Ý nghĩa vắng mặt | Dòng không còn trong snapshot mới = **đã bị xoá** ở nguồn |
| Lỗi đã biết | `CAT005` 2 phiên bản · cùng khách ở 2 nguồn khác thuộc tính · `CAT010`/`CAT999` orphan |

## 3. Quy ước chung
- Encoding UTF-8; dấu phân cách `,`; giá trị có `"` được escape kiểu RFC 4180.
- Chuỗi rỗng và chữ `NULL` đều có thể xuất hiện, nghĩa là "không có giá trị".
- Platform **chỉ đọc**; không bao giờ ghi vào nguồn.

## Quyết định
- Master ERP: full snapshot mỗi ngày. Orders ERP: incremental theo `updated_at` + lookback.
- File: chép nguyên bản, nhận diện file bằng `(path, size, modified_at)`; checksum để giai đoạn sau.

## Còn mở
- **Múi giờ của `updated_at` ERP:** DDL ERP mặc định `sysutcdatetime()` (gợi ý UTC), nhưng giá trị seed là giờ gốc của file. EDA (Bước 2) xem phân bố giờ trong ngày rồi chốt.
- Khoá sự kiện orders có duy nhất không (`order_no, product_code, order_status, updated_at`) — EDA.
- Tên cột chính xác của từng file retail/categories — EDA.
