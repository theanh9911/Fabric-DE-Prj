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
| Thời gian | `updated_at` theo **đồng hồ của ERP** = virtual clock trong mô phỏng; không lưu múi giờ. Ở ngày giả lập D, mọi dòng ERP có `updated_at < D + 1 ngày` (trừ dòng lỗi năm 2027) — đây là cận trên Platform dùng. Múi giờ (UTC hay VN) chỉ ảnh hưởng việc đổi sang UTC ở Silver — **chốt ở EDA** |
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
| Gửi lại | Gửi lại = **ghi đè cùng tên**. Storage gán LastModified = thời điểm ghi; nguồn **không** giữ thời gian sửa cũ → file gửi lại luôn có LastModified mới. File đến với LastModified cũ hơn 1 ngày là **vi phạm hợp đồng** (Platform không đảm bảo nhận) |
| Hoàn tất | File được ghi 1 lần (atomic rename trong simulator); hệ thật có thể đang ghi dở → Platform chờ 5 phút sau LastModified (settle) |
| Thời gian | LastModified là **giờ thật UTC** của storage (khác đồng hồ dữ liệu bên trong file) |
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

## 4. Hai đồng hồ — đừng trộn

| Đồng hồ | Gồm | Dùng cho |
|---|---|---|
| **Đồng hồ nguồn** (virtual clock) | `updated_at`, `order_date`… bên trong dữ liệu; `p_load_date` | Cửa sổ ERP incremental, watermark ERP, ngày nghiệp vụ |
| **Giờ thật UTC** | LastModified của file; `run_start`; `*_at_utc` trong log | Cửa sổ file, watermark file, log vận hành |

Mọi điều kiện so sánh chỉ dùng giá trị **cùng một đồng hồ**.

## Quyết định
- Master ERP: full snapshot mỗi ngày. Orders ERP: incremental theo `updated_at` + lookback; cận trên = cuối ngày `p_load_date` (đồng hồ nguồn).
- File: chép nguyên bản; cửa sổ theo LastModified (giờ thật) + lookback 1 ngày; nhận file trùng bằng **hash nội dung**.

## Còn mở
- **Múi giờ của `updated_at` ERP** (chỉ ảnh hưởng Silver): DDL ERP mặc định `sysutcdatetime()` gợi ý UTC, nhưng giá trị seed là giờ gốc của file. EDA (Bước 2) xem phân bố giờ trong ngày rồi chốt.
- Khoá sự kiện orders có duy nhất không (`order_no, product_code, order_status, updated_at`) — EDA.
- Tên cột chính xác của từng file retail/categories — EDA.
