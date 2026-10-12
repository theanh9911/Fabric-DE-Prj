# 1.2–1.4 — Bảng meta v4, config v4, dọn Dev

## Mục tiêu
`meta` chỉ còn đúng các bảng Bước 1 cần; `cfg_source_entity` mô tả 3 chiến lược ingest; Dev sạch dữ liệu thử.

## Hiểu trước khi làm

**Bảng meta sau bước này** (chi tiết cột: [design/04](../../design/04-control-tables.md)):

| Bảng | Vai trò | Ai ghi |
|---|---|---|
| `cfg_source_entity` | Danh sách nguồn + chiến lược | `nb_setup_config` |
| `watermark_state` | Mốc hiện hành của mỗi entity (1 dòng / entity) | `nb_brz_load` |
| `ingestion_batch` | Mỗi lần ingest 1 entity / 1 file: cửa sổ, số dòng, trạng thái | `nb_brz_load` |

Trạng thái pipeline/activity, thời lượng, lỗi, Copy output: xem ở **Monitoring hub** — không tự ghi lại ([design/04 §1](../../design/04-control-tables.md)).

**Migration v4:** bảng legacy được dọn một lần trong Dev; `cfg_source_entity` được nạp lại từ Git. Không chạy cell DROP lặp lại ở mỗi lần setup hoặc promote Prod. `dq_result`, `reconciliation_result` chỉ tạo ở Bước 3.

**`cfg_source_entity` v4** (9 dòng):

| source_system | entity | load_strategy | source_object | watermark_column | lookback_min | settle_min | source_timezone | business_keys | load_order |
|---|---|---|---|---|---|---|---|---|---|
| reference | categories | file_new_or_changed | `inbound/reference/categories` | | 1440 | 5 | Asia/Ho_Chi_Minh | category_code | 10 |
| wholesale | customers | db_full_snapshot | `dbo.customers` | | | | *(EDA chốt)* | customer_code | 20 |
| wholesale | products | db_full_snapshot | `dbo.products` | | | | *(EDA chốt)* | product_code | 20 |
| wholesale | sales_hierarchy | db_full_snapshot | `dbo.sales_hierarchy` | | | | *(EDA chốt)* | salesman_code | 20 |
| retail | customers | file_new_or_changed | `inbound/retail/customers` | | 1440 | 5 | Asia/Ho_Chi_Minh | customer_code | 20 |
| retail | products | file_new_or_changed | `inbound/retail/products` | | 1440 | 5 | Asia/Ho_Chi_Minh | product_code | 20 |
| retail | sales_hierarchy | file_new_or_changed | `inbound/retail/sales_hierarchy` | | 1440 | 5 | Asia/Ho_Chi_Minh | salesman_code | 20 |
| wholesale | orders | db_incremental | `dbo.orders` | updated_at | 1440 | | *(EDA chốt)* | order_no,product_code | 50 |
| retail | orders | file_new_or_changed | `inbound/retail/orders` | | 1440 | 5 | Asia/Ho_Chi_Minh | order_no,product_code | 50 |

## Làm

**Docs chốt trước khi implementation:**
1. `nb_setup_ddl` v4: DDL rerunnable cho `watermark_state`, `ingestion_batch`; migration `cfg_source_entity` và bảng legacy phải có guard hoặc được tách thành thao tác one-time, không xóa config mỗi lần chạy.
2. `nb_setup_config` v4: 1 MERGE cho `cfg_source_entity` như bảng trên; bỏ các cell `ref_*`.
   (`nb_common` không đổi ở bước này; function `std_*` viết ở Bước 2.)

**Thao tác Fabric sau khi implementation được duyệt/chốt:**
1. Đóng mọi tab notebook → Source control → **Update all**.
2. Trên Dev, sao lưu/ghi nhận bảng và landing cần giữ trước migration; chỉ xóa dữ liệu thử đã xác nhận là disposable. Không xóa landing có batch đã commit hoặc batch đang PREPARED/BRONZE_WRITTEN.
3. Mở `nb_setup_ddl` → **Run all** → Stop session.
4. Mở `nb_setup_config` → **Run all** → Stop session.
5. Mở `lh_platform` → đổi sang **SQL analytics endpoint** → **Refresh** (để Lookup thấy bảng mới).

## Kết quả mong đợi
- `SHOW TABLES IN meta` → đúng **3 bảng**.
- `SELECT load_strategy, count(*) FROM meta.cfg_source_entity GROUP BY load_strategy` → `file_new_or_changed 5 · db_full_snapshot 3 · db_incremental 1`.
- Landing đã commit hoặc đang recovery vẫn còn nguyên; chỉ dữ liệu thử đã xác nhận disposable mới được dọn.
