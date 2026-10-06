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
| `task_run` | Mỗi bước, mỗi lần thử | `nb_brz_load` (sau này runner) |
| `pipeline_run` | Mỗi lượt chạy pipeline | `nb_ops_run_end` |

**Bỏ** (đã tạo ở Bước 0): `cfg_pipeline_step`, `cfg_dq_rule`, `ref_order_status`, `ref_value_mapping`, `ref_holiday_vn`, `state_watermark`, `state_file_manifest`, `log_pipeline_run`, `log_task_run`, `dq_result_log`, `recon_result`, `schema_registry`. `dq_result`, `reconciliation_result` tạo lại ở Bước 3.

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

**C — Claude viết vào repo:**
1. `nb_setup_ddl` v4: cell xoá bảng bỏ (chỉ chạy được khi bảng tồn tại — `DROP TABLE IF EXISTS`), cell tạo 5 bảng trên.
2. `nb_setup_config` v4: 1 MERGE cho `cfg_source_entity` như bảng trên; bỏ các cell `ref_*`.
   (`nb_common` không đổi ở bước này; function `std_*` viết ở Bước 2.)

**B — Bạn trên Fabric (ws Dev):**
1. Đóng mọi tab notebook → Source control → **Update all**.
2. Xoá dữ liệu thử ở landing: mở `lh_platform` → Files → chuột phải `landing` → **Delete**.
3. Mở `nb_setup_ddl` → **Run all** → Stop session.
4. Mở `nb_setup_config` → **Run all** → Stop session.
5. Mở `lh_platform` → đổi sang **SQL analytics endpoint** → **Refresh** (để Lookup thấy bảng mới).

## Kết quả mong đợi
- `SHOW TABLES IN meta` → đúng **5 bảng**.
- `SELECT load_strategy, count(*) FROM meta.cfg_source_entity GROUP BY load_strategy` → `file_new_or_changed 5 · db_full_snapshot 3 · db_incremental 1`.
- `Files/` không còn `landing/`.
