# Source — từng bảng, từng cột

> Đọc sau [source.md](source.md). Mỗi bảng: **có để làm gì** → từng cột **nghĩa là gì, vì sao có**.
> Ký hiệu: 🔑 khoá · 🔗 trỏ sang bảng khác · ⚠️ chỗ dữ liệu bẩn / bẫy Platform phải xử lý.

---

## A. ERP `sqldb_erp_wholesale` — schema `dbo` (Platform đọc)

Mô phỏng hệ thống bán buôn. 4 bảng = mô hình bán hàng tối thiểu: **ai mua** (customers) · **mua gì** (products) · **ai bán** (sales_hierarchy) · **giao dịch** (orders).

### `dbo.customers` — khách hàng
| Cột | Kiểu | Nghĩa · vì sao có |
|---|---|---|
| 🔑 `customer_code` | NVARCHAR(20) NOT NULL | Mã khách — khoá nghiệp vụ, orders trỏ vào đây. ⚠️ cùng mã có thể có ở retail với thuộc tính khác → Silver phải gộp (survivorship) |
| `customer_name` | NVARCHAR(200) | Tên — hiển thị trên report |
| `country`, `city` | NVARCHAR(100) | Địa lý — report "doanh số theo quốc gia" (đề yêu cầu). ⚠️ có thể viết không thống nhất (kiểm ở Bước 2) |
| `gender` | NVARCHAR(20) | Giới tính — phân tích khách. ⚠️ nhiều cách viết → `std_gender()` trong `nb_common` (danh sách đúng: Bước 2) |
| `address` | NVARCHAR(400) | Địa chỉ |
| `created_at` | DATETIME2 | Lúc tạo khách ở ERP |
| `updated_at` | DATETIME2 | Lúc sửa gần nhất — **cột watermark**. ⚠️ cho phép NULL → dòng NULL không bao giờ được lấy incremental |

### `dbo.products` — sản phẩm
| Cột | Kiểu | Nghĩa · vì sao có |
|---|---|---|
| 🔑 `product_code` | NVARCHAR(20) NOT NULL | Mã sản phẩm |
| `product_name` | NVARCHAR(200) | Tên |
| `brand` | NVARCHAR(100) | Thương hiệu — report theo brand. ⚠️ viết khác nhau cho cùng 1 brand |
| 🔗 `category_code` | NVARCHAR(20) | Trỏ sang file `categories` (nằm ở nguồn khác!) → Gold ghép thành cây danh mục. ⚠️ `CAT010`, `CAT999` không tồn tại (orphan) |
| `created_at`, `updated_at` | DATETIME2 | như customers |

### `dbo.sales_hierarchy` — đội ngũ bán hàng (cây tổ chức)
| Cột | Kiểu | Nghĩa · vì sao có |
|---|---|---|
| 🔑 `salesman_code` | NVARCHAR(20) NOT NULL | Mã nhân viên bán |
| `salesman_name` | NVARCHAR(200) | Tên |
| `division` | NVARCHAR(100) | Khối/bộ phận |
| 🔗 `salesmanager_code` | NVARCHAR(20) | **Mã của người quản lý** — trỏ về chính bảng này (*self-reference*). Nối liên tiếp → cây: salesman → team lead → manager → director (Q7). ⚠️ có dòng tự trỏ vào chính mình |
| `salesmanager_name` | NVARCHAR(200) | Tên quản lý — **lặp lại** (phi chuẩn hoá) cho tiện tra cứu; có thể lệch với tên thật của người đó |
| `position` | NVARCHAR(50) | Chức danh (Salesman / Team Lead / Manager / Director). ⚠️ nhiều cách viết |
| `inserted_at`, `updated_at` | DATETIME2 | Lúc tạo / sửa. (Bảng này dùng tên `inserted_at` thay vì `created_at` — giữ nguyên như file gốc của đề) |

⚠️ **ERP chỉ giữ trạng thái hiện tại.** Đổi quản lý → dòng cũ bị ghi đè. Muốn report "tháng 3 ai thuộc team nào" (Q11) → Platform phải tự lưu lịch sử (SCD2).

### `dbo.orders` — dòng đơn hàng
| Cột | Kiểu | Nghĩa · vì sao có |
|---|---|---|
| 🔑 `order_line_id` | BIGINT IDENTITY | Số tự tăng do ERP sinh — **chỉ để làm PK trong DB**, dữ liệu gốc không có khoá duy nhất. Platform **không dùng** làm khoá nghiệp vụ |
| `order_no` | NVARCHAR(30) NOT NULL | Số đơn. 1 đơn có nhiều dòng (nhiều sản phẩm) |
| 🔗 `customer_code` | NVARCHAR(20) | Ai mua. ⚠️ `CUS099` không tồn tại |
| `order_date` | DATE | **Ngày nghiệp vụ** của đơn — report tính doanh thu theo ngày này |
| `order_status` | NVARCHAR(30) | Pending → Shipped → Delivered / Cancelled. **Chỉ Delivered tính doanh thu** (`is_sales_recognized()` trong `nb_common`). ⚠️ sai chính tả |
| 🔗 `product_code` | NVARCHAR(20) | Mua gì. ⚠️ `PRD999` không tồn tại |
| 🔗 `salesman_code` | NVARCHAR(20) | Ai bán → gắn doanh thu vào cây tổ chức |
| `quantity` | DECIMAL(18,2) | Số lượng. Để DECIMAL (không phải INT) vì ⚠️ dữ liệu có số lẻ, số âm — ERP giữ nguyên để Platform phát hiện |
| `price` | DECIMAL(18,2) | Đơn giá. Doanh thu = quantity × price |
| `tax_rate` | NVARCHAR(20) | Thuế suất. Để **chữ** vì ⚠️ có giá trị `five percent`, `5%`… — ép số thì mất bằng chứng |
| `created_at` | DATETIME2 | Lúc dòng được tạo |
| `updated_at` | DATETIME2 NOT NULL | Lúc dòng đổi gần nhất — **cột watermark** (có index). **Giờ giả lập** |

**Grain (1 dòng là gì):** 1 sản phẩm trong 1 đơn **ở 1 trạng thái**. Khi đơn đổi trạng thái, nguồn ghi **thêm dòng mới** (updated_at mới) chứ không sửa dòng cũ → cùng `order_no + product_code` xuất hiện nhiều lần = lịch sử trạng thái. Vì vậy Silver có 2 bảng: `slv_orders_history` (giữ mọi dòng) và `slv_orders_current` (chỉ trạng thái mới nhất).

**"Hybrid" (ADR 008):** cột thời gian và quantity/price đã ép kiểu (giá trị không ép được → NULL); cột chữ giữ nguyên bẩn; khoá ngoại 🔗 khai báo nhưng **không kiểm** (`NOCHECK`) → orphan vẫn vào. Lý do: nếu ERP chặn hết lỗi thì Platform không còn gì để xử lý (Q1, Q5).

### Schema `sim.stg_*` (Platform **không** đọc)
`stg_customers`, `stg_products`, `stg_sales_hierarchy`, `stg_orders`: bảng tạm, cùng cột với `dbo.*` (trừ `order_line_id`). Simulator ghi batch vào đây trước rồi `MERGE`/`INSERT` sang `dbo.*` trong 1 transaction → lỗi thì không để lại dữ liệu dở dang.

---

## B. Drop zone `lh_retail_drop/Files/inbound/` (Platform đọc)

File CSV, có dòng tiêu đề, **mọi giá trị là chữ**, giữ nguyên định dạng gốc.

| Thư mục | File | Cột |
|---|---|---|
| `retail/customers/` | `customers_YYYYMMDD.csv` (snapshot đầy đủ) | cùng ý nghĩa với `dbo.customers` |
| `retail/products/` | `products_YYYYMMDD.csv` (snapshot) | cùng ý nghĩa với `dbo.products` |
| `retail/sales_hierarchy/` | `sales_hierarchy_YYYYMMDD.csv` (snapshot) | cùng ý nghĩa với `dbo.sales_hierarchy` |
| `retail/orders/` | `orders_history_until_20251231.csv`, rồi `orders_YYYYMMDD.csv` mỗi ngày | cùng ý nghĩa với `dbo.orders` (không có `order_line_id`) |
| `reference/categories/` | `categories_YYYYMMDD.csv` (snapshot) | `category_code` 🔑 + tên/đường dẫn danh mục (vd `Electronics/AllInOne`) → Gold tách thành cấp 1–4 |

Khác ERP: ngày nhiều định dạng (`3/15/2025 9:30`, `25/3/2025`…), không múi giờ (coi là giờ VN), số có thể là chữ, `"NULL"` dạng chữ. ⚠️ `CAT005` có 2 dòng (`electronics/All-in-One` vs `Electronics/AllInOne`).

> **Chưa xác minh:** tên cột chính xác của từng file retail/categories (có thể lệch nhẹ so với ERP, vd `created_at` vs `inserted_at`). Bước 2 (EDA trên Bronze) sẽ liệt kê đúng và cập nhật bảng này.

**Vì sao `categories` là file `reference` chứ không nằm trong ERP:** đề chỉ liệt kê 4 bảng ERP; `categories.csv` không có hậu tố nguồn → danh mục dùng chung. Nếu đặt trong ERP, PK sẽ chặn mất dòng trùng `CAT005` — đó lại là lỗi Platform cần tự phát hiện.

---

## C. Hậu trường `lh_sim` (Platform **không** đọc)

### `seed_<source>_<entity>` (9 bảng)
Bản sao 9 CSV gốc của đề — "toàn bộ tương lai" đã biết trước.
| Cột | Nghĩa · vì sao có |
|---|---|
| *(các cột nghiệp vụ)* | Giữ nguyên dạng chữ như file gốc |
| `_row_no` | Số thứ tự dòng trong file gốc → giữ đúng thứ tự khi ghi lại file; dòng trùng khoá thì dòng đến trước thắng |
| `_release_date` | **Ngày dòng xuất hiện ở nguồn** theo virtual clock: orders = ngày của `updated_at`; master = ngày mới nhất của created/inserted/updated. Ngày không đọc được hoặc sau 2026-12-31 → `1900-01-01` (thả ngay lần đầu) |

### `sim_state` — virtual clock
| Cột | Nghĩa |
|---|---|
| `state_key` | luôn = `released_until` |
| `state_value` | ngày giả lập hiện tại (`2026-01-01`) |
| `sim_run_id` | lần chạy đã đặt giá trị này |
| `updated_at` | giờ thật lúc đặt |

### `sim_release_log` — mỗi lần thả bao nhiêu
| Cột | Nghĩa · vì sao có |
|---|---|
| `sim_run_id` | id lần chạy (`sim_<ngày>_<ngẫu nhiên>`) |
| `source_system`, `entity` | nguồn nào, bảng nào |
| `window_from`, `window_to` | cửa sổ ngày giả lập được thả |
| `rows_released` | số dòng đã thả → **đối chiếu với Bronze** (recon Source ↔ Platform) |
| `rows_rejected` | số dòng ERP từ chối |
| `target` | thả vào đâu (bảng ERP / thư mục file) |
| `logged_at` | giờ thật |

### `sim_reject_log` — dòng ERP từ chối
`sim_run_id`, `source_system`, `entity`, `window_from`, `window_to`, `reason` (vd `NOT NULL violated: order_no`, `PK violated: duplicate key`), `row_json` (nguyên dòng bị từ chối), `logged_at`. Hiện = 0 dòng. Có để: DB thật từ chối dữ liệu vi phạm ràng buộc → phải có dấu vết, không mất im lặng.
