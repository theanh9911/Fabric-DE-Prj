# 02 — Nguồn dữ liệu và simulator

Workspace `CompanyA-Source` giả lập hệ thống của các team khác. Platform **chỉ đọc** workspace này (L1). Chi tiết từng cột, kiểu, khoá, SLA, quality của từng nguồn nằm trong contract ([03-contract.md](03-contract.md)); file này mô tả hệ thống và cách nó sinh dữ liệu.

## 1. Thành phần

| Item | Loại | Vai trò | Platform đọc |
|---|---|---|---|
| `sqldb_erp_wholesale` | Fabric SQL Database | ERP bán buôn: `dbo.customers`, `dbo.products`, `dbo.sales_hierarchy`, `dbo.orders` | có (Copy activity qua Connection) |
| `lh_retail_drop` | Lakehouse | drop zone file: `Files/inbound/retail/{customers,products,sales_hierarchy,orders}/`, `Files/inbound/reference/categories/` | có (abfss) |
| `lh_sim` | Lakehouse | kho seed, đồng hồ ảo, log của simulator | chỉ `nb_test_e2e` đọc `sim_release_log` để đối chiếu |
| `nb_00_sim_common` | Notebook | cấu hình và hàm dùng chung của simulator | — |
| `nb_00_sim_setup` | Notebook | nạp 9 CSV gốc thành bảng seed | — |
| `nb_00_sim_daily` | Notebook | thả dữ liệu theo đồng hồ ảo | — |
| `nb_00_sim_reset` | Notebook | xoá dữ liệu nguồn để chạy lại từ đầu | — |
| `nb_00_sim_inject` | Notebook (**chưa có**, task T7.1) | tiêm sự cố nguồn có kiểm soát | — |

Code trong `fabric/source/`, nhánh `dev`.

## 2. Simulator

### 2.1 Seed

`nb_00_sim_setup` đọc 9 file gốc ở `lh_sim/Files/seed/` thành bảng `lh_sim.seed_<source>_<entity>` (mọi cột là chuỗi), thêm:
- `_row_no`: thứ tự dòng trong file gốc; khi trùng khoá, dòng đến trước thắng (giống PK của ERP).
- `_release_date`: ngày dòng "xuất hiện" ở nguồn. Orders: ngày của `updated_at`. Master: ngày lớn nhất trong các cột thời gian (`created_at`/`inserted_at`, `updated_at`). Không parse được hoặc sau `2026-12-31` ⇒ `1900-01-01` (thả ngay ở lần đầu, để platform tự phát hiện ngày sai).

| Seed | Nguồn | Đích |
|---|---|---|
| `categories.csv` | reference | file |
| `customers_wholesale.csv`, `products_wholesale.csv`, `sales_hierarchy_wholesale.csv`, `orders_wholesale.csv` | wholesale | ERP |
| `customers_retail.csv`, `products_retail.csv`, `sales_hierarchy_retail.csv`, `orders_retail.csv` | retail | file |

### 2.2 Đồng hồ ảo

`lh_sim.sim_state` giữ `released_until` (ngày giả lập hiện tại). Dữ liệu giữ **ngày gốc**; đồng hồ chỉ quyết định **khi nào** dòng được thả.

| Lần chạy `nb_00_sim_daily` | Cửa sổ thả |
|---|---|
| Lần đầu | mọi dòng có `_release_date` ≤ `2025-12-31` (cấu hình `initial_until`), gồm cả dòng `1900-01-01` |
| Các lần sau | `(released_until, released_until + p_days]`; hoặc tới đúng `p_sim_date` |

Không lùi đồng hồ được. Chạy lại khi lượt trước lỗi (state chưa tiến) cho cùng cửa sổ và không nhân đôi. Exit value = ngày giả lập mới, dùng làm `p_load_date` cho platform. Seed orders hết vào khoảng tháng 5/2026; sau đó simulator không sinh dữ liệu mới.

### 2.3 Cách ghi vào nguồn

| Đích | Cách ghi |
|---|---|
| ERP master | MERGE theo khoá (sửa tại chỗ; ERP không giữ lịch sử) |
| ERP orders | xoá rồi chèn các dòng có `updated_at` trong cửa sổ (một transaction) |
| File orders | lần đầu `orders_history_until_YYYYMMDD.csv`; sau đó một file mỗi ngày `orders_YYYYMMDD.csv` chỉ chứa dòng của ngày đó |
| File master | `<entity>_YYYYMMDD.csv` là **snapshot đầy đủ**, chỉ ghi khi có thay đổi trong cửa sổ |

ERP là "hybrid": cột thời gian ép `datetime2`, `quantity`/`price` ép số (không ép được ⇒ NULL); cột chữ giữ nguyên giá trị bẩn; khoá ngoại khai báo `NOCHECK`. Dòng vi phạm NOT NULL hoặc PK bị từ chối và ghi vào `lh_sim.sim_reject_log`. File giữ nguyên văn gốc: mọi giá trị là chữ, nhiều định dạng ngày, khoảng trắng thừa.

### 2.4 Log của simulator

| Bảng | Một dòng = | Dùng |
|---|---|---|
| `sim_release_log` | một lần thả một entity của một nguồn: cửa sổ, `rows_released`, `rows_rejected`, đích | đáp án đối chiếu trong `nb_test_e2e` (RC-05) |
| `sim_reject_log` | một dòng ERP từ chối: lý do, nội dung JSON | giải thích chênh lệch seed ↔ ERP |
| `sim_inject_log` | (T7.1) một lần tiêm sự cố: kịch bản, tham số, đối tượng bị tác động | đáp án cho kịch bản trong `nb_test_e2e` |

### 2.5 Reset

`nb_00_sim_reset` với `p_confirm = "RESET"`: xoá dữ liệu `dbo.*`, toàn bộ `lh_retail_drop/Files/inbound/`, `sim_state`, `sim_release_log`, `sim_reject_log`; giữ seed. Reset nguồn thì phải reset platform (Dev) — quy trình ở [07 §1.2](07-van-hanh-kiem-thu.md#12-reset-dev).

## 3. Tiêm sự cố nguồn — `nb_00_sim_inject` (đặc tả cho T7.1)

Notebook đóng vai team nguồn gây sự cố có kiểm soát, để diễn tập lặp lại được. Chạy sau `nb_00_sim_daily` của ngày giả lập hiện tại và trước platform.

Tham số: `p_scenario` (bắt buộc), `p_entity` (mặc định theo bảng), `p_confirm = "INJECT"`.

| `p_scenario` | Hành động | Mặc định `p_entity` |
|---|---|---|
| `file_resend` | ghi đè file orders của ngày hiện tại cùng tên, sửa `quantity` của 1 dòng (dòng `_row_no` nhỏ nhất) | `retail.orders` |
| `duplicate_file` | copy file orders của ngày hiện tại sang `orders_YYYYMMDD_copy.csv` | `retail.orders` |
| `missing_file` | xoá file của ngày hiện tại (sau khi đã ghi log nội dung) | `retail.orders` |
| `partial_file` | ghi lại file của ngày hiện tại với nửa số dòng đầu (giả lập đang upload); lần chạy thứ hai cùng tham số ghi đủ | `retail.orders` |
| `schema_change` | thêm cột `discount_amount` (giá trị `0`) vào file của ngày hiện tại | `retail.orders` |
| `master_delete` | xoá một mã (mã nhỏ nhất) khỏi snapshot mới nhất của entity: file ⇒ ghi snapshot mới ngày hiện tại thiếu mã đó; ERP ⇒ `DELETE` dòng đó | `retail.products` |
| `org_change` | đổi `salesmanager_code` của một salesman (mã nhỏ nhất có quản lý) sang quản lý khác | `wholesale.sales_hierarchy` |
| `bad_rows` | thêm 3 dòng vào file orders ngày hiện tại: `updated_at` rỗng, `order_status = 'Shiped'`, `quantity = 'abc'` | `retail.orders` |

Mỗi lần chạy ghi một dòng `lh_sim.sim_inject_log(sim_date, scenario, entity, detail_json, injected_at)`. Kết quả mong đợi phía platform ở [07 §5](07-van-hanh-kiem-thu.md#5-kịch-bản-sự-cố-nguồn).

Kịch bản có sẵn trong seed, không cần tiêm: **late arrival** (ngày 2026-01-15 có đơn `order_date = 2026-01-02`), **order update** (đơn đổi trạng thái qua nhiều ngày), **dữ liệu bẩn** (trạng thái sai chính tả, thuế dạng chữ, số âm, orphan `CUS099`/`PRD999`/`CAT010`/`CAT999`, ngày năm 2027).

## 4. Hai đồng hồ

| Đồng hồ | Gồm | Dùng cho |
|---|---|---|
| **Nguồn** | `updated_at`, `order_date`, `created_at` trong dữ liệu; `p_load_date`; đồng hồ ảo | cửa sổ trích xuất DB, ngày snapshot, `batch_id`, rule nghiệp vụ |
| **Hệ thống** (UTC thật) | `modifyTime` của file, thời điểm chạy, cột `*_at_utc` của audit | chọn file đã ổn định (settle), audit, SLA vận hành |

Không so sánh giá trị của hai đồng hồ với nhau trong điều kiện lọc dữ liệu. Ngoại lệ duy nhất có chủ đích: settle so `modifyTime` với thời điểm chạy — cả hai đều là đồng hồ hệ thống.
