# EDA — Khảo sát dữ liệu nguồn (T0.3)

Làm trực tiếp trên nguồn, chỉ đọc (D38): file CSV ở drop zone `lh_retail_drop/Files/inbound/**` và bảng ERP (Delta của item `sqldb_erp_wholesale` trên OneLake). Không cần dữ liệu platform. Mỗi mục: truy vấn đã chạy, kết quả rút gọn, kết luận, giá trị điền vào. Kết quả dùng ở T1.2 (contract) và T2.1 (từ điển trong `nb_config`).

Điều kiện: nguồn đã có dữ liệu (initial load xong). Chạy bằng `nb_eda` (tạm, xoá ở T8.4); notebook tìm workspace và item theo tên, không cần ID.

## Chuẩn bị (cell đầu `nb_eda`)

```python
import sempy.fabric as fabric
from pyspark.sql import functions as F

WS = fabric.resolve_workspace_id("CompanyA-Source")
DROP = fabric.resolve_item_id("lh_retail_drop", "Lakehouse", WS)
ERP = fabric.resolve_item_id("sqldb_erp_wholesale", "SQLDatabase", WS)
ONELAKE = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com"

def csv_of(source, entity):          # mỗi file đọc riêng rồi unionByName: header có thể khác nhau
    files = [f.path for f in notebookutils.fs.ls(f"{ONELAKE}/{DROP}/Files/inbound/{source}/{entity}")]
    ...

def erp_of(entity):
    return spark.read.format("delta").load(f"{ONELAKE}/{ERP}/Tables/dbo/{entity}")

def shape(c):
    return F.regexp_replace(F.regexp_replace(F.col(c), "[0-9]", "9"), "[A-Za-z]", "a")
```
Mã đầy đủ ở `nb_eda`. Nếu `Tables/dbo/<bảng>` của item SQL Database không đọc được bằng đường dẫn OneLake thì ghi lại cách đọc thay thế vào [DECISIONS §3](DECISIONS.md#3-kết-quả-spike).
## Checklist

| # | Câu hỏi | Cách xem | Điền vào |
|---|---|---|---|
| E01 | Tên cột chính xác của mỗi file retail và categories; có đổi giữa các file không | `df.columns` theo từng file | contract `properties[].name`; `source_by_system` trong `SILVER` nếu khác ERP |
| E02 | Mẫu định dạng `updated_at`, `created_at`, `inserted_at` ở file | `groupBy(shape(c)).count()` | `timestampFormats` của contract retail |
| E03 | Mẫu định dạng `order_date` ở file; số dòng mơ hồ (`a/b/yyyy`, cả hai ≤ 12, khác nhau) | như E02 + đếm | `dateFormats`; thứ tự M/d hay d/M |
| E04 | Có dòng chắc chắn d/M (số đầu > 12) hay chắc chắn M/d (số giữa > 12); trong cùng file có trộn không | đếm theo `_file` | thứ tự `dateFormats` |
| E05 | Múi giờ `updated_at` ERP: phân bố giờ trong ngày; so với `created_at` mặc định `sysutcdatetime()` | `groupBy(hour(updated_at))` | `sourceTimezone` của contract wholesale |
| E06 | Mọi biến thể `order_status` (sau trim) và số dòng, cả hai nguồn | `groupBy(order_status)` | `ORDER_STATUS`; danh sách trạng thái chuẩn (cột `first_*` của fact) |
| E07 | Có hai trạng thái khác nhau cùng `(order_no, product_code, updated_at)` không; cặp nào | `groupBy` khoá having count > 1 | xác nhận `STATUS_RANK` |
| E08 | Event key có duy nhất không; trùng thì nội dung có khác không | `groupBy` event key, `countDistinct` hash cột còn lại | xác nhận R-ORD-02/03 |
| E09 | `order_no` có xuất hiện ở cả hai nguồn không | join distinct `order_no` | xác nhận D20 |
| E10 | Biến thể `tax_rate` | `groupBy(trim(tax_rate))` | `TAX_WORDS`; R-STD-06 |
| E11 | `quantity`, `price`: không parse được, âm, thập phân, có dấu `,` | đếm theo loại | quality của contract; R-STD-07 |
| E12 | `order_date` sau 2026-12-31 | đếm, ví dụ | xác nhận D25 |
| E13 | Biến thể `gender`, `position`, `brand`, `country`, `city`, `division` | `groupBy` từng cột | `GENDER`, `POSITION`, `BRAND` |
| E14 | Giá trị `NULL`, `N/A`, rỗng dạng chữ ở từng cột | đếm | R-STD-01 |
| E15 | Trùng khoá trong cùng snapshot (CAT005, …) | `groupBy(_file, key)` having count > 1 | quality của contract; R-MST-01 |
| E16 | `salesmanager_code` tự trỏ; vòng 2+ cấp; trỏ mã không tồn tại | self-join | quality contract sales_hierarchy |
| E17 | Cùng `customer_code`/`product_code` ở hai nguồn: lệch ở cột nào, bao nhiêu | join hai nguồn | xác nhận D22 |
| E18 | Orphan: mã khách, sản phẩm, salesman trong orders không có trong master, theo nguồn | anti-join | kỳ vọng DQ-GLD-07 |
| E19 | `category_code` của products không có trong categories | anti-join | xác nhận R-PRD-02 |
| E20 | Số sự kiện mỗi dòng hàng; thứ tự trạng thái có đi lùi theo thời gian không | window theo `updated_at` | ghi chú cho lớp KPI sau này |

## Kết quả

Chạy ngày 2026-10-15 bằng `nb_eda` trên nguồn sau initial load tới 2025-12-31 (wholesale orders 41.874 dòng, retail orders 42.194).

| # | Kết quả | Kết luận, giá trị đã điền |
|---|---|---|
| E01 | Cột retail khớp wholesale (orders không có `order_line_id`). Master giống nhau giữa hai nguồn; `sales_hierarchy` dùng `inserted_at`. Categories: `category_code, category_lvl1..category_lvl4, created_at, updated_at` (không có `category_path`) | Contract categories đổi sang bốn cột cấp; `slv.categories_history` và R-PRD-02 sửa theo (04 §5.4, §6.3) |
| E02 | `created_at`, `updated_at` của orders ở cả hai nguồn chỉ có dạng `yyyy-MM-dd HH:mm:ss` | `timestampFormats` retail.orders = `["yyyy-MM-dd HH:mm:ss"]`. Master: xem E02b |
| E03 | Retail `order_date`: 42.162 dòng `yyyy-MM-dd`; 32 dòng `d/M/yyyy` (22 chắc chắn d/M vì số đầu > 12, 9 mơ hồ, 1 hai số bằng nhau). Wholesale chỉ `yyyy-MM-dd` | `dateFormats` retail.orders = `["yyyy-MM-dd", "d/M/yyyy"]`; 9 dòng mơ hồ được FLAG `ambiguous_date` (D31) |
| E04 | Không có dòng M/d chắc chắn nào (0); không file nào trộn hai thứ tự | thứ tự `d/M` là đúng cho nguồn này; mỗi delivery chỉ một file nên chưa kiểm được trộn theo file |
| E05 | Giờ phân bố đều 0–23 (dữ liệu giả lập), không suy ra được múi giờ. `updated_at` có giá trị đến 2027-12-24 | Múi giờ lấy từ DDL: ERP `DEFAULT sysutcdatetime()` ⇒ `UTC` (D41). Dòng `updated_at` ở tương lai chỉ vào Landing khi `p_load_date` tới ngày đó (cửa sổ `virtual`) |
| E06 | Ngoài 5 trạng thái đã biết còn `Returned` (retail 871, wholesale 916). Biến thể: `Confirmd`, `Pendng`, `Deliverd`, `Shippd`, `Cancel`, `Return` và các kiểu hoa/thường | thêm `Returned` làm trạng thái thứ sáu (D39); `ORDER_STATUS` ở 05 §2.2 |
| E07 | Retail 0; wholesale 25 khoá có hai trạng thái cùng `(order_no, product_code, updated_at)` | `STATUS_RANK` phân thắng bại (R-ORD-04) đúng như thiết kế |
| E08 | Không có event key trùng ở cả hai nguồn | R-ORD-02/03 vẫn giữ cho file gửi lại; chưa có dữ liệu thật để kiểm |
| E09 | 80 `order_no` có ở cả hai nguồn (retail 5.470, wholesale 5.407) | xác nhận D20: grain gồm `source_system` |
| E10 | `10%`, `8%`, `5%`, `15%`, `5`, `0.05`, `0.10`, `0.1`, `five percent`, NULL | R-STD-06 đủ; `TAX_WORDS = {"FIVEPERCENT": "0.05"}` |
| E11 | `quantity`: số âm 50 (retail) và 40 (wholesale); retail có 23 dòng thập phân. `price`: số âm 44 và 33. Không có dấu `,`, không có giá trị không parse được | số âm hiện không bị REJECT (chỉ cảnh báo contract); xem câu hỏi mở ở DECISIONS D42. R-STD-07 giữ nhưng chưa có dữ liệu cần |
| E12 | `order_date` năm 2027: 37 (retail), 42 (wholesale) | xác nhận D25 (REJECT `future_order_date`) |
| E13 | gender: `F, M, Female, Male, Mael, m, female, UNKNOWN, X, NULL`; position: `Salesman, Sale, Salseman, SALESMAN, Team Lead, TEAM LEAD, Team lead, Manager, Director`; brand: ~25 giá trị có biến thể hoa/thường; country/city/division có nhiều biến thể (`USA`, `U.S.A.`, `United States`; `HCM`, `Ho Chi Minh City`, `HoChiMinh`; `north`, `Northern`) | `GENDER`, `POSITION`, `BRAND` ở 05 §2.2; `X` chưa map ⇒ FLAG `unmapped_value`. Country, city, division **không** chuẩn hoá (ngoài phạm vi, ghi D42) |
| E14 | Dạng chữ: `UNKNOWN` (gender), một `category_code` và một `category_lvl2` dạng chữ; còn lại là NULL thật | `null_if_blank` đủ (R-STD-01) |
| E15 | Chỉ `CAT005` trùng khoá trong `categories_20251231.csv` | R-MST-01; DQ-SLV-03 |
| E16 | Retail: 1 salesman tự trỏ; wholesale: 1 manager không tồn tại; không có vòng dài hơn | contract `sales_hierarchy` đã có check tương ứng |
| E17 | Customers: 6 khoá chung, lệch ở country 1, city 1, gender 2. Products: 22 khoá chung, lệch ở brand 4, category_code 2, product_name 1. Sales hierarchy: 0 khoá chung | `source_priority = ["wholesale", "retail"]` (D22) đúng cho từng thuộc tính |
| E18 | Mồ côi: `CUS099` (retail 50, wholesale 33); `PRD021` (retail 689, wholesale 1.723) và `PRD999` (43, 27); không có salesman mồ côi | `PRD021` chiếm ~4% dòng wholesale ⇒ DQ-GLD-07 (ngưỡng 1%) sẽ WARN; đúng kỳ vọng, không phải lỗi |
| E19 | `category_code` không có trong categories: `CAT010`, `CAT099` (cả hai nguồn), `CAT999` (retail), NULL (wholesale) | R-PRD-02 ⇒ `Unknown (pending)` |
| E20 | Phần lớn dòng hàng có 4 sự kiện (tối đa 16). 147 dòng hàng retail và 267 wholesale có bước lùi trạng thái (ví dụ Delivered rồi Pending) | trạng thái hiện hành lấy theo `updated_at` mới nhất (R-ORD-04); mốc `first_*` vẫn là lần đầu; ghi nhận cho lớp KPI |

Chưa xong: E02b (định dạng thời gian của file master) cần một lượt chạy ngắn; contract master retail còn danh sách format rộng cho tới khi có kết quả.
