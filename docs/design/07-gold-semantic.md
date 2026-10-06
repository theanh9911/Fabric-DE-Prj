# 07 — Gold & semantic model

> Chốt **grain** trước khi viết MERGE. Gold phục vụ đúng câu hỏi nghiệp vụ: doanh thu đã giao theo sản phẩm, khách, quốc gia, cây bán hàng.

## 1. Fact

| Bảng | Grain | Nguồn | Lọc |
|---|---|---|---|
| `gld_f_sales_line` | 1 **dòng sản phẩm của đơn**, trạng thái hiện hành | `slv_orders_current` | `is_sales_recognized(order_status)` |
| `gld_a_sales_month` | tháng × product × customer × salesman | `gld_f_sales_line` | |

Không chọn grain "1 dòng trạng thái của đơn" cho fact doanh thu: đếm đôi khi đơn qua nhiều trạng thái. Lịch sử trạng thái đã có ở `slv_orders_history` nếu cần phân tích vòng đời.

Cột đo: `quantity`, `price`, `gross_amount = quantity × price`, `tax_rate`, `tax_amount`, `net_amount`. Khoá: `order_no, product_code`; FK: `date_key`, `customer_sk`, `product_sk`, `salesman_sk`.

Load: MERGE các đơn có thay đổi trong batch; đơn rời trạng thái được ghi nhận (vd Delivered → trả hàng) → xoá khỏi fact. Aggregate: xoá-chèn các tháng bị chạm.

## 2. Dimension

| Bảng | Grain | Key | Kiểu | Lý do |
|---|---|---|---|---|
| `gld_d_date` | 1 ngày | `date_key` YYYYMMDD | sinh (PySpark — Q9) | ngày lễ VN khai báo trong notebook; năm tài chính từ tháng 7 |
| `gld_d_customer` | 1 khách | `xxhash64(customer_code)` | SCD1 | đề không yêu cầu lịch sử khách |
| `gld_d_product` | 1 sản phẩm + category lvl1–4 | `xxhash64(product_code)` | SCD1 | |
| `gld_d_salesman` | 1 **phiên bản** salesman | `xxhash64(source, code, valid_from)` | **SCD2** | report phải đúng cơ cấu tổ chức tại thời điểm bán (Q11) |

Unknown member `-1` ở mọi dim → fact không có FK NULL; orphan được flag ở Silver.

## 3. SCD2 `gld_d_salesman`
- Nguồn: chuỗi snapshot `sales_hierarchy` ở Bronze (ERP full snapshot + file snapshot).
- Thay đổi = `_record_hash` (manager, position, division, name) khác bản `_is_current`.
- **Effective date:** nguồn chỉ giữ hiện tại → dùng **ngày Platform quan sát được** (`_load_date` của snapshot) làm `valid_from`. Nếu `updated_at` của dòng đáng tin (không NULL, ≤ ngày snapshot) thì dùng `updated_at` — chốt khi làm Bước 5, ghi rõ trong notebook.
- Flatten: salesman → team lead → manager → director **tại phiên bản đó**.
- Fact join **point-in-time**: `order_date BETWEEN valid_from AND valid_to`.
- Rebuild được hoàn toàn từ snapshot Bronze.

## 4. Semantic model `sm_sales`
- **Direct Lake** trên `gld.*`; star schema; `gld_d_date` đánh dấu date table; ẩn SK và cột kỹ thuật.
- Measures: Sales, Qty, Net Sales, Sales LY, YoY %, MTD/YTD, Top N Customer; Discount sau Q16.
- `rpt_sales`: Overview · Product · Customer/Country · Sales hierarchy (point-in-time).

## Quyết định
- Fact doanh thu ở grain dòng sản phẩm của đơn, trạng thái hiện hành. Salesman SCD2; còn lại SCD1.

## Còn mở
- Cách tính thuế khi `tax_rate` bẩn (sau `std_tax_rate`) — chốt sau EDA.
- Effective date SCD2 (§3).
