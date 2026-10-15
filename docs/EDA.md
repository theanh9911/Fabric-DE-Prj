# EDA — Khảo sát dữ liệu nguồn (T0.3)

Làm trên Landing v1 (chỉ đọc), **trước** khi reset Dev (T2.6). Mỗi mục: truy vấn đã chạy, kết quả rút gọn, kết luận, giá trị điền vào. Kết quả dùng ở T1.2 (contract) và T2.1 (từ điển trong `nb_config`).

## Chuẩn bị (cell đầu `nb_eda`)

```python
from pyspark.sql import functions as F

def landing(source_system, entity, fmt):
    pattern = f"Files/landing/*/{source_system}/{entity}/run=*/" + ("*.parquet" if fmt == "parquet" else "*.csv")
    df = (spark.read.parquet(pattern) if fmt == "parquet" else
          spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(pattern))
    return (df.select([F.col(c).cast("string").alias(c) for c in df.columns])
              .withColumn("_file", F.input_file_name()))

def shape(c):
    return F.regexp_replace(F.regexp_replace(F.col(c), "[0-9]", "9"), "[A-Za-z]", "a")

ro, wo = landing("retail", "orders", "csv"), landing("wholesale", "orders", "parquet")
```

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

<!-- Mỗi mục E01–E20: truy vấn, kết quả, kết luận, giá trị đã điền. -->
