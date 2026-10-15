# 00 — Tổng quan kiến trúc

Trang này là bản đồ: đọc một lần để hiểu hệ thống làm gì, dữ liệu chảy thế nào, vì sao thiết kế như vậy. Chi tiết (DDL, thuật toán, tham số) nằm ở các file được trỏ tới; không lặp lại ở đây.

## 1. Bài toán

Công ty A bán hàng qua hai kênh với hai hệ thống không liên quan nhau:

| Kênh | Hệ thống | Cách giao dữ liệu |
|---|---|---|
| Bán buôn (wholesale) | ERP quan hệ (Fabric SQL Database) | pipeline Copy activity đọc bảng qua Connection |
| Bán lẻ (retail) | không có kết nối DB | gửi file CSV vào drop zone OneLake |

Platform gom hai kênh thành **một nền dữ liệu bán hàng** có thể tin được: biết mỗi đơn hàng đã đi qua những trạng thái nào và khi nào; biết khách hàng, sản phẩm, nhân viên bán hàng là ai tại từng thời điểm; chạy lại được khi nguồn sửa số hay gửi trễ; và luôn đối soát được số liệu từ nguồn tới bảng cuối.

Dữ liệu mang các đặc điểm của hệ thống thật ([02-nguon.md](02-nguon.md)):

- **Orders là dòng sự kiện:** một đơn có nhiều dòng hàng; mỗi lần đổi trạng thái nguồn thêm một dòng mới, không sửa dòng cũ.
- **Master (khách, sản phẩm, nhân viên, danh mục) sửa tại chỗ:** nguồn không giữ lịch sử; khoá biến mất nghĩa là bị xoá.
- **Dữ liệu bẩn:** trạng thái gõ sai, thuế dạng chữ, số âm, ngày nhiều định dạng không múi giờ, mã mồ côi, ngày năm 2027, danh mục trùng khoá.
- **Sự cố giao nhận:** file gửi lại, gửi trùng, gửi thiếu, đang ghi dở, đổi cột, đơn nộp trễ.

## 2. Sơ đồ

```text
 CompanyA-Source (chỉ đọc)
 ┌────────────────────────────┐   ┌─────────────────────────────────────┐
 │ sqldb_erp_wholesale        │   │ lh_retail_drop/Files/inbound/**     │
 │ customers products         │   │ retail/{customers,products,         │
 │ sales_hierarchy orders     │   │   sales_hierarchy,orders}           │
 └─────────────┬──────────────┘   │ reference/categories                │
               │ Connection       └──────────────────┬──────────────────┘
               └──────── Copy activity (pipeline) ───┘
                    một delivery mỗi nguồn mỗi lượt; nb_land kiểm tra rồi đưa vào Landing
                                   │
 CompanyA-DataPlatform / lh_platform
 ┌─────────────────────────────────▼──────────────────────────────────────┐
 │ LANDING  Files/landing/<batch>/<source>/<entity>/run=<delivery>/       │
 │          dữ liệu nguyên trạng + _manifest.json     (bất biến)          │
 └─────────────────────────────────┬──────────────────────────────────────┘
                 nb_slv_orders / nb_slv_master  (conform theo contract)
 ┌─────────────────────────────────▼──────────────────────────────────────┐
 │ SILVER   slv.orders_event            + view v_orders_current           │
 │          slv.{customers,products,sales_hierarchy,categories}_history   │
 │          + view v_<entity>_current      slv.quarantine                 │
 └─────────────────────────────────┬──────────────────────────────────────┘
                 nb_gld_*  (đọc thay đổi bằng CDF từ checkpoint)
 ┌─────────────────────────────────▼──────────────────────────────────────┐
 │ GOLD     dim_date  dim_customer  dim_product  dim_salesman (SCD2)      │
 │          fct_order_line                                                │
 └─────────────────────────────────┬──────────────────────────────────────┘
                          nb_dq → nb_recon
 ┌─────────────────────────────────▼──────────────────────────────────────┐
 │ META     delivery_log landed_file run_log check_result table_state     │
 └────────────────────────────────────────────────────────────────────────┘
```
## 3. Ba tầng và `meta`

| Tầng | Vai trò | Bảng chính | Cách ghi | Chi tiết |
|---|---|---|---|---|
| **Landing** | bằng chứng: bản sao nguyên trạng, không biến đổi gì | thư mục delivery + manifest | thêm thư mục mới, không ghi đè | [04 §3](04-kien-truc.md#3-landing) |
| **Silver** | conform: một schema chuẩn cho mọi nguồn (đổi tên, ép kiểu, chuẩn hoá, UTC, dedupe kỹ thuật) | `slv.orders_event`, `slv.<entity>_history`, `slv.quarantine` | MERGE idempotent; full rebuild từ Landing | [04 §5](04-kien-truc.md#5-silver) |
| **Gold** | mô hình hoá nghiệp vụ: hợp nhất nguồn, SCD2, point-in-time, grain | `gld.dim_*`, `gld.fct_order_line` | đồng bộ/ upsert theo thay đổi (CDF) | [04 §6](04-kien-truc.md#6-gold) |
| **meta** | vận hành: delivery nào đã nạp, bước nào chạy, check nào PASS, đã xử lý tới version nào | 5 bảng | mỗi notebook ghi trạng thái của chính nó ([04 §4.6](04-kien-truc.md#46-ai-ghi-bảng-nào)) | [04 §4](04-kien-truc.md#4-schema-meta) |

Ba tầng chứ không thêm Bronze: Landing đã là raw bất biến nên một lớp Delta raw nữa chỉ nhân đôi dữ liệu.

## 4. Mô hình dữ liệu

```text
                      gld.dim_date
                           │ order_date_key
 gld.dim_customer ─── gld.fct_order_line ─── gld.dim_product
   customer_sk              │  salesman_sk        product_sk
                            │  (point-in-time)
                      gld.dim_salesman  (SCD2: valid_from..valid_to)
```

- **`fct_order_line`** — accumulating snapshot, grain `(source_system, order_no, product_code)`. Mỗi dòng hàng một dòng: trạng thái hiện tại, số lượng, giá, thuế, và thời điểm **lần đầu** đạt từng trạng thái (`first_pending_at_utc` … `first_cancelled_at_utc`). Lịch sử sự kiện đầy đủ vẫn ở `slv.orders_event`.
- **`dim_salesman`** — SCD2: đổi quản lý hay chức danh sinh phiên bản mới; fact gắn đúng phiên bản có hiệu lực tại `order_date`.
- **`dim_customer`, `dim_product`** — SCD1 hợp nhất hai kênh theo từng thuộc tính; mã có trong đơn mà chưa có master tạo dòng `inferred` để fact giữ đủ số dòng.
- Surrogate key là `xxhash64` của khoá tự nhiên nên fact tính được mà không phải tra dimension.

Lớp KPI (doanh thu ghi nhận, thuế, lead time, tổng hợp theo tháng) **chưa có**; sẽ dựng trên `fct_order_line` mà không đổi Silver ([DECISIONS D19](DECISIONS.md#2-mô-hình-và-nghiệp-vụ)).

## 5. Một lượt chạy

`pl_master_daily` là bộ điều phối. Mỗi bước là một activity; activity lỗi thì các activity sau không chạy:

```text
1. nb_plan         kiểm tra tham số, lập kế hoạch ingest (nguồn nào, cửa sổ nào, file nào)
2. ForEach nguồn   Copy activity (qua Connection) → nb_land: đếm, checksum, đưa vào Landing + manifest
3. nb_slv_*        delivery chưa nạp → Silver            (tự bỏ qua nếu không có delivery)
4. nb_gld_*        theo thứ tự cố định → Gold            (tự bỏ qua nếu input không đổi)
5. nb_dq           check platform + check của contract
6. nb_recon        đối soát Nguồn ↔ Landing ↔ Silver ↔ Gold
7. nb_close        ghi kết quả lượt; lỗi ⇒ nhánh lỗi ghi FAILED và gửi cảnh báo
```

Mỗi notebook Silver/Gold tự quyết định chạy hay bỏ qua bằng hàm `decide_run`, dựa trên checkpoint trong `meta.table_state`, `logic_version` và việc input có thay đổi dữ liệu thật hay không ([04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node)). Hai chế độ: `normal` (lấy mới từ nguồn) và `rerun` (xử lý lại những gì đã có ở Landing, không chạm nguồn).
## 6. Thành phần

| Thành phần | Là gì | Ai đọc/ghi | Chi tiết |
|---|---|---|---|
| `contracts/*.yaml` | thoả thuận với team nguồn: cột, khoá, cách giao, hạn giao, chất lượng, owner | người sửa; `build_contracts.py` kiểm tra | [03](03-contract.md) |
| `nb_contracts` | dict Python sinh từ contract | mọi notebook `%run` | [03 §6](03-contract.md#6-toolsbuild_contractspy) |
| `nb_config` | cấu hình platform: Silver, Gold, check, từ điển chuẩn hoá | mọi notebook `%run` | [05 §2](05-thu-vien.md#2-nb_config) |
| `nb_std` | sinh biểu thức Spark SQL chuẩn hoá cột và `conform_sql` | notebook Silver | [05 §3](05-thu-vien.md#3-nb_std) |
| `nb_common` | hạ tầng: đọc Landing, ghi Delta, CDF, quarantine, ghi trạng thái, quyết định chạy | mọi notebook | [05 §4](05-thu-vien.md#4-nb_common) |
| Notebook ingest | `nb_plan` (lập kế hoạch), `nb_land` (kiểm tra và đưa dữ liệu Copy vào Landing) | pipeline | [04 §9.1](04-kien-truc.md#91-nb_plan-và-nb_land) |
| Notebook Silver/Gold | `nb_slv_orders`, `nb_slv_master`, `nb_gld_*` — xử lý dữ liệu, tự ghi trạng thái của mình | pipeline | [04 §9](04-kien-truc.md#9-thuật-toán-từng-worker) |
| Copy activity | lấy dữ liệu từ nguồn qua Connection | pipeline | [04 §3](04-kien-truc.md#3-landing) |
| Pipelines | `pl_master_daily`, `pl_backfill`, `pl_sla_watch`, `pl_maintenance` | lịch / người | [04 §15](04-kien-truc.md#15-pipeline-lịch-cảnh-báo-bảo-trì) |

## 7. Các tình huống nguồn được xử lý thế nào

| Tình huống | Cơ chế |
|---|---|
| Đơn đổi trạng thái | nguồn thêm sự kiện; Silver chỉ thêm, không ghi đè; view hiện hành lấy sự kiện mới nhất; Gold cập nhật dòng hàng bị chạm |
| Đơn nộp trễ (`order_date` ở quá khứ) | watermark theo `updated_at` (đồng hồ kỹ thuật) nên không bỏ sót; fact cập nhật đúng dòng hàng |
| File gửi lại cùng tên | `modifyTime` mới ⇒ delivery mới; sự kiện trùng khoá được so `_record_hash`, chỉ sửa khi nội dung khác |
| File trùng nội dung khác tên | delivery mới nhưng Silver chèn 0 dòng; cảnh báo checksum trùng |
| File đang ghi dở | settle window bỏ qua file mới sửa; lượt sau nạp |
| File không tới | `nb_sla_watch` cảnh báo theo `deliveryCadence` và `availableByLocalTime` trong contract |
| Dòng bẩn | REJECT (không vào bảng, ghi quarantine kèm lý do) hoặc FLAG (vào bảng, ghi nhận) — một dòng lỗi không làm hỏng cả delivery |
| Nguồn đổi cột | Landing giữ nguyên; Silver bỏ cột lạ và ghi `schema_added`; thiếu cột bắt buộc ⇒ delivery FAILED |
| Master bị xoá ở nguồn | so snapshot ⇒ dòng `DELETE`; dimension `is_deleted = true`; fact lịch sử không đổi |
| Đổi quản lý bán hàng | history `UPDATE` ⇒ phiên bản SCD2 mới; đơn cũ giữ quản lý cũ nhờ point-in-time |
| Mã mồ côi (khách, sản phẩm, nhân viên) | dòng `inferred` trong dimension; fact vẫn đủ số dòng |
| Delivery hỏng (thiếu cột bắt buộc) | delivery `FAILED` và không được xử lý; activity Silver Failed chặn Gold và gửi cảnh báo; nguồn gửi lại bản đúng thì tự hết chặn; không gửi lại được thì người `park` kèm lý do |
| Lượt lỗi giữa chừng | activity lỗi, các activity sau không chạy; chạy lại từ activity lỗi, phần đã xong tự bỏ qua nhờ checkpoint |
| Đổi logic một bảng | tăng `logic_version` ⇒ bảng đó và bảng con tự dựng lại đầy đủ |

## 8. Vì sao thiết kế như vậy

| Lựa chọn | Lý do | Chi tiết |
|---|---|---|
| Contract YAML + sinh code | chuẩn ngành, team nguồn đọc và duyệt được; Fabric chỉ chạy notebook | [DECISIONS D01](DECISIONS.md#1-kiến-trúc-và-kỹ-thuật) |
| Silver một bảng mỗi entity cho mọi nguồn | thêm kênh bán không thêm bảng; Gold không lặp logic parse | D05 |
| Master lưu thay đổi, không lưu snapshot mỗi ngày | gọn; đủ dựng SCD2 | D06 |
| Incremental bằng CDF + checkpoint | không phụ thuộc version cả bảng; OPTIMIZE không kích hoạt chạy lại | D08 |
| Surrogate key bằng hash | fact không phải tra dimension; chạy lại cho cùng key | D12 |
| Pipeline điều phối, notebook tự quyết chạy hay bỏ qua | thứ tự và lỗi là việc của pipeline; logic checkpoint nằm trong code, dễ kiểm | D34 |
| Ingest bằng Copy activity qua Connection | cách dự án thật làm; credentials ngoài code; không tốn Spark | D35 |
| `conform` sinh Spark SQL từ cấu hình | SQL-first; Python chỉ ghép chuỗi | D36 |
| DDL tường minh, tắt `autoMerge` | schema có kiểm soát; không phụ thuộc thứ tự nạp | D09 |

Bản v1 cũ đã mắc những lỗi nào và thiết kế này ngăn ra sao: [01 §8](01-nguyen-tac.md#8-bài-học-từ-v1).

## 9. Bản đồ tài liệu

| Muốn biết | Đọc |
|---|---|
| Luật, nguyên tắc, cách ghi theo loại bảng, đặt tên | [01-nguyen-tac.md](01-nguyen-tac.md) |
| Nguồn và simulator, sự cố diễn tập | [02-nguon.md](02-nguon.md) |
| Cấu trúc contract, quy trình đổi | [03-contract.md](03-contract.md) |
| DDL, thuật toán, điều phối, check, đối soát | [04-kien-truc.md](04-kien-truc.md) |
| Cấu hình, hàm dùng chung, mẫu file | [05-thu-vien.md](05-thu-vien.md) |
| Việc cần làm | [06-tasks.md](06-tasks.md) |
| Dựng workspace, vận hành, kiểm thử | [07-van-hanh-kiem-thu.md](07-van-hanh-kiem-thu.md) |
