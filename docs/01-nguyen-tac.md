# 01 — Nguyên tắc

## 1. Kim chỉ nam

> **Raw bất biến; mỗi tầng một trách nhiệm; mọi bước là hàm thuần trên một phạm vi xác định; chọn cách ghi theo loại bảng; kiểm tra trước khi công bố; dựng lại được từ Landing bất cứ lúc nào.**

| # | Nguyên tắc | Nghĩa là trong dự án này |
|---|---|---|
| N1 | **Raw bất biến, dựng lại được** | Landing chỉ thêm, không sửa, không xoá. Xoá toàn bộ `slv`, `gld` rồi chạy full rebuild phải ra đúng số như trước. |
| N2 | **Mỗi tầng một trách nhiệm** | Landing giữ bằng chứng. Silver conform: đổi tên, ép kiểu, chuẩn hoá, gộp nguồn, dedupe kỹ thuật. Gold mô hình hoá: survivorship, SCD, point-in-time, star schema. |
| N3 | **Hàm thuần trên phạm vi xác định** | Worker nhận phạm vi tường minh (danh sách delivery, dải version CDF, hoặc `full`) và trả kết quả. Cùng phạm vi + cùng code ⇒ cùng output. Worker không tự quyết phạm vi theo giờ hệ thống. |
| N4 | **Incremental theo thay đổi** | Gold đọc `table_changes()` từ checkpoint đã lưu, lấy tập khoá bị chạm, tính lại đúng tập đó. |
| N5 | **Chọn cách ghi theo loại bảng** | Theo bảng §2. |
| N6 | **Contract tại ranh giới** | Mô tả nguồn nằm trong contract ODCS; code đọc contract (qua `nb_contracts`), không tự giả định cột/định dạng. |
| N7 | **Kiểm tra trước khi công bố** | Lượt chạy chỉ `SUCCEEDED` khi mọi check critical PASS. Mọi thứ đọc từ Gold (sau này: semantic model) chỉ làm mới khi lượt `SUCCEEDED`. |
| N8 | **Đối soát độc lập** | Hai con số so sánh phải được tính từ hai nơi khác nhau; không so một bảng với bảng sinh ra từ chính nó. |
| N9 | **Mỗi bảng trạng thái có nhóm writer xác định** | Mỗi bảng `meta` chỉ do các notebook nêu ở [04 §4.6](04-kien-truc.md#46-ai-ghi-bảng-nào) ghi; mỗi notebook Silver/Gold chỉ ghi trạng thái của chính nó (`table_state` của bảng đích, `run_log` của bước nó, `delivery_log` của delivery nó xử lý). Pipeline điều phối thứ tự và lỗi. |
| N10 | **Đơn giản trước, đo rồi mới tối ưu** | Không partition, không chạy song song, không thêm bảng điều khiển ngoài các bảng `meta` đã định cho tới khi có số đo cho thấy cần. |

## 2. Cách ghi theo loại bảng

Đây là quy định. Helper tương ứng ở [05 §4.6](05-thu-vien.md#46-ghi-delta).

| Loại bảng | Bảng | Cách ghi | Helper |
|---|---|---|---|
| Raw | `Files/landing/...` | thêm thư mục delivery mới; không ghi đè | `write_manifest` (sau khi ghi data) |
| Sự kiện Silver | `slv.orders_event` | MERGE theo event key: chèn nếu mới; cập nhật nếu delivery mới hơn và nội dung khác | `merge_events` |
| Lịch sử master Silver | `slv.<entity>_history` | MERGE chỉ chèn theo `(source_system, key, _snapshot_date)` | `merge_insert_only` |
| Dimension Gold | `gld.dim_*` | đồng bộ toàn phần: update khi hash khác, insert mới, xoá dòng không còn | `sync_table` |
| Fact Gold | `gld.fct_order_line` | MERGE upsert theo grain, update khi hash khác | `merge_upsert` |
| Rebuild bất kỳ bảng nào | khi full | overwrite toàn bảng, giữ schema | `overwrite_table` |
| `meta.*` | | append (`delivery_log`, `run_log`, `check_result`, `landed_file`), MERGE (`table_state`) | `log_*`, `record_*`, `state_put` |
| Quarantine | `slv.quarantine` | MERGE chỉ chèn; đánh dấu đã xử lý bằng UPDATE `resolved_at_utc` | `quarantine_rows`, `resolve_quarantine` |

Cấm: `DELETE` rồi `append` thành hai lệnh; `DROP TABLE` trong worker; tạo bảng ngầm bằng `saveAsTable` lần đầu (mọi bảng tạo bằng DDL ở `nb_setup_ddl`); `mergeSchema`/`autoMerge`.

## 3. Trách nhiệm từng tầng

| Tầng | Được làm | Không được làm |
|---|---|---|
| **Landing** `Files/landing` | copy nguyên byte (file), ghi Parquet nguyên kiểu nguồn (DB), đếm dòng, checksum, ghi manifest | đổi kiểu, lọc, dedupe, sửa giá trị |
| **Silver** `slv` | đổi tên cột về canonical; ép kiểu; chuẩn hoá mã, chữ, ngày, trạng thái, thuế; đổi giờ về UTC; gộp nguồn với cột `source_system`; dedupe kỹ thuật; quarantine dòng không conform được | survivorship giữa nguồn; lọc theo nghiệp vụ; tính chỉ số |
| **Gold** `gld` | survivorship; SCD2; point-in-time; inferred member; dựng fact theo grain | parse chuỗi thô; đọc Landing; tính KPI (để sau — xem [04 §16](04-kien-truc.md#16-ngoài-phạm-vi-hiện-tại)) |
| **meta** | nhật ký, trạng thái, kết quả check | dữ liệu nghiệp vụ |

## 4. Đặt tên

| Đối tượng | Quy ước | Ví dụ |
|---|---|---|
| Source ID (= `id` của contract) | `<source_system>.<entity>` | `retail.orders` |
| File contract | `contracts/<source_system>/<entity>.yaml` | `contracts/retail/orders.yaml` |
| Bảng Silver sự kiện / lịch sử / view hiện hành | `slv.<entity>_event` / `slv.<entity>_history` / `slv.v_<entity>_current` | `slv.orders_event` |
| Bảng Gold | `gld.dim_<x>`, `gld.fct_<x>` | `gld.fct_order_line` |
| Notebook | `nb_<tầng>_<tên>`; Gold theo tên bảng | `nb_slv_orders`, `nb_gld_dim_salesman` |
| Cột kỹ thuật | tiền tố `_` | `_delivery_id` |
| Thời gian UTC | hậu tố `_utc` | `updated_at_utc` |
| Khoá ngày dạng số `yyyyMMdd` | hậu tố `_date_key` | `order_date_key` |
| Surrogate key `bigint` | hậu tố `_sk` | `customer_sk` |
| Cờ | tiền tố `is_` | `is_inferred` |
| Temp view | `_<notebook>__<tên>` | `_nb_gld_fct_order_line__src` |
| Check | `DQ-<tầng>-<số>` (platform), `CT-<source id>-<số>` (contract), `RC-<số>` (đối soát) | `DQ-GLD-03` |
| Rule nghiệp vụ / chuẩn hoá | `R-<miền>-<số>` | `R-SLM-02` |
| Task | `T<pha>.<số>` | `T4.1` |

## 5. Điều cấm

| Không làm | Làm thay |
|---|---|
| Parse thời gian bằng `to_timestamp(col)` không có format | `parse_ts(col, formats, tz)` với format lấy từ contract |
| Gọi `.count()` nhiều lần trên cùng DataFrame đọc từ file | `persist()` một lần, tính mọi số đếm bằng một `groupBy().agg()`, rồi `unpersist()` |
| Đọc dữ liệu nguồn bằng JDBC, token hoặc `fs.cp` trong notebook | Copy activity của pipeline qua Connection |
| Truy vấn nguồn nhiều lần cho một delivery | ghi Landing một lần, đếm và tính checksum từ Landing |
| Phát hiện thay đổi bằng version của cả bảng | checkpoint version + `table_changes`, bỏ qua commit không đổi dữ liệu |
| Đặt rule nghiệp vụ trong `nb_common` | rule ở notebook của bảng; từ điển ở `nb_config`; mô tả nguồn ở contract |
| `spark.conf.set` trong worker | chỉ `nb_common` đặt cấu hình phiên |
| `%%configure` | không dùng ([TROUBLESHOOTING](TROUBLESHOOTING.md)) |
| `%run` chung cell với code | mỗi `%run` một cell |
| `except: pass` | bắt lỗi cụ thể; không xử lý được thì raise để activity Failed và pipeline ghi FAILED |
| Sửa tay `nb_contracts` | sửa YAML rồi chạy `uv run tools/build_contracts.py` |
| Comment giải thích "đã làm gì" hoặc code để dành | comment chỉ ghi lý do không hiển nhiên; không để code không dùng |

## 6. Definition of Done

Một task chỉ `DONE` khi đủ:

- [ ] Code khớp đặc tả trong 03/04/05; mọi tên, path, tham số khớp tài liệu.
- [ ] `uv run tools/check_repo.py` không lỗi.
- [ ] Đổi logic một object ⇒ `logic_version` của object đó tăng trong cùng commit.
- [ ] Tài liệu bị ảnh hưởng được sửa trong cùng commit; không để lại mô tả cũ.
- [ ] WORKLOG có mục của task, kèm khối kiểm chứng.
- [ ] Người đã chạy kiểm chứng trên Fabric, kết quả dán vào WORKLOG, mọi tiêu chí "Xong khi" đạt.

## 7. Thuật ngữ

| Từ | Nghĩa |
|---|---|
| Source ID | `<source_system>.<entity>`, ví dụ `wholesale.orders`; trùng `id` của contract |
| Delivery | một lần ingest một source ID trong một lượt = một thư mục `run=<delivery_id>` ở Landing |
| Manifest | `_manifest.json` của một delivery; ghi sau cùng; có manifest mới là delivery hợp lệ |
| Batch | ngày nghiệp vụ của lượt chạy, `batch_id = yyyyMMdd` của `p_load_date` |
| Snapshot | ảnh toàn bộ một bảng master tại một ngày; khoá vắng mặt ở snapshot mới nghĩa là đã bị xoá |
| Event | một thay đổi trạng thái của một dòng hàng; không bao giờ bị ghi đè |
| Watermark | mốc `updated_at` đã lấy tới (nguồn DB incremental); chỉ tiến khi delivery đã nạp Silver thành công |
| Lookback | lùi watermark để bắt dòng commit trễ |
| Settle | bỏ qua file vừa sửa trong `settleMinutes` phút gần nhất vì có thể đang ghi dở |
| Conform | biến dữ liệu nguồn thành cột canonical đúng kiểu, đã chuẩn hoá |
| REJECT / FLAG | dòng bị loại khỏi bảng đích / dòng vẫn vào nhưng được ghi nhận vấn đề |
| Checkpoint | version Delta của từng input mà một bảng đích đã xử lý tới (`meta.table_state`) |
| CDF | Change Data Feed của Delta: các dòng insert/update/delete giữa hai version |
| `logic_version` | số nguyên tăng mỗi khi đổi logic của một object; khác với checkpoint ⇒ object chạy full |
| Inferred member | dòng dimension tạm cho mã có trong orders nhưng chưa có master |
| Point-in-time join | nối fact với phiên bản SCD2 có hiệu lực tại ngày giao dịch |
| Grain | một dòng của bảng đại diện cho cái gì (fact: một dòng hàng; dimension: một thực thể) |
| SCD1 / SCD2 | dimension ghi đè thuộc tính khi đổi / giữ mỗi phiên bản với khoảng hiệu lực `valid_from`–`valid_to` |
| Accumulating snapshot | fact mà mỗi dòng là một quy trình có vòng đời, được cập nhật khi quy trình tiến triển (ở đây `fct_order_line`) |
| Surrogate key (`_sk`) | khoá số thay cho khoá tự nhiên; ở đây là `xxhash64` của khoá tự nhiên |
| Orphan key | mã có trong orders mà không có trong master (ví dụ `CUS099`) |
| Late arrival | sự kiện đến sau ngày nó xảy ra; không bị sót vì watermark theo `updated_at`, không theo `order_date` |
| Idempotent | chạy lại cùng input cho cùng kết quả |
| Quarantine | nơi giữ dòng không conform được (`slv.quarantine`) kèm lý do, để không làm hỏng cả delivery |
| Park / release | người loại một delivery hỏng khỏi xử lý có lý do / đưa lại vào xử lý |
| Thay thế (supersede) | delivery `FAILED` hết bị tính khi có delivery `LOADED` mới hơn (file gửi lại) |
| Đồng hồ nguồn / đồng hồ hệ thống | thời gian bên trong dữ liệu (`updated_at`, `order_date`, `p_load_date`) / giờ thật UTC (thời điểm file được sửa, thời điểm chạy); không so sánh chéo ([02 §4](02-nguon.md#4-hai-đồng-hồ)) |

## 8. Bài học từ v1

Lỗi đã thấy khi review bản v1 (2026-10-14). Thiết kế ở cột phải là cách tài liệu này ngăn lỗi đó; người review đối chiếu khi duyệt code.

| Lỗi ở v1 | Thiết kế ngăn |
|---|---|
| Phiên bản SCD2 đầu tiên bắt đầu ở ngày snapshot đầu ⇒ đơn hàng lịch sử mất salesman | R-SLM-02 ([04 §8](04-kien-truc.md#8-rule-chuẩn-hoá-và-mô-hình)) |
| Settle window tính ra nhưng không áp dụng ⇒ đọc file đang ghi dở | [04 §3.3](04-kien-truc.md#33-nguồn-file) bước 3 |
| Đối soát so một số với chính nó ⇒ luôn PASS | N8; [04 §14](04-kien-truc.md#14-đối-soát) |
| Full reload kết hợp lọc theo ngày ⇒ Gold còn một ngày dữ liệu | PF05 ([04 §11.2](04-kien-truc.md#112-kiểm-tra-trước-khi-chạy)) |
| Chạy lại Silver không áp logic mới cho dòng đã có | `logic_version` ⇒ full ([04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node)) |
| Parse thời gian không format ⇒ cả sự kiện retail bị quarantine; ngày M/d–d/M đoán lặng lẽ | format từ contract; FLAG `ambiguous_date` |
| Đọc nguồn DB ba lần cho một delivery | [04 §3.2](04-kien-truc.md#32-nguồn-db) |
| Bảng dùng chung tạo ngầm, kiểu cột phụ thuộc nguồn nạp trước | DDL tường minh; tắt `autoMerge` |
| Gold chạy lại vì OPTIMIZE; đổi code Gold không bị phát hiện | `data_changed_since`, `logic_version` |
| Lookback 24h copy lại cùng file mỗi lượt | `meta.landed_file` |
| Aggregate xoá rồi chèn ở hai commit | cấm (§2) |
| Checksum cộng `bigint` có thể tràn | checksum dùng `decimal(38,0)` |
| Delivery lỗi ở batch cũ không được thử lại | `meta.v_pending_delivery` không giới hạn batch |
| Không khoá chống chạy chồng | `concurrency: 1` của pipeline; mọi ghi idempotent |
| Cấu hình `depends_on`, `dq` khai báo nhưng không ai đọc; thứ tự hardcode | DAG sinh từ `nb_config`; check từ `CHECKS` và contract |
| Silver tách theo nguồn, kiểu thô; Gold tự parse lại | Silver canonical một bảng mỗi entity |
| Quarantine "xử lý" bằng DELETE ⇒ mất vết | `resolved_at_utc` |
| Tài liệu mô tả object không tồn tại | một bộ tài liệu; Definition of Done bắt sửa docs cùng commit |
