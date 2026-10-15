# DECISIONS — Quyết định và lý do

Mỗi quyết định có ID cố định. Đổi một quyết định: thêm dòng mới ghi "thay D<xx>", sửa trạng thái dòng cũ thành "Bị thay bởi D<yy>"; không xoá dòng.

## 1. Kiến trúc và kỹ thuật

| ID | Quyết định | Lý do | Ngày |
|---|---|---|---|
| D01 | Contract nguồn viết bằng YAML theo ODCS v3.0.2 trong `contracts/`; `tools/build_contracts.py` kiểm tra và sinh `nb_contracts`; runtime đọc dict đã sinh | chuẩn ngành, team nguồn đọc và duyệt được; notebook Fabric không đọc được file trong repo lúc chạy nên sinh code thay vì upload file | 2026-10-14 |
| D02 | Profile ODCS tối giản ([03 §3](03-contract.md#3-profile-odcs-của-dự-án)); validator tự viết, chỉ phụ thuộc `pyyaml` | chuẩn ở mức cần dùng, không thêm công cụ nặng | 2026-10-14 |
| D03 | Thư viện dùng chung là notebook `%run` (`nb_contracts`, `nb_config`, `nb_std`, `nb_common`); không đóng gói wheel | đơn giản; đổi sang wheel khi cần test ngoài Fabric | 2026-10-14 |
| D04 | Ba tầng Landing → Silver → Gold; `meta` là schema vận hành; không có Bronze | Landing đã là raw bất biến | 2026-10-14 |
| D05 | Silver một bảng mỗi entity cho mọi nguồn, kiểu canonical, chuẩn hoá tại Silver | thêm kênh bán không sinh bảng mới; Gold không lặp logic parse | 2026-10-14 |
| D06 | Master ở Silver chỉ lưu thay đổi (`_history`) + view hiện hành | không lưu nguyên snapshot mỗi ngày; đủ để dựng SCD2 | 2026-10-14 |
| D07 | Trạng thái hiện hành của orders là view `slv.v_orders_current` | bớt một bảng phải đồng bộ | 2026-10-14 |
| D08 | Gold incremental bằng CDF + checkpoint `meta.table_state`; đổi logic bằng `logic_version` ⇒ full | không phụ thuộc version cả bảng (OPTIMIZE không kích hoạt); đổi code được phát hiện | 2026-10-14 |
| D09 | Mọi bảng tạo bằng DDL tường minh; tắt `autoMerge`; cột mới qua contract + DDL | schema có kiểm soát, không phụ thuộc thứ tự nạp | 2026-10-14 |
| D10 | Delivery ghi vào staging rồi `mv` sang Landing, manifest ghi sau cùng | Landing chỉ chứa delivery hoàn chỉnh; snapshot không đổi không để lại thư mục | 2026-10-14 |
| D11 | Inferred member do notebook dimension tạo; fact không ghi dimension | một writer mỗi bảng | 2026-10-14 |
| D12 | Surrogate key = `xxhash64` của khoá tự nhiên (SCD2: kèm `valid_from`) | tất định, fact không cần tra dimension; va chạm được kiểm bằng DQ-GLD-08 | 2026-10-14 |
| D13 | Orchestrator tuần tự trong một Spark session | **Bị thay bởi D34** | 2026-10-14 |
| D14 | Một bảng `slv.quarantine` với `action` REJECT/FLAG; xử lý xong đánh dấu `resolved_at_utc` | đơn giản, không mất vết | 2026-10-14 |
| D15 | Mọi activity `retry: 0`; chạy chồng chặn bằng `concurrency: 1` (không có bảng khoá); khôi phục bằng checkpoint ở lượt sau | retry mù có thể để lại staging dở; ghi idempotent làm chạy lại an toàn | 2026-10-14 |
| D16 | Cảnh báo bằng activity Teams có sẵn của Data pipeline, connection cấu hình trên workspace | không có secret trong Git; chờ spike K07 xác nhận | 2026-10-14 |
| D17 | Sự cố nguồn tạo bằng `nb_00_sim_inject` ở workspace Source | diễn tập lặp lại được, test E2E tự động; platform vẫn chỉ đọc nguồn | 2026-10-14 |
| D18 | Làm lại từ đầu trên Dev sau reset; Prod chưa có dữ liệu cần giữ; code v2 không đọc Landing/meta của v1 | không cần lớp tương thích | 2026-10-14 |
| D27 | Delivery có khuyết tật xác định (thiếu cột bắt buộc) bị `FAILED` và không được xử lý; node Silver FAILED chặn Gold (fail-closed). Người loại nó bằng `nb_ops_delivery park` kèm lý do; DQ-OPS-03 cảnh báo mỗi lượt cho tới khi `release` | Gold không bao giờ tính trên dữ liệu thiếu mà không ai biết; vẫn thoát được bằng một thao tác có dấu vết | 2026-10-14 |
| D28 | Worker Gold đọc `slv.orders_event` đã lọc theo khoá bị chạm rồi chọn dòng mới nhất bằng `latest_event`; không đọc view `v_orders_current` | window function trên cả bảng làm mất lợi ích incremental; view giữ cho người đọc và đối soát, kiểm bằng test cùng kết quả | 2026-10-14 |
| D29 | Ở Dev chờ hơn `settleMinutes` sau khi simulator ghi file; không có tham số bỏ settle. Lượt bị settle ghi `NO_DATA` kèm `settle_pending:<n>` | giữ hành vi giống Prod (kịch bản `partial_file`); không có tham số dễ bị dùng nhầm ở Prod | 2026-10-14 |
| D30 | `lookbackMinutes = 0` cho nguồn `sourceClock: virtual`; chỉ đặt > 0 khi nguồn thật có commit trễ, theo đo đạc | cửa sổ ảo chính xác nên lookback chỉ nạp lại dữ liệu vô ích vào Landing | 2026-10-14 |
| D32 | Bổ sung D27: delivery `FAILED` tự được thay thế khi có delivery `LOADED` mới hơn của cùng source (snapshot: bất kỳ; DB: bất kỳ; file event: trùng tên file). `park` chỉ còn cho trường hợp nguồn không gửi lại được | nguồn gửi lại bản đúng thì không cần ai thao tác; Landing vẫn bất biến (bản gửi lại là delivery mới) | 2026-10-14 |
| D33 | `nb_ops_delivery` không có khoá; bắt buộc `p_operator`; quy tắc vận hành: không chạy khi lượt đang chạy, báo nhóm khi chạy | thao tác hiếm, ghi append-only có người chịu trách nhiệm | 2026-10-14 |
| D34 | Điều phối bằng pipeline: `nb_plan` → ForEach(Copy → `nb_land`) → notebook Silver/Gold nối bằng `dependsOn` → `nb_dq` → `nb_recon` → `nb_close`. Không có notebook orchestrator, không `BLOCKED`/dry-run/`run_lock`; mỗi notebook tự `decide_run`. Thay D13 | khớp cách dự án thật làm, ít code điều phối hơn, thấy từng bước trên Monitor, chạy lại từ activity lỗi | 2026-10-14 |
| D35 | Ingest bằng Copy activity qua Connection (DB và file); notebook không đọc nguồn, chỉ liệt kê tên file (`nb_plan`) và kiểm tra staging (`nb_land`) | bài tập phải phản ánh dự án thật: nguồn sau gateway/Connection, credentials ngoài code (tiếp nối quyết định 2026-10-06) | 2026-10-14 |
| D36 | `conform` sinh câu Spark SQL từ cấu hình; Python chỉ ghép chuỗi | SQL-first: logic biến đổi là SQL, team đọc và sửa được | 2026-10-14 |
| D37 | Mỗi Notebook activity có thể khởi động Spark riêng; đo thời gian và số lần khởi động ở T8.3 trước khi tối ưu (chia sẻ session) | L12 | 2026-10-14 |
| D38 | Làm lại từ đầu, không dựa vào dữ liệu hay code v1: bỏ T0.1 (baseline), EDA đọc trực tiếp nguồn, T8.1 đối chiếu với nguồn; nguồn coi như đã sẵn sàng (đúng layout contract) và không reset ở T2.6. Bổ sung D18 | không còn gì của v1 cần so hoặc giữ; EDA không phụ thuộc Landing v1 nên không phải làm trước reset | 2026-10-15 |

## 2. Mô hình và nghiệp vụ

| ID | Quyết định | Lý do | Ngày |
|---|---|---|---|
| D19 | Gold hiện tại gồm 4 dimension và `gld.fct_order_line` (accumulating snapshot của dòng hàng, mọi trạng thái, mốc thời gian lần đầu mỗi trạng thái). Lớp KPI (doanh thu ghi nhận, thuế, lead time, tổng hợp tháng, khoá kỳ) làm sau | tập trung nền tảng dữ liệu đúng và đủ; KPI dựng trên fact này mà không đổi Silver | 2026-10-14 |
| D20 | Grain fact `(source_system, order_no, product_code)` | hai hệ thống có thể dùng trùng số đơn | 2026-10-14 |
| D21 | `STATUS_RANK` theo vòng đời; trạng thái kết thúc xếp sau; dùng khi hai sự kiện cùng `updated_at` | thứ tự tất định, đúng diễn tiến đơn hàng | 2026-10-14 |
| D22 | Survivorship khách hàng/sản phẩm theo từng thuộc tính, ưu tiên `wholesale` rồi `retail` | ERP là hệ thống gốc của master; retail bổ sung chỗ trống | 2026-10-14 |
| D23 | SCD2 salesman: phiên bản đầu từ `1900-01-01`; theo dõi tên, khối, chức danh, quản lý, trạng thái xoá | đơn lịch sử luôn gắn được salesman | 2026-10-14 |
| D24 | Cây quản lý: chỉ quản lý trực tiếp, tên quản lý theo thời điểm; nhiều cấp để sau | đủ cho phân bổ theo người; bridge nhiều cấp cần khi có KPI theo cấp | 2026-10-14 |
| D25 | `order_date` sau ngày batch + 1 ⇒ REJECT `future_order_date` | lỗi nhập liệu (năm 2027) không được vào số liệu | 2026-10-14 |
| D26 | Giá trị phụ thuộc dữ liệu (tên cột file, định dạng thời gian, múi giờ ERP, từ điển) chốt bằng EDA (T0.3), ghi vào contract và `nb_config` (T1.2, T2.1). Trước đó contract để `status: draft` với giá trị hiện có | không đoán; mỗi giá trị có bằng chứng | 2026-10-14 |
| D31 | Ngày mơ hồ ở `updated_at` của file: parse theo thứ tự format trong contract, FLAG `ambiguous_date`, kèm DQ-SLV-07 đo lệch ngày sự kiện so với tên file. File trộn hai thứ tự trong cùng file ⇒ bắt nguồn chuẩn hoá ISO 8601 trước khi contract `active` | thứ tự sự kiện quyết định trạng thái hiện hành: không đoán lặng lẽ, nhưng không chặn chỉ vì mơ hồ | 2026-10-14 |
| D39 | `Returned` là trạng thái thứ sáu: `STATUS_RANK` Pending 1, Confirmed 2, Shipped 3, Delivered 4, Returned 5, Cancelled 6; thêm cột `first_returned_at_utc` vào `gld.fct_order_line`. Thay quyết định bỏ `first_returned_at_utc` ngày 2026-10-14 | EDA E06: ~1.800 dòng có trạng thái này; REJECT sẽ làm mất dữ liệu thật và chạm ngưỡng quarantine | 2026-10-15 |
| D40 | Từ điển `ORDER_STATUS`, `GENDER`, `POSITION`, `BRAND` lấy từ EDA E06, E13 (05 §2.2); `X` của gender và country/city/division không chuẩn hoá ở lượt này | chỉ map giá trị có bằng chứng; giá trị lạ vào FLAG `unmapped_value` thay vì đoán | 2026-10-15 |
| D41 | `sourceTimezone`: wholesale `UTC` (DDL ERP dùng `sysutcdatetime()`); retail `Asia/Ho_Chi_Minh` là **giả định** vì dữ liệu không cho biết (E05); xác nhận với owner nguồn trước khi promote Prod | không suy ra được từ phân bố giờ của dữ liệu giả lập | 2026-10-15 |
| D42 | Câu hỏi mở: số âm của `quantity`/`price` (E11) hiện đi qua Silver và Gold, chỉ cảnh báo ở contract; country, city, division chưa chuẩn hoá. Cần quyết định nghiệp vụ nếu muốn REJECT/FLAG hoặc thêm từ điển | không có quy tắc nghiệp vụ nào trong tài liệu | 2026-10-15 |

## 3. Kết quả spike

Điền ở T0.2. Lượt 1 và 2 (2026-10-15, `nb_spike`): K01–K05, K09, K10. Còn K06, K07, K08, K11, K12 (cần `pl_spike`). Runtime: Spark 4.1.1.5.5.20260930.

| Spike | Kết quả | Ghi chú, tài liệu đã sửa |
|---|---|---|
| K01 `table_changes` | PASS | MERGE ⇒ `update_preimage`/`update_postimage`; DELETE ⇒ `delete`; overwrite ⇒ `delete` + `insert` cho mọi dòng còn lại. Đúng giả định, không sửa tài liệu |
| K02 `NOT MATCHED BY SOURCE` | PASS | `sync_table` dùng một câu MERGE như 05 §4.6 |
| K03 `fs.ls` có `size`, `modifyTime` | PASS | `sempy.fabric.resolve_workspace_id` / `resolve_item_id` tìm theo tên được; `fs.ls` trả `name, path, size, modifyTime, isDir, isFile`; `modifyTime` là **epoch mili-giây** (đổi sang UTC trong `list_drop_zone`); `fs.ls` không đệ quy, thư mục có `size = 0`. Cây thư mục thật cần đối chiếu với `physicalName` của contract (xem WORKLOG) |
| K04 `fs.mv` thư mục | PASS | `mv(src, dst, True)` trả `True`, staging biến mất, file nằm đúng ở đích. `fs.rm` trên path không tồn tại **ném lỗi** ⇒ luôn kiểm `fs.exists` trước |
| K05 Spark / ANSI / `try_to_timestamp` | PASS | Spark 4.1.1; `try_to_timestamp` có, `'abc'` ⇒ NULL. `spark.sql.ansi.enabled = false`: `sum(bigint)` tràn **quay vòng im lặng** (`9223372036854775807 + 1 = -9223372036854775808`) ⇒ checksum `decimal(38,0)` là bắt buộc. `CAST` hỏng cho NULL; vẫn dùng `try_cast` để không phụ thuộc cấu hình |
| K06 pipeline `concurrency` | CHƯA CHẠY | cần `pl_spike` |
| K07 activity Teams | CHƯA CHẠY | cần `pl_spike` |
| K08 exit value trong expression | CHƯA CHẠY | cần `pl_spike` |
| K09 `operationMetrics` | PASS có điều kiện | MERGE có `numTargetRowsInserted/Updated/Deleted`; `WRITE` có `numOutputRows`; **overwrite qua `saveAsTable` ghi `operation = 'CREATE OR REPLACE TABLE AS SELECT'`** (chỉ `numOutputRows`, không đếm dòng xoá); DELETE chỉ có `numDeletedRows`. Đã sửa 05 §4.6 `last_commit_metrics` theo `operation` |
| K10 DDL `NOT NULL` + overwrite | PASS | overwrite `overwriteSchema=false` giữ `NOT NULL`; ghi NULL ⇒ `DeltaInvariantViolationException [DELTA_NOT_NULL_CONSTRAINT_VIOLATED]` kèm tên cột, bọc trong `Py4JJavaError` ⇒ bắt `Exception` và đọc mã trong thông điệp. Không cần kiểm NOT NULL trong `align_to_target` |
| K11 Copy activity (DB → Parquet, file Binary + lọc last modified, ID động, `rowsRead`) | CHƯA CHẠY | cần `pl_spike` |
| K12 nhánh lỗi bắt mọi activity | CHƯA CHẠY | cần `pl_spike` |
