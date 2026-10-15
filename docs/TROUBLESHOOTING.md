# TROUBLESHOOTING — Lỗi Fabric đã gặp

> Chỉ giữ các lỗi còn áp dụng với luồng hiện tại (`pl_master_daily`: Copy, `nb_land`, Silver, Gold). Khi nhờ chẩn đoán, đính kèm Activity Run ID, Notebook Run ID, thời điểm và **exception đầu tiên**; ảnh chỉ có trạng thái Failed thường chưa đủ.

## Capacity và Spark session

| Triệu chứng | Nguyên nhân | Xử lý |
|---|---|---|
| `TooManyRequestsForCapacity`, HTTP 430 | capacity chạm giới hạn Spark tổng thể; có thể do workspace khác chiếm, dù workspace hiện tại hiện Active = 0 | Monitoring hub: lọc Spark job đang chạy hoặc queued, huỷ job treo, đóng notebook và session thừa, chờ capacity nhả rồi thử lại. Vẫn gặp liên tục thì giảm tải hoặc tăng SKU. Đừng bấm Run nhiều lần liên tiếp |
| `Kernel is not found`, WebSocket close 1000, notebook `Stopped` | kernel hoặc session đã dừng hoặc không được cấp sau lỗi capacity | đóng và mở lại notebook hoặc tạo session mới khi capacity cho phép. Không coi là lỗi code nếu chưa có traceback trước đó |
| Chạy được một cell, cell sau báo 430 | job hoặc session kế tiếp bị capacity chặn | kiểm tra Monitoring rồi chạy tiếp khi cho phép; tránh mở nhiều notebook và bấm Connect hoặc Run All đồng thời |
| `ShutdownHook ... TimeoutException`, `no active spark session found`, log token, cuối log `Shutting down` | các dòng này xuất hiện lúc session đang dừng, không chỉ ra nguyên nhân gốc | trong Run details tìm exception đầu tiên ở driver stderr hoặc traceback của cell; nếu không có thì lấy run ID và thời điểm rồi xem trạng thái capacity và kernel |

F4 hoặc Trial có ít Spark compute; high concurrency giúp chia sẻ session nhưng không tăng tổng capacity. Đổi code notebook không giải phóng capacity khi gặp 430.

## Notebook, import, Lakehouse binding

| Lỗi | Nguyên nhân | Xử lý |
|---|---|---|
| `MagicUsageError: %run cannot run with other code or magic commands` | `%run` chung cell với code hoặc magic khác | đặt mỗi `%run` trong cell riêng |
| `MagicUsageError` với `%%configure` trong high concurrency | `%%configure -f` cần restart session, không hợp high concurrency | platform không dùng `%%configure`; gắn `lh_platform` qua **Data items**, đặt làm default Lakehouse, bật Lakehouse Auto-Binding in Git, lưu rồi sync. Không thêm lại `%%configure -f` |
| `No default context found, please attach a lakehouse before running spark sql queries with partial namespaces` | lệnh dùng tên bảng không đầy đủ nhưng notebook không có default Lakehouse | chọn `lh_platform` làm default Lakehouse cho từng notebook chạy từ pipeline, lưu và sync binding; mở phiên mới trước khi thử lại |
| `FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME: DecimalType(38,4) can not accept object 0 in type int` khi ghi `meta.check_result` | schema khai báo `DECIMAL(38,4)` nhưng truyền Python `int`; PySpark không tự ép khi schema tường minh | helper ghi check chuyển số sang `decimal.Decimal` trước khi tạo DataFrame; sync bản sửa, mở session mới, chạy lại. Lần trước có thể đã xong Gold nhưng chưa xong check |
| `cannot import name 'fabric' from 'notebookutils'` | runtime không export `fabric` từ `notebookutils` | dùng `sempy.fabric` và `FabricRestClient` |
| `Notebook execution failed ... HTTP status code 200` kèm `MagicUsageError` | HTTP 200 là trạng thái của dịch vụ notebook; notebook vẫn có thể Failed | đọc `errorName`, thông điệp, run ID và cell lỗi, đừng kết luận từ HTTP status |
| `WRONG_COMMAND_FOR_OBJECT_TYPE` (`DROP VIEW` trên object kiểu `MANAGED`) | script cleanup gọi sai loại lệnh | xem loại object trước khi drop; bảng dùng `DROP TABLE IF EXISTS`, view dùng `DROP VIEW IF EXISTS`; không bắt lỗi rồi giả định đã xoá |

## Git và UI

| Hiện tượng | Nguyên nhân | Xử lý |
|---|---|---|
| Hộp thoại *Saved version / Your version* sau **Update all** | tab đang mở giữ nội dung cũ | chọn **Saved version** nếu Git là chuẩn; tốt hơn là đóng tab trước khi Update all |
| Git sync pipeline báo `Object reference not set to an instance of an object` | artifact hoặc reference chưa bind, hoặc JSON pipeline không hợp lệ | kiểm tra JSON hợp lệ; activity loại `TridentNotebook`; `notebookId` trỏ đúng notebook; binding do Git sync tạo. Sau sync, mở pipeline và Validate trước khi Run; lưu Activity Output hoặc Request ID nếu vẫn lỗi |
| Pipeline không tự lưu; canvas trống hoặc mất tham số sau khi quay lại | thay đổi chưa Save bị bỏ | Ctrl+S sau mỗi activity; commit sớm |
| Banner "Upgrade to Runtime 2.0" dù workspace đã dùng | thông báo chung hoặc UI chưa refresh | kiểm tra Spark settings thực tế, không đổi runtime chỉ vì banner |
| Markdown bị gộp khi Git sync | editor Fabric diễn giải dòng trống và heading theo ranh giới cell | chia thành các Markdown cell riêng; xem preview sau sync |
| Source và Platform ở workspace khác nhau | ranh giới có chủ đích; thêm item vào Explorer của Platform không cấp quyền đọc Lakehouse ở Source | kiểm tra đúng workspace và item trong tham số pipeline, và quyền đọc OneLake của identity chạy pipeline |
| ANSI tuỳ runtime/phiên | Spark 4.1.1 trên Fabric đo ở K05 có `ansi = false`: `CAST` hỏng ra NULL, `sum(bigint)` tràn quay vòng im lặng; nếu bật thì `CAST` lỗi | dùng `try_cast`, `try_to_timestamp`; checksum dùng `decimal(38,0)` |

## Pipeline và tham số

| Hiện tượng | Nguyên nhân | Xử lý |
|---|---|---|
| Pipeline Failed nhưng Output hiện `0 items`, không thấy Spark job | activity lỗi trước khi submit notebook, hoặc đang xem tab không có chi tiết executor | mở run, chọn activity, xem Error và Output; nếu notebook đã được gọi thì mở run của notebook, xem traceback và driver logs |
| Pipeline Failed nhưng không rõ bước nào | mỗi bước là một activity; activity sau activity lỗi bị bỏ qua | mở run trên Monitor, tìm activity Failed đầu tiên; với notebook, mở run của notebook để xem traceback; `meta.v_run_latest` cho `reason` |
| Tham số sai định dạng, pipeline không nhận entity | tham số là chuỗi hoặc boolean theo khai báo, không phải JSON | ví dụ `p_scope=reference.categories` (chuỗi), `p_mode=normal`, `p_load_date=2026-01-01`, `p_full_reload=false`. Không bọc cả bộ tham số thành JSON |
| Chạy một entity đơn lẻ | pipeline nhận `p_scope` | chạy `pl_master_daily` với `p_scope` là source ID, `p_mode=normal`, `p_load_date` đúng ngày batch |
| "Validated 9 contracts, 5 silver, 5 gold, N checks" mà không thấy pipeline | đó là kết quả kiểm tra cấu hình, không phải một pipeline run | chạy `pl_master_daily` |

## Ingest và Landing

| Hiện tượng | Nguyên nhân | Xử lý |
|---|---|---|
| `PathNotFound` khi đọc thư mục drop zone | `physicalName` trong contract không khớp thư mục thật, hoặc sai workspace/Lakehouse nguồn | xác nhận workspace `CompanyA-Source`, Lakehouse `lh_retail_drop`, thư mục thật và quyền đọc; sửa `physicalName` trong contract (PATCH) nếu nguồn đổi layout, sinh lại `nb_contracts` |
| Landing thiếu entity sau lần chạy trước | activity có thể fail trước khi tạo delivery và manifest cho entity đó | xem activity hoặc notebook run lỗi trước; sửa nguyên nhân rồi chạy lại entity đó (`normal`, `p_scope` = source ID). Không cần xoá Landing cũ; xác nhận manifest mới xuất hiện |
| Thấy `landing/wholesale` và hỏi có phải shortcut | Landing là vùng **bản sao** do ingest tạo, không phải shortcut trỏ nguồn | giữ ingest copy vào `Files/landing/<YYYYMMDD>/<source>/<entity>/run=<delivery_id>/`; không thay bằng shortcut |
| Run xong nhưng Files trống | Explorer chưa làm mới | Refresh trên thư mục Files |
| Lookup hoặc câu lệnh T-SQL trên lakehouse thấy bảng cũ hoặc báo thiếu bảng mới | SQL analytics endpoint chưa đồng bộ bảng vừa tạo bằng Spark | mở `lh_platform`, chuyển sang SQL analytics endpoint, Refresh, chờ khoảng một phút |

## Ghi chú vận hành

- Đổi notebook binding hoặc nội dung Git: lưu, commit và sync đúng nhánh, đóng và mở lại tab đang giữ bản cũ trước khi chạy.
- Một phiên Spark mỗi lúc; xong thì dừng session.
- `STARTED` trong `meta.run_log` mà không có kết thúc nghĩa là session chết hoặc bị dừng; nguyên nhân nằm trong Monitoring hub, không phải trong `meta`.
- Không sửa tay bảng `meta` hoặc `ctl` trên Fabric: cấu hình sửa trong code và sync; audit do notebook ghi.
