# Lỗi Fabric đã gặp — Spark, notebook và workspace

> Nhật ký các lỗi đã xuất hiện khi dựng và chạy platform trên Fabric. Luồng hiện tại bỏ `%%configure -f`; không áp dụng hướng dẫn lịch sử yêu cầu tắt High Concurrency để chạy magic này.

## Capacity và Spark session

| Lỗi / triệu chứng | Nguyên nhân / cách hiểu | Cách xử lý |
|---|---|---|
| `TooManyRequestsForCapacity` · HTTP 430 | Capacity đã chạm giới hạn Spark tổng thể. Đây là giới hạn compute của capacity, không nhất thiết do workspace hiện tại có nhiều job; màn hình workspace có thể hiện Active = 0 nhưng workload ở workspace khác vẫn chiếm capacity. | Monitoring hub → lọc các Spark jobs đang chạy/queued trên các workspace có quyền xem; huỷ job treo nếu phù hợp, đóng notebook/session thừa, đợi capacity nhả compute rồi thử lại. Nếu vẫn liên tục gặp, cần giảm tải hoặc tăng SKU. Đừng bấm Run nhiều lần liên tiếp. |
| `Kernel is not found`, WebSocket close code 1000, notebook chuyển `Stopped` | Kernel/session đã bị dừng hoặc không được cấp sau lỗi capacity; tab notebook còn mở nhưng kernel ID cũ không còn. | Đóng/reopen notebook hoặc tạo session mới khi capacity cho phép; chạy lại sau khi xác nhận session sẵn sàng. Không xem đây là lỗi code nếu chưa có traceback Python/Spark trước đó. |
| Chỉ chạy được một cell; cell kế tiếp báo 430 | Cell đầu đã chạy được, nhưng job/session tiếp theo bị capacity chặn. | Mở Monitoring để kiểm tra job/session, rồi chạy tiếp khi capacity cho phép. Hạn chế mở nhiều notebook và bấm Connect/Run All đồng thời. |
| `ShutdownHook ... TimeoutException`, `no active spark session found`, log token/SAS, cuối log `Shutting down` | Các dòng này có thể xuất hiện lúc Spark session đang dừng; riêng chúng không chỉ ra nguyên nhân gốc. | Trong Run details, tìm exception đầu tiên ở driver stderr/stdout hoặc traceback của cell/activity. Nếu không có exception trước shutdown, lấy run ID + timestamp và xem lại trạng thái capacity/kernel. |

## Notebook magic, imports và Lakehouse binding

| Lỗi | Nguyên nhân / cách hiểu | Cách xử lý |
|---|---|---|
| `MagicUsageError: %run cannot run with other code or magic commands.` | `%run` được đặt chung cell với code hoặc magic khác. | Đặt mỗi `%run nb_config` / `%run nb_common` trong cell riêng; không ghép Python code vào cùng cell. |
| `MagicUsageError` khi dùng `%%configure` trong High Concurrency; cần `-f`/restart Livy session | `%%configure -f` yêu cầu khởi động lại session và không phù hợp với High Concurrency; restart cũng tiêu thụ/thử cấp Spark session mới. | Các notebook platform hiện không dùng `%%configure`. Gắn `lh_platform` qua **Data items**, đặt làm **default Lakehouse**, bật Lakehouse Auto-Binding in Git, lưu binding metadata rồi sync. Tránh thêm lại `%%configure -f`. |
| `UnsupportedOperationException: No default context found, please attach a lakehouse before running spark sql queries with partial namespaces.` | Lệnh như `saveAsTable("meta.table_run")` dùng namespace tương đối nhưng notebook root không có default Lakehouse context. Chỉ thấy Lakehouse trong Explorer/chưa đặt default có thể chưa đủ. | Trong chính notebook chạy từ pipeline (hiện là `nb_process`), chọn `lh_platform` làm default Lakehouse, lưu và sync binding. Child notebooks cần kế thừa cùng default context; code hiện gọi `useRootDefaultLakehouse=True`. Sau khi đổi binding, mở phiên mới trước khi retry. |
| `FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME: DecimalType(38,4) can not accept object 0 in type int` khi `nb_dq` ghi `meta.check_result` | Schema khai báo `expected`, `actual`, `threshold` là `DECIMAL(38,4)`, nhưng các check thường truyền Python `int` (ví dụ `0`). PySpark không tự ép kiểu khi dùng schema tường minh. | `nb_common.log_check` chuyển các giá trị số sang `decimal.Decimal` trước `createDataFrame`; sync bản sửa của `nb_common`, mở session mới rồi rerun. Lần chạy trước có thể đã hoàn thành Gold nhưng chưa xong DQ/reconciliation. |
| `cannot import name 'fabric' from 'notebookutils'` | `fabric` không được export từ `notebookutils` trong runtime này. | Dùng `import sempy.fabric as fabric` rồi `fabric.FabricRestClient()`. |

## UI và Git integration

| Hiện tượng | Nguyên nhân / cách hiểu | Cách xử lý |
|---|---|---|
| Hộp thoại *Saved version / Your version* sau **Update all** | Tab đang mở giữ nội dung cũ chưa sync với Git. | Chọn bản **Saved version** nếu Git là nguồn chuẩn; tốt hơn đóng tab trước khi Update all rồi mở lại notebook/pipeline. |
| Git sync pipeline báo `Object reference not set to an instance of an object` | Lỗi tổng quát từ Fabric Git integration khi artifact/reference không được bind hoặc JSON không tương thích. | Theo checklist pipeline phía trên: validate JSON, `TridentNotebook`, notebookId và binding. Ghi lại Request/Activity ID nếu tiếp tục. |
| Banner đề nghị “Upgrade to Runtime 2.0” dù workspace đã đặt Runtime 2.0 | Banner có thể là thông báo chung hoặc chưa refresh UI. | Kiểm tra Spark settings thực tế của workspace; không đổi runtime chỉ dựa trên banner. |
| Bảng/định dạng Markdown bị gộp khi Git sync | Editor Fabric có thể diễn giải dòng trống/heading theo cell boundary khác với file Markdown thông thường. | Chia đoạn thành các Markdown cells riêng; sau sync xem lại preview trong notebook. |
| Workspace Source và Platform khác nhau | Đây là ranh giới workspace có chủ đích; thêm item vào Explorer của Platform không tự cấp quyền đọc Lakehouse ở Source. | Kiểm tra đúng source workspace/item trong pipeline parameters và quyền đọc OneLake cho identity chạy pipeline. Luồng daily sync phải copy file vào Landing; shortcut chỉ trỏ nguồn, không tạo delivery copy. |

## Workspace, Git và pipeline

| Lỗi / triệu chứng | Nguyên nhân / cách hiểu | Cách xử lý |
|---|---|---|
| Git sync báo `Object reference not set to an instance of an object` cho pipeline artifacts | Fabric Git integration có thể không xử lý được artifact/reference chưa bind hoặc pipeline JSON có reference không hợp lệ. Lỗi chung này không chỉ ra chính xác field lỗi. | Kiểm tra JSON của pipeline hợp lệ; activity dùng loại `TridentNotebook`; notebookId trỏ đúng notebook; workspace/item bindings được tạo bằng Git sync/Update all. Sau sync, mở pipeline và Validate trước khi Run. Lưu Activity Output/Request ID nếu lỗi vẫn còn. |
| Pipeline Failed nhưng Output hiện `0 items` / không thấy Spark job trong tab Jobs | Có thể activity lỗi trước khi submit notebook job hoặc UI đang xem tab Jobs không có executor detail. | Mở pipeline run → chọn activity → xem Error/Output; nếu notebook đã được gọi thì mở notebook run ID tương ứng và xem traceback/driver logs. Không kết luận chỉ từ `0 items`. |
| `pl_master_daily` chỉ có một activity | Đây là cấu trúc hiện tại có chủ đích: một `TridentNotebook` activity chạy `nb_process`; notebook này gọi plan/ingest/Silver/Gold bằng child notebooks trong cùng luồng. | Một activity là đúng với version hiện tại; kiểm tra child notebook runs/exit value và `nb_process` log để thấy các bước con. Nếu activity chính Failed, đọc lỗi từ notebook con đầu tiên bị fail. |
| Parameter nhập không đúng format / pipeline không nhận entity | Pipeline parameters là chuỗi/boolean theo khai báo, không phải JSON object tùy ý. | Ví dụ `p_entities = reference.categories` (string), `p_mode = normal`, `p_from_layer = ingest`, `p_load_date = 2026-01-01` (ISO date), `p_full_reload = false` (boolean). Không bọc cả bộ tham số thành JSON string; dùng đúng loại parameter UI hiển thị. |

## Ghi chú vận hành

- F4/Trial có ít Spark compute; High Concurrency giúp chia sẻ session trong điều kiện tương thích nhưng không làm tăng tổng capacity. Khi gặp 430, đổi notebook code không tự giải phóng capacity.
- Khi đổi notebook binding hoặc Git content: lưu thay đổi, commit/sync đúng branch và đóng/reopen tab đang giữ bản cũ trước khi chạy.
- Đính kèm Activity Run ID, Notebook Run ID, timestamp và exception đầu tiên khi nhờ chẩn đoán; ảnh chụp chỉ có trạng thái Failed thường chưa đủ để tìm root cause.
