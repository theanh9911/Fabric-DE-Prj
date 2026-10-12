# Lỗi đã gặp — Bước 1

| Lỗi / hiện tượng | Nguyên nhân | Sửa |
|---|---|---|
| Không tìm thấy *SQL analytics endpoint* của `lh_platform` trong OneLake catalog | Pipeline không liệt kê endpoint riêng | Chọn connection **Lakehouse** → Root folder **Tables** → Use query **T-SQL Query** (chạy qua endpoint) |
| `Invalid connection credentials` khi tạo connection SQL Server | Connection tạo ra chưa Sign in | Không cần connection này nữa (`cn_lh_platform_sql`) — xoá ở Settings → Manage connections and gateways |
| `Invalid object name 'meta.cfg_source_entity'` | SQL analytics endpoint chưa đồng bộ bảng mới tạo bằng Spark | Mở `lh_platform` → đổi sang **SQL analytics endpoint** → **Refresh** → đợi ~1 phút |
| Mũi tên nối khối có điều kiện **Skipped** / **Completed** | Kéo nhầm từ ô ↷ (Skipped) hoặc → (Completed) | Xoá mũi tên, kéo lại từ ô **✓ xanh lá** (Succeeded). Kiểm bằng `{ }` → `dependencyConditions` |
| Biểu thức có `\n` ở cuối (`"@utcNow(...)\n"`) | Nhấn Enter khi dán vào ô dynamic content | Xoá dòng trống cuối trong ô |
| `Saving error: 'sw_source_type' should have at least one Activity` | Case của Switch không được để trống | Tạm thêm **Wait** 1 giây vào case trống |
| Canvas trống, mất Parameters sau khi quay lại | Pipeline **không tự lưu**; thay đổi chưa Save bị bỏ | Ctrl+S sau mỗi activity; Commit sớm |
| Bấm Preview data ở Copy → hộp thoại hỏi giá trị `item()…` | Preview cần giá trị thật cho biểu thức | Cancel — kiểm bằng cách Run thật |
| Run xong nhưng Files trống | Explorer chưa làm mới | **…** cạnh Files → **Refresh** |
| `PathNotFound` / `FileNotFoundException` khi đọc `inbound/reference/categories` | Cấu hình canonical trỏ tới `Files/inbound/reference/categories`, trong khi workspace đã triển khai có thể đang để file ở `Files/inbound/categories`. Các workspace/source phiên bản khác nhau có thể có layout khác nhau. | Bản `nb_ingest` commit `b5c787c` thử path canonical trước, rồi fallback sang `inbound/<entity>`. Sync/publish notebook này vào workspace trước khi retry. Nếu vẫn lỗi, xác nhận đúng workspace `CompanyA-Source`, Lakehouse `lh_retail_drop`, và folder/file thực tế; kiểm tra quyền đọc. |
| `Notebook execution failed ... HTTP status code 200` kèm `MagicUsageError` | HTTP 200 là status của dịch vụ notebook; notebook vẫn có thể ở trạng thái Failed. `errorName`/traceback mới là nguyên nhân. | Đọc `errorName`, `message`, run ID và cell lỗi. Với `%run cannot run with other code or magic commands`, tách `%run` thành cell riêng; không retry chỉ dựa vào HTTP status. |
| `AnalysisException: WRONG_COMMAND_FOR_OBJECT_TYPE` — `DROP VIEW` yêu cầu VIEW nhưng object là `MANAGED` | Script cleanup gọi lệnh drop sai loại object; bảng managed cần `DROP TABLE`. | Xem loại object trước khi drop; dùng `DROP TABLE IF EXISTS` cho bảng hoặc `DROP VIEW IF EXISTS` cho view. Không bắt lỗi drop view rồi giả định object đã bị xóa. |
| `Validated 9 source entities and 6 Gold tables` nhưng không thấy pipeline | Đây là kết quả kiểm tra cấu hình (`nb_config`), không phải một pipeline run. | Chạy `pl_master_daily` để ingest/process. Trong pipeline hiện tại chỉ có một Notebook activity gọi `nb_process`; các notebook con chạy bên trong activity đó. |
| Landing thiếu entity sau lần chạy trước | Activity có thể đã fail trước khi tạo delivery/manifest cho entity đó; landing theo batch/entity/run là immutable. | Xem activity/notebook run lỗi trước. Sau khi sửa nguyên nhân, chạy lại entity đó qua `pl_master_daily` (`p_mode=normal`, `p_from_layer=ingest`, `p_entities=<source>.<entity>`). Không cần xóa landing cũ; xác nhận manifest mới xuất hiện ở batch dự kiến. |
| Thấy `landing/wholesale` và hỏi có shortcut hay không | Shortcut là pointer tới dữ liệu nguồn, không phải daily copy. Landing là vùng bản sao mà ingest tạo ra. | Với luồng giả lập daily sync, giữ ingest copy vào `Files/landing/<YYYYMMDD>/<source>/<entity>/run=<delivery_id>/`. Không tạo shortcut thay cho delivery copy; chỉ dùng shortcut nếu chủ đích là đọc trực tiếp nguồn. |

### Tham số chạy entity đơn lẻ (luồng hiện tại)

Chạy pipeline **`pl_master_daily`**, không chạy `nb_ingest` trực tiếp vì notebook đó nhận plan JSON do orchestrator sinh.

| Parameter | Giá trị mẫu |
|---|---|
| `p_mode` | `normal` |
| `p_from_layer` | `ingest` |
| `p_entities` | `reference.categories` (chuỗi, không phải JSON array/object) |
| `p_load_date` | ngày batch dạng `YYYY-MM-DD`, ví dụ `2025-12-31` |
| `p_full_reload` | `false` |

Sau khi chạy, xem pipeline Activity Output và notebook child run. Chỉ khi run thành công mới kiểm tra `Files/landing/<YYYYMMDD>/reference/categories/run=<delivery_id>/` và `_manifest.json`.
