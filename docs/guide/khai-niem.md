# Khái niệm

> Tra cứu khi gặp từ lạ. Mỗi mục: nghĩa · ở dự án này là gì.

## Hai loại tách biệt

| Từ | Nghĩa | Ở dự án này |
|---|---|---|
| **Ranh giới hệ thống** | Team nguồn ↔ team dữ liệu; Platform chỉ đọc nguồn | `CompanyA-Source` ↔ `CompanyA-DataPlatform-*` |
| **Vòng đời triển khai** | Code đi Dev → Test → Prod | `…-Dev` → `…-Prod` (Test tuỳ chọn) |

## Nguồn

| Từ | Nghĩa | Ở dự án này |
|---|---|---|
| **Source contract** | Những gì Platform được phép giả định về nguồn: grain, khoá, cập nhật/xoá, thời gian | [design/01](../design/01-source-contract.md) |
| **Master data** | Dữ liệu danh mục: ai, cái gì — ít dòng, sửa tại chỗ | customers, products, sales_hierarchy, categories |
| **Giao dịch / event** | Việc đã xảy ra — nhiều dòng, tăng liên tục | orders |
| **Grain** | 1 dòng đại diện cho cái gì | orders: 1 sản phẩm × 1 đơn × 1 trạng thái |
| **Snapshot** | Ảnh **toàn bộ** bảng tại 1 thời điểm | file master retail; ERP master lấy full mỗi ngày |
| **Hard delete** | Dòng bị xoá hẳn ở nguồn | watermark không phát hiện được → so snapshot |
| **CDC** | Nguồn tự phát ra mọi thay đổi (cả xoá) | không có ở đây; lựa chọn ưu tiên ở dự án thật nếu nguồn hỗ trợ |
| **Virtual clock** | Ngày giả lập của simulator | đang ở **2026-01-01** |
| **Scenario** | Tình huống lỗi simulator cố ý tạo | late arrival, resend, missing file… ([design/02](../design/02-simulator.md)) |

## Ingest

| Từ | Nghĩa | Ở dự án này |
|---|---|---|
| **Metadata-driven** | Pipeline đọc danh sách nguồn từ bảng rồi lặp, không viết cứng | Lookup `cfg_source_entity` → ForEach |
| **Load strategy** | Cách lấy của 1 entity | `db_incremental` · `db_full_snapshot` · `file_new_or_changed` |
| **Source window** | Khoảng nguồn được đọc trong 1 lượt: `(window_start, window_end]` | ghi vào `ingestion_batch` |
| **Watermark** | Mốc "đã lấy tới đây" hiện hành | `watermark_state`; ERP = `max(updated_at)` đã lấy; file = `window_end` |
| **Lookback** | Lùi mốc để bắt dòng commit trễ | ERP orders: 1440 phút |
| **Settle** | Bỏ qua file vừa sửa sát giờ chạy (có thể đang ghi dở) | file: 5 phút |
| **`run_start`** | Giờ thật lúc bắt đầu lượt chạy; mốc trên chung | biến `v_run_start` |
| **Landing** | Bản sao **nguyên trạng** vừa kéo về; bất biến | `Files/landing/<source>/<entity>/load_date=…/batch=…/` |
| **normal / rerun / reprocess** | Lấy phần mới · đọc lại landing cũ (cùng input) · lấy lại 1 khoảng nguồn chỉ định | tham số `p_mode` |
| **Idempotent** | Chạy lại cùng input → cùng kết quả | rerun: xoá rồi ghi lại đúng `batch_id` |

## Ba định danh

| Từ | Nghĩa | Ví dụ |
|---|---|---|
| `run_id` | 1 lượt chạy pipeline | RunId của Fabric |
| `ingestion_batch_id` | 1 dòng log = 1 lần thử ingest 1 entity / 1 file | uuid |
| `batch_id` | **Dữ liệu** của 1 entity trong 1 lượt normal — rerun giữ nguyên để replay | `retail_orders_20260102_ab12cd34` |
| `load_date` | Ngày logic (giả lập) của lượt chạy | `2026-01-02` |

## Hai đồng hồ — đừng trộn

| Đồng hồ | Gồm | Dùng cho |
|---|---|---|
| **Đồng hồ nguồn** (virtual clock) | `updated_at`, `order_date` trong dữ liệu; `p_load_date` | Cửa sổ & watermark ERP, ngày nghiệp vụ |
| **Giờ thật UTC** | LastModified của file; `run_start`; `*_at_utc` trong log | Cửa sổ & watermark file, log vận hành |

## Layer

| Từ | Nghĩa |
|---|---|
| **Bronze (`brz`)** | Bằng chứng nguồn dạng bảng: ERP giữ kiểu, CSV là chữ; + cột kỹ thuật; append-only |
| **Silver (`slv`)** | Parse → chuẩn hoá → grain → dedup → hợp nhất nguồn; có quarantine |
| **Gold (`gld`)** | Star schema: dimension + fact + aggregate |
| **Schema drift** | Nguồn thêm/bớt/đổi kiểu cột; Bronze ghi nhận, downstream mở rộng có chủ đích |
| **Survivorship** | Khi 2 nguồn cùng 1 khách khác thuộc tính: chọn giá trị nào |
| **SCD1 / SCD2** | Ghi đè / giữ lịch sử theo phiên bản (`valid_from`, `valid_to`) |
| **Point-in-time join** | Fact nối dimension SCD2 theo phiên bản hiệu lực tại `order_date` |

## Chất lượng

| Từ | Nghĩa |
|---|---|
| **Quarantine** | Dòng không dùng được → bảng `quarantine_<entity>` kèm lý do |
| **Flag** | Dòng dùng được nhưng đáng ngờ → ở lại, cột `dq_flags` |
| **Fail batch** | Lỗi toàn cục → dừng pipeline |
| **Reconciliation (recon)** | Đối soát count + amount giữa 2 phía, có ngưỡng |

## Fabric

| Từ | Nghĩa |
|---|---|
| **Lakehouse** | `Tables/` (Delta) + `Files/` (file bất kỳ) |
| **SQL analytics endpoint** | Cổng T-SQL **chỉ đọc** của lakehouse; đồng bộ trễ vài giây–phút |
| **Connection** | Địa chỉ nguồn + thông tin đăng nhập; không nằm trong Git |
| **Dependency (mũi tên)** | ✓ Succeeded · ✗ Failed · ↷ Skipped · → Completed. **Mặc định dùng ✓** |
| **Dynamic content** | Giá trị tính lúc chạy, bắt đầu `@` |
| **`item()`** | Dòng hiện tại trong ForEach |
| **Parameter cell** | Cell chứa giá trị mặc định tham số notebook; pipeline ghi đè |
| **`%run` / `runMultiple`** | Dùng chung code cùng session / chạy nhiều notebook chung 1 session |
| **Standard / high-concurrency session** | `%%configure` chỉ chạy ở standard |
| **Variable Library** | Giá trị khác nhau theo môi trường (Dev/Prod) |
| **Retention / VACUUM** | VACUUM xoá file cũ → mất khả năng RESTORE về version cũ |
