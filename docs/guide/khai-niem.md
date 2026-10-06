# Khái niệm

> Tra cứu khi gặp từ lạ. Mỗi mục: nghĩa · ở dự án này là gì.

## Kiến trúc dữ liệu

| Từ | Nghĩa | Ở dự án này |
|---|---|---|
| **Source** | Hệ thống của team khác, Platform chỉ được **đọc** | ws `CompanyA-Source`: ERP `sqldb_erp_wholesale` + drop zone file `lh_retail_drop` |
| **Simulator** | Notebook giả lập Source phát sinh dữ liệu mỗi ngày | `nb_00_sim_daily` — mỗi lần chạy tiến 1 ngày giả lập |
| **Virtual clock** | Ngày giả lập, độc lập ngày thật | hiện đang ở **2026-01-01** |
| **Landing** | Bản sao **nguyên trạng** dữ liệu vừa kéo về, không sửa, không xoá | `lh_platform/Files/landing/<source>/<entity>/load_date=…/batch=…/` |
| **Bronze (`brz`)** | Bảng Delta chứa dữ liệu thô từ landing, **mọi cột là chuỗi** + cột kỹ thuật | `brz.brz_<source>_<entity>` |
| **Silver (`slv`)** | Dữ liệu đã làm sạch, chuẩn hoá, gộp nguồn | `slv.slv_<entity>` |
| **Gold (`gld`)** | Mô hình cho báo cáo (dim/fact) | `gld.gld_d_*`, `gld.gld_f_*` |
| **`meta`** | Bảng điều khiển & theo dõi pipeline (không phải dữ liệu nghiệp vụ) | `cfg_*` config · `ref_*` luật · `state_*` mốc đã lấy · `log_*` nhật ký |

## Ingest

| Từ | Nghĩa | Ở dự án này |
|---|---|---|
| **Metadata-driven** | Pipeline không viết cứng từng nguồn; **đọc danh sách nguồn từ bảng config** rồi lặp | Lookup `meta.cfg_source_entity` → ForEach |
| **Incremental** | Mỗi lần chỉ lấy **phần mới/đổi** từ lần trước | ERP: `updated_at > watermark` · File: `LastModified > watermark` |
| **Watermark** | Mốc "đã lấy tới đây" của từng nguồn | `meta.state_watermark`; chưa có → `1900-01-01` (lấy hết) |
| **Lookback** | Lùi watermark 1 chút để không sót dòng commit trễ | `lookback_days = 1` cho ERP |
| **`run_start`** | Mốc trên cố định của 1 lần chạy: lấy `(watermark, run_start]` | biến `v_run_start = utcNow()` đầu pipeline |
| **Batch** | 1 lần kéo 1 entity = 1 batch, có id riêng | thư mục `batch=<RunId>`; cột `_batch_id` ở Bronze |
| **Full snapshot** | Nguồn gửi **toàn bộ** bảng mỗi lần có thay đổi (không phải phần thay đổi) | file master của retail/reference |
| **Idempotent** | Chạy lại cùng tham số → cùng kết quả, không nhân đôi | Bronze: xoá theo `_batch_id` rồi chèn lại |

## Fabric

| Từ | Nghĩa |
|---|---|
| **Workspace (ws)** | "Thư mục dự án" trên Fabric; mỗi ws gắn 1 nhánh Git + 1 thư mục trong repo |
| **Lakehouse** | Kho gồm `Tables/` (bảng Delta) + `Files/` (file bất kỳ) |
| **SQL analytics endpoint** | Cổng **chỉ đọc** bằng T-SQL vào bảng của lakehouse; tự sinh kèm mỗi lakehouse; **đồng bộ trễ** vài giây–phút so với bảng Delta |
| **Connection** | Đối tượng lưu *địa chỉ nguồn + thông tin đăng nhập*; pipeline dùng lại; không nằm trong code/Git |
| **Pipeline activity** | 1 khối trên canvas: Set variable, Lookup, ForEach, Switch, Copy, Notebook, Fail… |
| **Dependency (mũi tên)** | Điều kiện chạy khối sau: ✓ **Succeeded** · ✗ Failed · ↷ Skipped · → Completed. **Luôn dùng ✓** trừ khi cố ý |
| **Dynamic content** | Giá trị tính lúc chạy, bắt đầu bằng `@` (vd `@item().entity`) |
| **`item()`** | Dòng hiện tại trong ForEach (1 dòng của Lookup) |
| **Standard / High-concurrency session** | Phiên Spark của notebook. `%%configure` **chỉ chạy ở standard** |
