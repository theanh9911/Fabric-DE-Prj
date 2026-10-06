# 02 — Simulator

> Simulator chỉ làm việc của **hệ thống nguồn**: sinh/sửa dữ liệu nguồn theo ngày. Nó **không biết gì về Platform**: không đọc/ghi watermark, không chạy Silver/Gold.

## 1. Đang có (xong)

| Khả năng | Cách làm |
|---|---|
| Virtual clock | `lh_sim.sim_state.released_until`; mỗi lần `nb_00_sim_daily` tiến `p_days` ngày; exit value = ngày mới |
| Insert orders | Thả dòng có `_release_date` trong cửa sổ |
| "Update" orders | Đổi trạng thái = dòng mới với `updated_at` mới (theo seed) |
| Update master | ERP `MERGE`; file gửi snapshot đầy đủ |
| Dữ liệu bẩn | Có sẵn trong seed (định dạng ngày, status, tax, orphan, trùng, năm 2027) |
| Ràng buộc DB | NOT NULL / PK → `sim_reject_log` |
| Idempotent | Chạy lại cùng cửa sổ không nhân đôi |

**Thiếu:** mọi ngày đều "suôn sẻ" — file luôn đến đúng hạn, không trùng, không thiếu, schema không đổi. Chưa luyện được các tình huống ingest thật.

## 2. Cần thêm — tình huống lỗi (scenario)

Khai báo trong code của simulator (danh sách scenario theo ngày giả lập), bật/tắt bằng tham số `p_scenarios`.

| Scenario | Làm gì ở nguồn | Platform phải chứng minh | Câu đề |
|---|---|---|---|
| `late_arrival` | Ngày 2026-01-15 thả thêm đơn có `order_date = 2026-01-02` | Watermark (theo `updated_at`) vẫn bắt được; Gold tháng 1 cập nhật | Q14 |
| `order_update` | Sửa `quantity` của 1 đơn cũ + tăng `updated_at` | Silver current lấy bản mới; history giữ bản cũ | Q10 |
| `file_resend` | Ghi đè lại `orders_YYYYMMDD.csv` hôm trước (cùng tên, nội dung sửa) | Nhận ra phiên bản mới (modified/size đổi) → nạp lại, Silver không đếm đôi | — |
| `duplicate_file` | Gửi thêm bản copy khác tên của file hôm trước | Silver dedup theo khoá; DQ cảnh báo | — |
| `missing_file` | Bỏ qua 1 ngày không gửi file retail orders | DQ freshness cảnh báo; ngày sau gửi bù vẫn nạp đủ | — |
| `partial_file` | Ghi file thành 2 lần (giả lập đang ghi dở lúc pipeline đọc) | Biên an toàn `settle_minutes` bỏ qua file vừa sửa | — |
| `bad_rows` | Bơm qty NULL/âm, price 0, ngày sai locale | Quarantine / flag đúng mức | Q1, Q8 |
| `schema_change` | Từ 2026-06-01 thêm cột `discount_amount` (ERP + file) | Bronze ghi nhận cột mới + log drift; Silver/Gold chủ động mở rộng | Q16 |
| `org_change` | Giữa tháng 3 đổi quản lý của 1 team | SCD2: report tháng 3 đúng cơ cấu tại thời điểm | Q11 |
| `master_delete` | 1 sản phẩm biến mất khỏi snapshot retail | Phát hiện xoá bằng so snapshot | — |
| `generate` | Hết seed (~2026-05-10) → sinh đơn mới theo phân phối cũ | Chạy liên tục > 7 ngày | S1 |

## 3. Nguyên tắc
- Mỗi scenario **tất định** (cùng ngày → cùng kết quả) để drill lặp lại được.
- Mọi scenario ghi vào `sim_release_log` (cột `scenario`) → biết "đáp án" để đối chiếu với điều Platform phát hiện.
- Reset Source → Platform cũng phải reset (state riêng).

## Quyết định
- Làm scenario **sau** khi ingest happy-path chạy (Bước 1) — để có pipeline mà thử lỗi.

## Còn mở
- Thứ tự làm scenario: theo nhu cầu drill (Bước 6–10).
