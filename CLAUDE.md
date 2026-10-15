# CLAUDE.md — Luật và cách làm việc

Dành cho AI agent và kỹ sư làm việc trên repo này. Thiết kế, task và vận hành nằm ở [docs/README.md](docs/README.md); file này chỉ chứa luật và quy tắc làm việc.

## 1. Dự án

Sales Data Platform trên Microsoft Fabric, mô phỏng dự án thật. Nguồn: ERP bán buôn (Fabric SQL Database) và file CSV bán lẻ (drop zone OneLake), do simulator ở workspace `CompanyA-Source` sinh ra. Platform đi qua ba tầng **Landing → Silver → Gold**, với schema `meta` cho vận hành. Contract nguồn ở `contracts/` (ODCS YAML).

Trạng thái: tài liệu đã xong (v2); code trong `fabric/platform/` vẫn là v1 cũ, sẽ được viết lại theo [docs/06-tasks.md](docs/06-tasks.md).

## 2. Đọc gì trước khi làm

1. [docs/00-tong-quan.md](docs/00-tong-quan.md) và [docs/01-nguyen-tac.md](docs/01-nguyen-tac.md) — một lần, trước task đầu tiên.
2. [docs/06-tasks.md](docs/06-tasks.md) — mỗi phiên.
3. Các mục của [docs/04-kien-truc.md](docs/04-kien-truc.md), [docs/05-thu-vien.md](docs/05-thu-vien.md), [docs/03-contract.md](docs/03-contract.md) mà task trỏ tới.

## 3. Luật (bất biến)

| # | Luật | Nghĩa |
|---|---|---|
| L1 | Nguồn chỉ đọc | Platform không tạo bảng, không ghi dữ liệu vào workspace `CompanyA-Source`. |
| L2 | Landing là raw duy nhất và bất biến | Lưu nguyên byte theo `Files/landing/<batch_id>/<source_system>/<entity>/run=<delivery_id>/` kèm `_manifest.json`. Không ghi đè, không xoá (ngoại lệ duy nhất: reset Dev). Mọi tầng dựng lại được từ Landing. |
| L3 | Mọi lệnh ghi idempotent | Ghi bằng MERGE hoặc overwrite có phạm vi. Chạy 1 lần hay 10 lần với cùng input cho cùng kết quả. |
| L4 | Watermark chỉ tiến khi thành công | Watermark lấy từ delivery đã nạp Silver thành công. |
| L5 | Hỗ trợ `normal` và `rerun` | `normal` lấy mới từ nguồn; `rerun` xử lý lại dữ liệu đã có ở Landing mà không chạm nguồn. |
| L6 | Không nuốt lỗi, không mất dữ liệu | Dòng lỗi vào `slv.quarantine` kèm lý do; lỗi vận hành ⇒ FAILED và cảnh báo. |
| L7 | Báo cáo chỉ cập nhật khi toàn chuỗi PASS | Check critical FAIL thì không làm mới bất cứ thứ gì đọc từ Gold. |
| L8 | Đối soát ở mọi ranh giới | Đối chiếu số dòng và số tiền Nguồn ↔ Landing ↔ Silver ↔ Gold, bằng hai số tính độc lập. |
| L9 | Mở rộng bằng khai báo | Thêm nguồn = thêm contract (và mapping nếu entity mới); không viết lại pipeline. |
| L10 | Chỉ chạy phần có thay đổi | Bỏ qua bước khi input không đổi (checkpoint, `logic_version`); không lấy lại file đã xử lý. |
| L11 | Git là nguồn sự thật | Không hardcode môi trường; không có secret trong Git. |
| L12 | Đo trước khi tối ưu | Đo thời gian chạy, số file Delta, lỗi 430 trước khi song song hoá hay partition. |
| L13 | Trạng thái do chủ của nó ghi | Pipeline điều phối thứ tự và lỗi; mỗi notebook chỉ ghi trạng thái của chính nó ([docs/04-kien-truc.md §4.6](docs/04-kien-truc.md#46-ai-ghi-bảng-nào)). Người đổi trạng thái delivery chỉ qua `nb_ops_delivery`. |

## 4. Vòng lặp một task

```text
1. Mở docs/06-tasks.md, chọn task TODO đầu tiên mà mọi phụ thuộc đã DONE.
2. Đọc các mục tài liệu task trỏ tới và code hiện có của các file trong "File".
3. Đổi trạng thái task → IN_PROGRESS.
4. Nhánh feat/<task-id>-<mô-tả> tách từ dev.
5. Làm đúng "Việc làm"; chỉ sửa file liệt kê. Cần sửa file khác ⇒ dừng, hỏi người.
6. uv run tools/check_repo.py (khi đã có).
7. Sửa tài liệu bị ảnh hưởng trong cùng commit.
8. Ghi WORKLOG (mẫu trong docs/WORKLOG.md), kèm các bước kiểm chứng trên Fabric.
9. Commit "<task-id>: <tóm tắt>"; trạng thái → REVIEW.
10. Người chạy kiểm chứng, dán kết quả vào WORKLOG. Đạt ⇒ DONE; không đạt ⇒ BLOCKED kèm lý do.
```

## 5. Quy tắc làm việc

| # | Quy tắc |
|---|---|
| W1 | Một task một nhánh; không gộp task. |
| W2 | Không đoán: tên bảng, cột, path, tham số phải khớp tài liệu. Tài liệu thiếu hoặc sai ⇒ sửa tài liệu trước, trong cùng commit. |
| W3 | Đọc Landing, ghi Delta, CDF, quarantine, audit đều qua helper trong `nb_common`/`nb_std`; không viết MERGE/overwrite tay trong worker. |
| W4 | Biến đổi dữ liệu viết bằng Spark SQL hoặc Column expression; Python chỉ điều phối. |
| W5 | Không đổi tên hay ý nghĩa của tham số pipeline, cột bảng `meta`/`slv`/`gld` ngoài những gì tài liệu quy định. |
| W6 | Không hardcode workspace ID, item ID, đường dẫn abfss, secret. |
| W7 | Không sửa tay khối `# META` do Fabric sinh; không sửa tay `nb_contracts`. `logicalId` mới sinh bằng `uv run python -c "import uuid; print(uuid.uuid4())"`. |
| W8 | Đổi logic một object ⇒ tăng `logic_version` của nó trong `nb_config`, cùng commit. |
| W9 | Hỏi người khi: cần một quyết định nghiệp vụ chưa có trong [docs/DECISIONS.md](docs/DECISIONS.md); thao tác phá huỷ (DROP, xoá file, reset); task mâu thuẫn tài liệu. |
| W10 | Báo cáo trung thực: chưa chạy trên Fabric thì ghi "chưa kiểm chứng". |
| W11 | Mọi script Python local chạy bằng `uv run`. |
| W12 | Comment chỉ giải thích lý do không hiển nhiên; không để code, cấu hình hay tài liệu không dùng tới. |
