# Tài liệu dự án — mục lục

Bộ tài liệu này là **nguồn sự thật duy nhất** về thiết kế, cách làm và cách vận hành Sales Data Platform trên Microsoft Fabric. Mỗi thông tin chỉ được viết ở một chỗ; nơi khác chỉ trỏ tới. Luật và quy tắc làm việc cho agent nằm ở [../CLAUDE.md](../CLAUDE.md).

## Đọc theo thứ tự

| # | File | Nội dung | Đọc khi |
|---|---|---|---|
| 0 | [00-tong-quan.md](00-tong-quan.md) | bài toán, sơ đồ kiến trúc, ba tầng, mô hình dữ liệu, một lượt chạy, cách xử lý từng tình huống nguồn, vì sao thiết kế như vậy | đầu tiên, một lần |
| 1 | [01-nguyen-tac.md](01-nguyen-tac.md) | kim chỉ nam, cách ghi theo loại bảng, trách nhiệm từng tầng, đặt tên, điều cấm, Definition of Done, thuật ngữ, bài học từ v1 | trước task đầu tiên; khi phân vân thiết kế |
| 2 | [02-nguon.md](02-nguon.md) | hệ thống nguồn, simulator, đồng hồ ảo, notebook tiêm lỗi | khi làm ingest, EDA, kiểm thử |
| 3 | [03-contract.md](03-contract.md) | contract ODCS: cấu trúc file, thuộc tính dùng, cách sinh `nb_contracts`, quy trình đổi contract | khi đọc/sửa `contracts/` |
| 4 | [04-kien-truc.md](04-kien-truc.md) | đặc tả chi tiết: path, manifest, DDL từng bảng, business rule, quarantine, điều phối, tham số, thuật toán, check, đối soát | trước mọi task code |
| 5 | [05-thu-vien.md](05-thu-vien.md) | `nb_config`, `nb_std`, `nb_common`: chữ ký và hành vi từng hàm; mẫu notebook/pipeline; công cụ local | khi viết code dùng chung hoặc gọi helper |
| 6 | [06-tasks.md](06-tasks.md) | backlog: trạng thái, phụ thuộc, việc làm, tiêu chí xong, cách kiểm chứng | mỗi phiên làm việc |
| 7 | [07-van-hanh-kiem-thu.md](07-van-hanh-kiem-thu.md) | dựng workspace, công thức vận hành, chẩn đoán, kiểm thử, thư viện SQL kiểm chứng, kịch bản lỗi | khi kiểm chứng, vận hành |
| — | [DECISIONS.md](DECISIONS.md) | mọi quyết định đã chốt và lý do | khi cần biết "vì sao" |
| — | [EDA.md](EDA.md) | khảo sát dữ liệu nguồn; kết quả điền vào contract và từ điển | task T0.3 |
| — | [WORKLOG.md](WORKLOG.md) | nhật ký từng task | cuối mỗi task |
| — | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | lỗi Fabric đã gặp và cách xử lý | khi gặp lỗi runtime |

## Nguồn sự thật theo loại thông tin

| Thông tin | Nằm ở | Không viết lại ở |
|---|---|---|
| Cột, kiểu, khoá, SLA, quality của dữ liệu nguồn | `contracts/<source_system>/<entity>.yaml` | docs, `nb_config` |
| Bảng, cột, path, tham số, rule của platform | [04-kien-truc.md](04-kien-truc.md) | code comment, notebook markdown |
| Giá trị cấu hình platform (Silver/Gold/check/từ điển) | `fabric/platform/nb_config` | docs (docs chỉ mô tả cấu trúc) |
| Chữ ký hàm dùng chung | [05-thu-vien.md](05-thu-vien.md) | |
| Việc cần làm và trạng thái | [06-tasks.md](06-tasks.md) | |
| Lý do một lựa chọn | [DECISIONS.md](DECISIONS.md) | |

## Trạng thái hiện tại (2026-10-14)

Code trong `fabric/platform/` đang được viết lại từng task theo [06-tasks.md](06-tasks.md); notebook còn là bản cũ (v1) sẽ bị viết đè ở task tương ứng. Tài liệu trong thư mục này mô tả thiết kế đích (v2), không mô tả v1.
