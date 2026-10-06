# Tài liệu thiết kế

> **PLAN.md** = tổng quan + quyết định + roadmap. **design/** = thiết kế chi tiết từng phần (cái gì, vì sao, quy tắc, trường hợp biên). **guide/** = thao tác click-by-click.
> Mỗi file kết thúc bằng mục **Quyết định** và **Còn mở** để biết chỗ nào đã chốt, chỗ nào cần kiểm.

| # | File | Trả lời |
|---|---|---|
| 01 | [source-contract.md](01-source-contract.md) | Mỗi entity nguồn: grain, khoá, cập nhật/xoá, thời gian, tần suất, lỗi đã biết |
| 02 | [simulator.md](02-simulator.md) | Simulator đang làm gì, cần thêm những tình huống lỗi nào |
| 03 | [ingest.md](03-ingest.md) | Lấy dữ liệu ERP và file thế nào; cửa sổ nguồn; chạy lại vs xử lý lại |
| 04 | [control-tables.md](04-control-tables.md) | Bảng `meta`: config, run log, batch, watermark, DQ, recon — cột và luồng ghi |
| 05 | [bronze.md](05-bronze.md) | Bronze lưu gì, kiểu dữ liệu, schema drift |
| 06 | [silver.md](06-silver.md) | Parse → chuẩn hoá → khoá/grain → dedup → hợp nhất; quarantine/flag; history |
| 07 | [gold-semantic.md](07-gold-semantic.md) | Grain fact, dimension, SCD2, semantic model |
| 08 | [dq-recon.md](08-dq-recon.md) | Rule, mức xử lý, ngưỡng, đối soát |
| 09 | [orchestration-ops.md](09-orchestration-ops.md) | Pipeline, log, cảnh báo, khôi phục, retention, hiệu năng (đo trước) |
| 10 | [environments-cicd.md](10-environments-cicd.md) | Ranh giới Source/Platform vs vòng đời Dev/Test/Prod; Git, promote, cấu hình theo môi trường |

**Hai khái niệm tách biệt (đừng gộp):**
- **Source ↔ Platform** = ranh giới **hệ thống** (team nguồn vs team dữ liệu).
- **Dev → Test → Prod** = vòng đời **triển khai** của Platform.
