# 08 — Data quality & reconciliation

> DQ và recon là **một phần của pipeline**, chạy mỗi lượt, có ngưỡng và hành động — không phải tài liệu kiểm tay.

## 1. Ba mức xử lý

| Mức | Khi nào | Hành động |
|---|---|---|
| **Quarantine** | Dòng không parse được / thiếu khoá bắt buộc / giá trị không nhận ra | Dòng sang `quarantine_<entity>`; batch vẫn chạy |
| **Flag** | Dòng dùng được nhưng đáng ngờ | Ở lại Silver với `dq_flags` |
| **Fail batch** | Lỗi toàn cục: trùng khoá fact, schema không tương thích, recon vượt ngưỡng, tỉ lệ quarantine vượt ngưỡng | Dừng pipeline, cảnh báo |

## 2. Rule

Khai báo trong notebook `nb_dq` (danh sách: `rule_id, layer, table, điều kiện dòng lỗi, severity, threshold`). Runner gọi sau mỗi layer; kết quả vào `meta.dq_result`.

| Layer | Rule mẫu | Severity · ngưỡng |
|---|---|---|
| Bronze | freshness: entity có batch trong ngày (file retail orders) | warning |
| Bronze | số dòng batch lệch > 3× trung bình 7 ngày | warning |
| Silver | khoá not null & unique theo grain | critical · 0 |
| Silver | tỉ lệ quarantine theo batch | critical nếu > 5% |
| Silver | orphan FK | warning (flag) |
| Silver | outlier qty / price | warning (flag) |
| Gold | fact không trùng grain, không FK NULL | critical · 0 |
| Gold | SCD2 không chồng lấp, mỗi khoá đúng 1 bản current | critical · 0 |

Danh sách đầy đủ lấy từ `dq_findings.md` (Bước 2).

## 3. Reconciliation

Khai báo trong `nb_ops_recon`; kết quả vào `meta.reconciliation_result` với `tolerance`.

| Phép đối soát | Grain | Tolerance |
|---|---|---|
| Source ↔ Bronze: số dòng trong cửa sổ (Copy rowsRead vs Bronze) | batch | 0 |
| Bronze → Silver: `vào = ra + quarantine + bỏ do dedup` | batch | 0 |
| Silver ↔ Gold: số đơn, số dòng, tổng `gross_amount` đã ghi nhận | ngày, tháng | 0 |
| Gold ↔ aggregate tháng | tháng | 0 |
| Gold ↔ report (DAX query) | tháng | 0 |
| Theo thuế: tổng `tax_amount` | tháng | 0,01 |

Không chỉ so count: luôn so **count + amount**, theo **ngày/tháng**, kèm số dòng quarantine.

## 4. Cảnh báo
- Cuối mỗi lượt: email/Teams tóm tắt (rule WARN, số quarantine, recon).
- Fail batch: pipeline fail → email/Teams cảnh báo + link Monitoring.
- `rpt_pipeline_health`: xu hướng DQ/recon theo ngày.

## Quyết định
- Rule và recon nằm trong code (notebook), kết quả trong bảng.

## Còn mở
- Ngưỡng cụ thể (5%, 3×) — điều chỉnh sau khi có số liệu thật vài ngày.
