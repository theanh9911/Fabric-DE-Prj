# Fabric-DE-Prj — Company A Sales Data Platform

Mô phỏng một dự án Data Engineering **thật** trên Microsoft Fabric — dựa trên đề *Data Engineer Case Study* — để luyện các năng lực production: tách hệ thống nguồn khỏi data platform, ingest chạy lại được và truy vết được, medallion (Bronze/Silver/Gold), DQ + đối soát, log + cảnh báo + khôi phục, Git + promote Dev → Prod.

Hướng triển khai **SQL-first**: biến đổi bằng Spark SQL; Python chỉ là keo dán; ingest bằng pipeline Copy.

**Trạng thái:** Source ✅ · Nền ✅ · Ingest ▶ — chi tiết ở [docs/PLAN.md](docs/PLAN.md#5-roadmap-end-to-end).

## Kiến trúc

```
CompanyA-Source (hệ thống "team khác", chỉ đọc)          CompanyA-DataPlatform-<Dev|Prod>
  ERP sqldb_erp_wholesale ─┐                               lh_platform
  Drop zone lh_retail_drop ┼── Pipeline Copy ──────────▶     Files/landing  (raw, bất biến)
  Simulator (virtual clock)┘                                  brz → slv → gld  (medallion)
                                                              meta            (config, watermark, log, DQ, recon)
                                                            Semantic model + report
```

## Tài liệu

| Đọc | Để |
|---|---|
| [docs/PLAN.md](docs/PLAN.md) | Mục tiêu, nguyên tắc, kiến trúc, đánh giá hiện trạng, roadmap |
| [docs/design/](docs/design/README.md) | Thiết kế chi tiết từng phần: source contract, simulator, ingest, control tables, Bronze, Silver, Gold, DQ/recon, vận hành, môi trường |
| [docs/guide/](docs/guide/README.md) | Thao tác click-by-click từng bước, kết quả mong đợi, lỗi đã gặp |

Người mới: đọc PLAN §1–4 → [guide/source.md](docs/guide/source.md) → [guide/khai-niem.md](docs/guide/khai-niem.md) → guide của bước đang làm.

## Cấu trúc repo

| Thư mục | Nội dung |
|---|---|
| `fabric/source/` | Workspace `CompanyA-Source` (Git sync): ERP (SQL project), drop zone, lakehouse simulator, notebook `nb_00_sim_*` |
| `fabric/platform/` | Workspace `CompanyA-DataPlatform-<Dev\|Prod>` (Git sync): `lh_platform`, notebook `nb_*`, pipeline `pl_*` |
| `docs/` | PLAN, design, guide |

## Môi trường

| Workspace | Nhánh | Thư mục |
|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` (nối ở bước promote) |
