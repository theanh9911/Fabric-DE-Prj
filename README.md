# Fabric-DE-Prj — Company A Sales Data Platform

Mô phỏng một dự án Data Engineering trên Microsoft Fabric theo cách làm production: nguồn ERP và file bán lẻ do simulator sinh, platform ba tầng Landing → Silver → Gold, contract nguồn theo ODCS, kiểm tra chất lượng và đối soát ở mọi lượt chạy.

```text
CompanyA-Source (chỉ đọc)                 CompanyA-DataPlatform-<Dev|Prod> / lh_platform
  sqldb_erp_wholesale ─┐                    Files/landing   raw bất biến + manifest
  lh_retail_drop ──────┼── Copy ───────▶    slv             conform, lịch sử, quarantine
  simulator (lh_sim) ──┘                    gld             dimension + fact
                                            meta            nhật ký, trạng thái, check
```

## Bắt đầu

- Luật và cách làm việc: [CLAUDE.md](CLAUDE.md)
- Tổng quan kiến trúc: [docs/00-tong-quan.md](docs/00-tong-quan.md)
- Tài liệu: [docs/README.md](docs/README.md)
- Việc cần làm: [docs/06-tasks.md](docs/06-tasks.md)

## Cấu trúc repo

| Thư mục | Nội dung |
|---|---|
| `contracts/` | contract dữ liệu nguồn (ODCS YAML) — nguồn sự thật về dữ liệu nguồn |
| `fabric/source/` | workspace nguồn: SQL project ERP, Lakehouse, simulator |
| `fabric/platform/` | workspace platform: Lakehouse, notebook, pipeline |
| `docs/` | thiết kế, task, vận hành |
| `tools/` | công cụ local (`uv run tools/…`) |

## Workspace

| Workspace | Nhánh | Thư mục |
|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` |
