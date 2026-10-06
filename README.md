# Fabric-DE-Prj — Company A Sales Data Platform

Mô phỏng một dự án Data Engineering trên Microsoft Fabric (medallion, config-driven, incremental, DQ, SCD).
Hướng triển khai **SQL-first**: biến đổi dữ liệu bằng Spark SQL (`%%sql`), Python chỉ làm phần "keo dán".
Kế hoạch & nguyên tắc: [docs/PLAN.md](docs/PLAN.md).

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `fabric/source/` | Workspace `CompanyA-Source` (Git sync): ERP giả lập, drop zone, simulator |
| `fabric/platform/` | Workspace `CompanyA-DataPlatform-<Dev\|Prod>` (Git sync): `lh_platform`, notebook, pipeline |
| `docs/` | Plan, ADR, runbook |
