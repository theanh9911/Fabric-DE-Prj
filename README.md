# Fabric-DE-Prj — Company A Sales Data Platform

Mô phỏng một dự án Data Engineering trên Microsoft Fabric (medallion, config-driven, incremental, DQ, SCD).
Kế hoạch & nguyên tắc: [docs/PLAN.md](docs/PLAN.md).

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `fabric/source/` | Workspace `CompanyA-Source` (Git sync): ERP giả lập, drop zone, simulator |
| `fabric/platform/` | Workspace `CompanyA-DataPlatform-<Dev\|Prod>` (Git sync) |
| `src/companya_de/` | Package Python dùng chung (build wheel → Fabric Environment) |
| `tests/` | Unit test (pytest + Spark/Delta local) |
| `docs/` | Plan, ADR, runbook |

## Chạy test (Docker — không cần cài Java/Spark trên máy)

```powershell
docker compose build test    # lần đầu hoặc khi đổi pyproject.toml / uv.lock
docker compose run --rm test # chạy pytest
```

Môi trường test khớp Fabric Runtime 2.0: Java 21, Python 3.13, Spark 4.1.1, Delta 4.2.0.
