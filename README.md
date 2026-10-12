# Fabric-DE-Prj — Company A Sales Data Platform

Mô phỏng dự án Data Engineering trên Microsoft Fabric theo hướng production. Kiến trúc hiện hành ở [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): Landing là raw bất biến duy nhất, không có bảng Bronze; dữ liệu đi qua Silver, Gold, DQ/đối soát và semantic/report.

**Trạng thái:** Source simulator đã có; platform code đang được dựng theo architecture mới. Xem [docs/platform-implementation.md](docs/platform-implementation.md) để biết item, thứ tự deploy và các phần còn cần bind trong Fabric.

## Kiến trúc

```text
CompanyA-Source (chỉ đọc)                    CompanyA-DataPlatform-<Dev|Prod>
  ERP SQL Database ─┐                         lh_platform
  Drop zone ─────────┼─ ingest ─────────────▶   Files/landing (raw bất biến + manifest)
  Simulator ─────────┘                           slv → gld → meta audit/checks
                                                Direct Lake semantic model + reports
```

## Tài liệu

| Đọc | Để |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Nguồn sự thật về kiến trúc và quy tắc triển khai |
| [docs/platform-implementation.md](docs/platform-implementation.md) | Item hiện có, cách deploy, giới hạn cần xác minh trên Fabric |
| [docs/PLAN.md](docs/PLAN.md) | Kế hoạch bản trước; đang được đồng bộ dần |
| [docs/design/](docs/design/README.md) | Thiết kế chi tiết cũ; đối chiếu với architecture mới |
| [docs/guide/](docs/guide/README.md) | Hướng dẫn thao tác đang chuyển từ Bronze sang Landing → Silver |

## Cấu trúc repo

| Thư mục | Nội dung |
|---|---|
| `fabric/source/` | Workspace nguồn mô phỏng: SQL project, drop zone, lakehouse và simulator notebooks |
| `fabric/platform/` | Lakehouse, notebooks và pipelines của data platform |
| `docs/` | Architecture, kế hoạch, source contract và hướng dẫn vận hành |

## Workspace

| Workspace | Nhánh | Thư mục |
|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` |
