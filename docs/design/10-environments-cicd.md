# 10 — Môi trường & CI/CD

## 1. Hai loại tách biệt

| Loại | Mô phỏng | Ở dự án này |
|---|---|---|
| **Ranh giới hệ thống** | Team nguồn ↔ team dữ liệu | `CompanyA-Source` ↔ `CompanyA-DataPlatform-*` |
| **Vòng đời triển khai** | Code đi từ phát triển → kiểm thử → chạy thật | `…-Dev` → (`…-Test`) → `…-Prod` |

Source dùng chung cho mọi môi trường Platform (cùng nguồn để so sánh); mỗi môi trường Platform có **state riêng** (watermark, Bronze…).

## 2. Workspace — Git

| Workspace | Nhánh | Thư mục | Trạng thái |
|---|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` | ✅ |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` | ✅ |
| `CompanyA-DataPlatform-Test` | `test` | `fabric/platform` | tuỳ chọn (Bước 9) |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` | Bước 9 |

## 3. Cấu hình theo môi trường

| Giá trị | Khác nhau giữa môi trường? | Cách xử lý |
|---|---|---|
| Lakehouse của Platform | có (mỗi ws 1 `lh_platform`) | Notebook gắn **theo tên** (`%%configure -f`); pipeline: Fabric thay workspace ID bằng `0000…` khi commit → tự trỏ ws hiện tại |
| Connection tới Source | thường giống | Cùng connection (đọc chung Source) |
| `environment` ghi vào log | có | **Variable Library** của Fabric (giá trị theo ws) — dùng khi lên Test/Prod |
| Lịch chạy, người nhận cảnh báo | có | Variable Library / thiết lập trên ws Prod |
| Bí mật | — | Không nằm trong Git; nằm trong Connection |

## 4. Quy trình promote

```
Dev: làm → kiểm (DQ, recon, rerun 2 lần xanh)
  → PR dev → (test) → main   [review diff notebook/pipeline/SQL]
  → ws đích: Update all → nb_setup_ddl → nb_setup_config → chạy thử 1 ngày
  → kiểm sau deploy: pipeline_run SUCCEEDED, recon PASS, report mở được
  → tag (v1.0…)
```

## 5. Quy ước Git
- Notebook: Claude viết trong repo; bạn **đóng tab → Update all**.
- Pipeline: dựng trên UI, **Ctrl+S mỗi activity**, Commit sớm; Claude review JSON.
- Commit `feat(scope): …`, `fix(scope): …`, `docs: …`.

## Quyết định
- Source chung; Platform Dev/Prod (Test tuỳ chọn); Variable Library cho giá trị theo môi trường khi lên Prod.

## Còn mở
- Có làm ws Test hay không (Bước 9) — phụ thuộc thời gian.
