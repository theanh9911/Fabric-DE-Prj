# 0.1 — Workspace, Git, lakehouse ✅

## Mục tiêu
Workspace `CompanyA-DataPlatform-Dev` nối Git (nhánh `dev`, thư mục `fabric/platform`), có `lh_platform` bật schema, chạy Runtime 2.0.

## Hiểu trước khi làm
- **1 workspace ↔ 1 nhánh + 1 thư mục.** Mọi item trong ws được lưu thành thư mục `<tên>.<Loại>/` trong repo.

  | Workspace | Nhánh | Thư mục |
  |---|---|---|
  | `CompanyA-Source` | `dev` | `fabric/source` |
  | `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` |
  | `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` — **chưa nối**, nối ở Bước 9 sau khi merge PR |

- **Commit** = đẩy thay đổi từ ws lên Git. **Update** = kéo thay đổi từ Git về ws.

## Làm
1. Workspace settings → **Git integration** → Connect: repo `Fabric-DE-Prj`, Branch **`dev`**, Git folder **`fabric/platform`** → Connect and sync.
2. Workspace settings → **Data Engineering/Science → Spark settings**:
   - Tab **Environment** → Runtime version **2.0 (Spark 4.1, Delta 4.2)** → Save.
   - Tab **High concurrency** → **tắt** "For notebooks" → Save (để `%%configure` chạy được).
3. New item → **Lakehouse** → tên `lh_platform` → **tick "Lakehouse schemas"** → Create.
4. Source control → **Commit**.

## Kết quả mong đợi
- Repo có `fabric/platform/lh_platform.Lakehouse/`, file `lakehouse.metadata.json` = `{"defaultSchema":"dbo"}` (= đã bật schema).

## Lỗi đã gặp
- Ws Prod từng nối nhầm `main` + thư mục gốc → commit `lh_platform` lạc vào `main`. Đã revert (`41e9da7`) và ngắt Git ở Prod. **Prod chưa nối Git cho tới Bước 9.**
