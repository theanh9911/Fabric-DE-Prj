# Bước 0 — Nền ✅

**Mục tiêu:** workspace Dev có lakehouse `lh_platform` chia schema theo layer, notebook setup, và hàm làm sạch dùng chung.

| # | Việc | File | Trạng thái |
|---|---|---|---|
| 1 | Workspace Dev + Git + `lh_platform` | [01-workspace-git-lakehouse.md](01-workspace-git-lakehouse.md) | ✅ |
| 2 | `nb_setup_ddl` — schema + bảng meta | [02-setup-ddl.md](02-setup-ddl.md) | ✅ (bảng meta thay ở Bước 1.2) |
| 3 | `nb_setup_config` — config | [03-setup-config.md](03-setup-config.md) | ✅ (nội dung thay ở Bước 1.3) |
| 4 | `nb_common` — hàm dùng chung | [04-nb-common.md](04-nb-common.md) | ✅ (thêm `std_*` ở Bước 2) |
| — | Lỗi đã gặp | [99-loi-da-gap.md](99-loi-da-gap.md) | |

## Bước 0 để lại gì — và Bước 1 đổi gì

| | Sau Bước 0 (v2) | Sau Bước 1.2–1.4 (v4) |
|---|---|---|
| Schema | `brz`, `slv`, `gld`, `meta` | giữ |
| Bảng `meta` | 13 bảng (cfg 3 · ref 3 · state 2 · log/dq/recon/registry 5), 10 bảng rỗng | **3 bảng:** `cfg_source_entity`, `watermark_state`, `ingestion_batch` (log vận hành xem ở Monitoring hub) |
| Luật nghiệp vụ | bảng `ref_order_status`, `ref_value_mapping` | SQL function trong `nb_common` |
| Lý do đổi | — | [PLAN §2.2](../../PLAN.md#22-điều-chỉnh-v3--v4-sau-phản-biện), [design/04](../../design/04-control-tables.md) |

**Dựng lại ở môi trường mới (vd Prod):** Update all từ Git → chạy `nb_setup_ddl` → chạy `nb_setup_config`. Hai notebook chạy lại bao nhiêu lần cũng được.
