# Bước 0 — Nền ✅

**Mục tiêu:** workspace Dev có lakehouse `lh_platform` chia schema theo layer, các bảng `meta.*` có config, và bộ hàm làm sạch dùng chung.

| # | Việc | File | Trạng thái |
|---|---|---|---|
| 1 | Workspace Dev + Git + `lh_platform` | [01-workspace-git-lakehouse.md](01-workspace-git-lakehouse.md) | ✅ |
| 2 | Tạo schema + bảng meta (`nb_setup_ddl`) | [02-setup-ddl.md](02-setup-ddl.md) | ✅ |
| 3 | Ghi config (`nb_setup_config`) | [03-setup-config.md](03-setup-config.md) | ✅ |
| 4 | Hàm dùng chung (`nb_common`) | [04-nb-common.md](04-nb-common.md) | ✅ |
| — | Lỗi đã gặp ở bước này | [99-loi-da-gap.md](99-loi-da-gap.md) | |

**Kết quả:**

```
lh_platform
├── Tables
│   ├── brz   (trống — Bước 1 tạo)
│   ├── slv   (trống)
│   ├── gld   (trống)
│   └── meta  13 bảng: cfg_* (3) · ref_* (3) · state_* (2) · log/dq/recon/schema_registry (5)
└── Files     (trống — Bước 1 ghi landing/)
```

**Dựng lại ở môi trường mới (vd Prod):** Update all từ Git → chạy `nb_setup_ddl` → chạy `nb_setup_config`. Hai notebook chạy lại bao nhiêu lần cũng được.
