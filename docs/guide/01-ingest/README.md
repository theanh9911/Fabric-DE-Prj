# Bước 1 — Ingest: Source → landing → Bronze ▶

**Mục tiêu:** 1 pipeline `pl_ingest(p_load_date)` kéo **phần mới** của cả 9 nguồn về `lh_platform`, nạp vào bảng Bronze, rồi ghi lại mốc đã lấy. Chạy lại cùng ngày → Bronze không đổi.

## Bức tranh

```
                    CompanyA-Source (chỉ đọc)                         CompanyA-DataPlatform-Dev / lh_platform
                    ─────────────────────────                         ──────────────────────────────────────
 sqldb_erp_wholesale.dbo.{customers,products,sales_hierarchy,orders} ─┐
                                                                     ├─ Copy ─▶ Files/landing/<source>/<entity>/load_date=<d>/batch=<RunId>/
 lh_retail_drop/Files/inbound/{retail/*, reference/categories}  ─────┘                    │
                                                                                          ▼ nb_brz_load
                                                                                   Tables/brz.brz_<source>_<entity>
                                                                                          │
                                                                                          ▼
                                                                                   meta.state_watermark (mốc mới)
```

## Pipeline `pl_ingest` — hình dạng cuối

```
set_run_start ──✓──▶ lkp_source_entity ──✓──▶ fe_source_entity (ForEach, song song 4) ──✓──▶ nb_brz_load
                                                └─ sw_source_type (Switch theo source_type)
                                                     ├─ db      → cp_db_to_landing
                                                     ├─ file    → cp_file_to_landing
                                                     └─ default → fail_unknown_source_type
```

**Vì sao pipeline + Copy (không phải notebook đọc thẳng nguồn)?** Dự án thật: nguồn thường sau firewall → chỉ đi được qua *data gateway* của pipeline; mật khẩu nằm trong *Connection*; có sẵn retry/song song; không tốn Spark. Khung này đem sang dự án thật chỉ cần đổi Connection + dòng config.

## Các việc

| # | Việc | File | Trạng thái |
|---|---|---|---|
| 1 | Khung pipeline: tham số, biến, Lookup, ForEach, Switch | [01-khung-pipeline.md](01-khung-pipeline.md) | ✅ |
| 2 | Nhánh `db`: ERP → landing (parquet) | [02-nhanh-db.md](02-nhanh-db.md) | ✅ chạy được |
| 3 | Nhánh `file`: drop zone → landing (giữ nguyên file) | [03-nhanh-file.md](03-nhanh-file.md) | ▶ làm tiếp |
| 4 | `nb_brz_load`: landing → Bronze | [04-brz-load.md](04-brz-load.md) | thiết kế |
| 5 | Ghi watermark + nối notebook vào pipeline | [05-watermark.md](05-watermark.md) | thiết kế |
| 6 | Kiểm thử: chạy 2 lần, chạy ngày kế tiếp | [06-kiem-thu.md](06-kiem-thu.md) | |
| — | Lỗi đã gặp | [99-loi-da-gap.md](99-loi-da-gap.md) | |

**Quy tắc khi làm bước này:** Source đang ở ngày giả lập **2026-01-01** → **không chạy `nb_00_sim_daily`** cho tới hết việc 6 (Source đứng yên mới kiểm được "chạy lại không đổi").
