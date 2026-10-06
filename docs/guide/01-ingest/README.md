# Bước 1 — Ingest happy path ▶

**Mục tiêu:** `pl_ingest(p_load_date)` lấy phần mới của 9 nguồn → landing → Bronze; ghi audit dữ liệu (`ingestion_batch`); tiến watermark khi thành công (không lùi). Rerun → Bronze không đổi. Trạng thái chạy, thời lượng, lỗi: Monitoring hub.

Thiết kế: [design/03 ingest](../../design/03-ingest.md) · [04 control tables](../../design/04-control-tables.md) · [05 bronze](../../design/05-bronze.md).

## Hình dạng cuối của `pl_ingest`

```
set_run_start ─✓─▶ lkp_source_entity ─✓─▶ fe_source_entity ─✓─▶ nb_brz_load
                                           └ sw_source_type
                                               ├ db      → cp_db_to_landing
                                               ├ file    → cp_file_to_landing
                                               └ default → fail_unknown_source_type
```

| Activity | Làm gì |
|---|---|
| `set_run_start` | `v_run_start` = giờ thật lúc bắt đầu |
| `lkp_source_entity` | Mỗi entity: chiến lược, đối tượng nguồn, **cửa sổ** `window_start`/`window_end`, watermark cũ |
| `fe_source_entity` | Lặp các entity (song song 4); `p_mode = rerun` → lặp rỗng (không Copy) |
| `sw_source_type` | `db_*` → nhánh db · `file_*` → nhánh file |
| `cp_db_to_landing` | ERP → parquet; full snapshot hoặc theo cửa sổ |
| `cp_file_to_landing` | Chép nguyên file có LastModified trong cửa sổ |
| `nb_brz_load` | landing → Bronze; ghi `ingestion_batch`, `watermark_state` (không lùi); lỗi → pipeline FAILED |

## Việc (thứ tự làm)

| # PLAN | File | Ai | Trạng thái |
|---|---|---|---|
| 1.1 | Khung pipeline + nhánh db bản đầu | B | ✅ (sẽ sửa ở 1.5) |
| 1.2–1.4 | [01-meta-setup.md](01-meta-setup.md) — bảng meta v4, config v4, dọn Dev | C → B | ⏳ tiếp theo |
| 1.5 | [02-khung-pipeline.md](02-khung-pipeline.md) — tham số, Lookup, ForEach, Switch | B | ⏳ |
| 1.5 | [03-nhanh-db.md](03-nhanh-db.md) — Copy ERP | B | ⏳ (sửa câu query) |
| 1.5 | [04-nhanh-file.md](04-nhanh-file.md) — Copy file | B | ⏳ |
| 1.6–1.7 | [05-bronze-load.md](05-bronze-load.md) — `nb_brz_load`, nối vào pipeline | C → B | ⏳ |
| 1.8 | [06-kiem-thu.md](06-kiem-thu.md) — normal, rerun, ngày kế tiếp | B + C | ⏳ |
| — | [99-loi-da-gap.md](99-loi-da-gap.md) | | |

**Trong suốt Bước 1: không chạy `nb_00_sim_daily`** (Source đứng yên ở 2026-01-01) cho tới ca kiểm thử T3.
