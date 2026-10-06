# Plan — Fabric DE Project (Company A Sales)

> Mô phỏng 1 dự án Data Engineering thật trên Microsoft Fabric, dựa trên đề *Data Engineer Case Study*.
> Làm việc trên **Fabric web UI** + **Git (GitHub)**. Hướng **SQL-first**: biến đổi dữ liệu bằng Spark SQL; Python chỉ là "keo dán".
> Trạng thái: **Source ✅ xong · Platform: Bước 0 ✅ → Bước 1 Ingest** (xem §15) · Cập nhật: 2026-10-06
> Thao tác chi tiết từng bước (click-by-click, kết quả mong đợi, lỗi đã gặp): **[docs/guide/](guide/README.md)**.

---

## Mục lục

0. [Mục tiêu & phạm vi](#0-mục-tiêu--phạm-vi)
1. [Nguyên tắc cốt lõi](#1-nguyên-tắc-cốt-lõi)
2. [Kiến trúc tổng thể](#2-kiến-trúc-tổng-thể)
3. [Môi trường & Git](#3-môi-trường--git)
4. [Cách viết code: SQL-first](#4-cách-viết-code-sql-first)
5. [Conventions](#5-conventions)
6. [Bảng meta: config, ref, state, log](#6-bảng-meta-config-ref-state-log)
7. [Source (đã xong)](#7-source-đã-xong)
8. [Ingest & Bronze](#8-ingest--bronze)
9. [Silver](#9-silver)
10. [Gold](#10-gold)
11. [Orchestration, log, DQ, recon](#11-orchestration-log-dq-recon)
12. [Semantic model & report](#12-semantic-model--report)
13. [Performance & vận hành](#13-performance--vận-hành)
14. [Scenario drills](#14-scenario-drills)
15. [Roadmap từng bước](#15-roadmap-từng-bước)
16. [Mapping câu hỏi → phần](#16-mapping-câu-hỏi--phần)
17. [Rủi ro, spike cần kiểm chứng & ADR](#17-rủi-ro-spike-cần-kiểm-chứng--adr)

---

## 0. Mục tiêu & phạm vi

1. Trả lời Part I (Q1–Q17) — mỗi câu có **bằng chứng chạy thật**; Part II (Q18–Q22) nếu còn thời gian.
2. Platform chạy **mỗi ngày với data mới** (virtual clock), thiết kế như ở **quy mô lớn**.
3. **Đơn giản, ai biết SQL cũng đọc/sửa được.** Không dựng hạ tầng thừa (không wheel, không Docker, không framework tự chế).

**Tiêu chí thành công**

| # | Tiêu chí | Đo bằng |
|---|---|---|
| S1 | Chạy end-to-end tự động ≥ 7 ngày giả lập liên tục | `meta.log_pipeline_run` |
| S2 | Chạy lại bất kỳ ngày nào → kết quả không đổi | chạy 2 lần, so count + tổng tiền |
| S3 | Thêm 1 entity nguồn mới → chỉ thêm config (Bronze) | PR chỉ đụng `nb_setup_config` |
| S4 | Report = SQL = recon | `meta.recon_result` |
| S5 | Drill Q11, Q12, Q14, Q15, Q16 có runbook + bằng chứng | `docs/runbook.md` |

---

## 1. Nguyên tắc cốt lõi

> Luật của dự án. Mọi thiết kế/code/review đối chiếu danh sách này.

| # | Nguyên tắc | Nghĩa là |
|---|---|---|
| **P1** | **Git là nguồn sự thật** | Notebook, pipeline, DDL, config, semantic model đều qua Git; workspace dựng lại được từ repo |
| **P2** | **Phần cơ học → config; phần nghiệp vụ → SQL tường minh** | Ingest/Bronze/log/DQ/maintenance chạy generic theo bảng config. Silver/Gold là SQL viết rõ cho từng bảng (đó là logic nghiệp vụ, phải đọc được) |
| **P3** | **Không hardcode** | Không ID/path tuyệt đối (lakehouse gắn theo **tên**); luật nghiệp vụ nằm trong bảng `ref_*` (vd "doanh thu" = `ref_order_status.is_sales_recognized`, không viết `'Delivered'`); tham số vận hành trong `cfg_*` |
| **P4** | **Một logic — một chỗ** | Hàm parse/làm sạch dùng chung = **SQL function** định nghĩa 1 lần (`nb_common`); log/DQ/metrics do runner làm, notebook nghiệp vụ không lặp lại |
| **P5** | **Incremental mặc định, full reload luôn có** | Mỗi bước xử lý phần thay đổi; tham số `p_full_reload` để rebuild |
| **P6** | **Idempotent** | Chạy lại cùng tham số → cùng kết quả: `MERGE` theo khoá, hoặc `DELETE` theo batch/partition rồi `INSERT`; watermark chỉ tiến khi thành công; không dùng `current_date()` trong logic |
| **P7** | **Raw bất biến, downstream rebuild được** | `Files/landing` + `brz.*` append-only, giữ lâu (snapshot master giữ vĩnh viễn) → Silver/Gold dựng lại được |
| **P8** | **Không mất dữ liệu, không im lặng** | Dòng lỗi → `quarantine` kèm lý do; lỗi → log + alert |
| **P9** | **Reconcile mọi bước** | count & tổng tiền khớp Source → Bronze → Silver → Gold → report |
| **P10** | **Truy vết được** | `_batch_id`, `_run_id` đi xuyên các layer; mọi bước có log |
| **P11** | **Contract giữa layer** | Layer chỉ đọc schema của layer ngay trước (+ `meta`); đổi schema qua cell DDL mới trong `nb_setup_ddl` |
| **P12** | **Thiết kế cho quy mô lớn** | Không full scan khi không cần, partition pruning, không `collect()` dữ liệu lớn, file size hợp lý |
| **P13** | **Không code chết** | Notebook nào cũng nằm trong pipeline/runner hoặc thuộc nhóm `setup`/`ops`/`sim`; notebook nháp để folder `sandbox` không commit |
| **P14** | **Kiểm trước khi promote** | DQ + recon + chạy lại 2 lần xanh trên Dev mới merge `main` |
| **P15** | **Đơn giản trước** | Chọn cách nhẹ nhất đủ đúng; tối ưu khi có số đo; quyết định lớn ghi ADR |

---

## 2. Kiến trúc tổng thể

```
┌──────────────── CompanyA-Source (đã xong — giả lập hệ thống của team khác) ──────────────┐
│ sqldb_erp_wholesale (Fabric SQL DB)            lh_retail_drop (Files only)               │
│   dbo.customers/products/sales_hierarchy/orders   Files/inbound/<source>/<entity>/*.csv  │
│                 ▲                                          ▲                             │
│                 └────────── nb_00_sim_daily (virtual clock) ┘                            │
└─────────────────┬──────────────────────────────────────────┬─────────────────────────────┘
                  │ Copy activity (watermark updated_at)      │ Copy activity (LastModified)
                  ▼                                           ▼
┌──────────────── CompanyA-DataPlatform-<Dev|Prod> ───────────────────────────────────────┐
│ lh_platform  (1 lakehouse, schema-enabled)                                               │
│   Files/landing/<source>/<entity>/load_date=…/batch=…/      raw, bất biến                │
│   brz.   brz_<source>_<entity>     all-string + metadata     ← nb_brz_load (generic)     │
│   slv.   slv_<entity>, quarantine_<entity>                   ← nb_slv_* (%%sql)          │
│   gld.   gld_d_*, gld_f_*, gld_a_*                           ← nb_gld_* (%%sql)          │
│   meta.  cfg_*, ref_*, state_*, log_*, dq_*, recon_*                                     │
│                                                                                          │
│ pl_master_daily → pl_ingest → nb_run_layer(brz|slv|gld) → nb_ops_recon → refresh model   │
│ sm_sales (Direct Lake → gld.*) → rpt_sales        rpt_pipeline_health (→ meta.*)         │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

| Quy tắc | Lý do |
|---|---|
| Platform chỉ **đọc** Source; simulator không chạm Platform | ranh giới 2 team |
| Ingest bằng **Copy** (không shortcut/mirroring) | thể hiện ingest thật: watermark, landing, manifest |
| Dev & Prod đọc cùng Source, **state riêng** | chạy lại Dev không ảnh hưởng Prod |
| **1 lakehouse, mỗi layer 1 schema** (ADR 010) | gọn, 1 SQL endpoint, khớp đề (`gld.gld_f_orders`) |

---

## 3. Môi trường & Git

| Workspace | Branch | Git folder |
|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` |

**Luồng làm việc (web UI):** sửa trên ws Dev → Source control → Commit (`dev`) → PR `dev → main` trên GitHub → ws Prod: Update all → chạy `nb_setup_ddl`, `nb_setup_config` → chạy pipeline.

- Commit message: `feat(slv): …`, `fix(dq): …`, `docs: …`. Tag mỗi milestone (`v0.1`, `v1.0`).
- **Quy tắc tránh conflict:** không sửa cùng 1 notebook ở 2 nơi; sửa trên Fabric thì Commit ngay; có Update đến thì Update trước khi sửa tiếp.
- **Không hardcode môi trường:** mọi notebook Platform gắn `lh_platform` làm default lakehouse **theo tên** (cell đầu `%%configure`), Fabric tự lấy lakehouse của workspace đang chạy (Dev/Prod). Bảng gọi bằng `schema.table`, file bằng `Files/...` tương đối.

**Repo**

```
Fabric-DE-Prj/
├── fabric/source/      ← CompanyA-Source (Git sync) — xong
├── fabric/platform/    ← DataPlatform Dev/Prod (Git sync): lh_platform, nb_*, pl_*, sm_*, rpt_*
├── sql/answers/        ← q05, q06, q07, q17 … (câu trả lời SQL của đề)
├── docs/               ← PLAN.md, runbook.md, dq_findings.md, adr/
└── README.md
```

---

## 4. Cách viết code: SQL-first

### 4.1 Ai làm gì

| Việc | Công cụ | Lý do |
|---|---|---|
| Kéo data từ nguồn → `Files/landing` | **Pipeline Copy activity** | không code, có retry/song song/incremental |
| Landing → `brz.*` | **1 notebook generic** `nb_brz_load` | giống nhau mọi entity → viết 1 lần, chạy theo config |
| Bronze → Silver → Gold | **Notebook `%%sql`**, mỗi bảng 1 notebook | logic nghiệp vụ → SQL tường minh, ai cũng đọc được |
| Thứ tự chạy, log, DQ, metrics | **Runner** `nb_run_layer` (Python mỏng) | làm 1 lần cho mọi bước → notebook SQL không phải lặp lại |
| Hàm làm sạch dùng chung | **SQL function** trong `nb_common` | `parse_ts(...)`, `clean_text(...)`… gọi được từ mọi `%%sql` |
| Q9 (dim_date), Q10 (Silver orders) | **PySpark** | đề yêu cầu PySpark → dùng chính notebook đó trong pipeline (không viết 2 bản) |

### 4.2 Tránh trùng lặp (P4)

| Thứ dễ lặp | Cách gom về 1 chỗ |
|---|---|
| Parse ngày nhiều format, trim/hoa-thường, ép số an toàn | **SQL function** định nghĩa 1 lần trong `nb_common`: `parse_ts(s)`, `clean_text(s)`, `clean_code(s)`, `to_amount(s)` |
| Chuẩn hoá giá trị (gender, position, status, brand…) | **Bảng `meta.ref_value_mapping`** + JOIN — không `CASE WHEN` rải rác |
| Định nghĩa "doanh thu" | **`meta.ref_order_status`** (`is_sales_recognized`) |
| Ghi log, đếm dòng, chạy DQ sau mỗi bước | **Runner** làm quanh mỗi notebook; số dòng lấy từ Delta history của bảng đích |
| Load Bronze cho từng entity | **1 notebook generic** + `meta.cfg_source_entity` |
| Danh sách bước & thứ tự | **`meta.cfg_pipeline_step`** (runner đọc) — không liệt kê cứng trong pipeline |
| Mẫu SCD1/SCD2 | SCD1 = `MERGE` chuẩn; SCD2 chỉ có 1 bảng (`gld_d_salesman`) → không cần trừu tượng hoá |

### 4.3 Khung 1 notebook nghiệp vụ (Silver/Gold)

```
Cell 0  %%configure -f  → default lakehouse = lh_platform (theo tên); -f vì session có thể đã chạy sẵn
Cell 1  (markdown)   Mục đích · Input · Output · Grain · Cách load
Cell 2  (parameters) p_load_date, p_run_id, p_full_reload
Cell 3  %run nb_common              → đăng ký SQL function, set biến SQL
Cell 4+ %%sql                       → CREATE OR REPLACE TEMP VIEW src … (chỉ phần thay đổi)
                                     → MERGE INTO slv.xxx … / INSERT INTO quarantine_xxx …
```

Không có code log/DQ trong notebook — runner lo (P4).

### 4.4 Kiểm thử (thay cho unit test)

| Mức | Cách |
|---|---|
| Đúng dữ liệu | DQ rule sau mỗi bước (`meta.cfg_dq_rule`) |
| Không mất/không thừa | Recon count + tổng tiền giữa các layer |
| Idempotent | `nb_ops_check_idempotency`: chạy lại 1 ngày → so count + checksum từng bảng |
| Rebuild | `p_full_reload` từ Bronze == kết quả incremental |

---

## 5. Conventions

### 5.1 Naming

| Loại | Pattern | Ví dụ |
|---|---|---|
| Lakehouse | `lh_<scope>` | `lh_platform` |
| Schema | `brz` · `slv` · `gld` · `meta` | |
| Bảng | `brz_<source>_<entity>` · `slv_<entity>` · `gld_d_` / `gld_f_` / `gld_a_` · giữ prefix dù có schema (khớp đề) | `gld.gld_f_sales_line` |
| Meta | `cfg_*` config · `ref_*` luật nghiệp vụ · `state_*` runtime · `log_*` · `dq_*` · `recon_*` | `meta.state_watermark` |
| Notebook | `nb_<layer>_<entity>` · `nb_setup_*` · `nb_ops_*` · `nb_run_layer` · `nb_common` | `nb_slv_customer` |
| Pipeline | `pl_<scope>` | `pl_master_daily` |
| Cột | snake_case · `*_sk` surrogate · `*_code` natural · `*_key` date · `is_*` · `*_at` timestamp UTC · `*_date` ngày nghiệp vụ | |

### 5.2 Cột kỹ thuật

| Cột | brz | slv | gld | Ý nghĩa |
|---|---|---|---|---|
| `_source_system` | ✓ | ✓ | ✓ | `wholesale` / `retail` / `reference` |
| `_source_file` | ✓ (file) | ✓ | | file gốc |
| `_batch_id` | ✓ | ✓ | ✓ | `<source>_<entity>_<yyyymmdd>_<run_id>` |
| `_run_id` | ✓ | ✓ | ✓ | lần chạy |
| `_ingested_at` | ✓ | | | thời điểm vào Bronze |
| `_record_hash` | | ✓ | ✓ | hash cột nghiệp vụ → chỉ update khi đổi |
| `_inserted_at` / `_updated_at` | | ✓ | ✓ | |
| `_valid_from` / `_valid_to` / `_is_current` | | | SCD2 | |

### 5.3 Thời gian & khoá

- Timestamp lưu **UTC**; nguồn file không có múi giờ → `cfg_source_entity.source_timezone`.
- Ngày nghiệp vụ (`order_date`, `date_key`) theo **Asia/Ho_Chi_Minh**. `p_load_date` = ngày logic (virtual clock).
- Surrogate key **tất định**: `xxhash64(natural_key[, _valid_from])` → rebuild bao nhiêu lần cũng cùng SK. Unknown member `-1`. `date_key = YYYYMMDD`.

---

## 6. Bảng meta: config, ref, state, log

| Loại | Ai ghi | Nguồn sự thật |
|---|---|---|
| `cfg_*` config | `nb_setup_config` (`%%sql MERGE … VALUES`) | **Git** (nội dung notebook) |
| `ref_*` luật nghiệp vụ | `nb_setup_config` | **Git** |
| `state_*` | pipeline khi chạy | runtime |
| `log_*`, `dq_result_log`, `recon_result` | runner khi chạy (append-only) | runtime |

**Không sửa tay `cfg_*`/`ref_*` trên Fabric** — sửa trong `nb_setup_config` → Commit → chạy lại (MERGE idempotent, xoá dòng không còn trong script).

| Bảng | Cột chính | Dùng cho |
|---|---|---|
| `cfg_source_entity` | source_system, entity, source_type (db/file), source_object, load_type (incremental/full_snapshot), watermark_type (column/file_modified), watermark_column, lookback_days, source_timezone, business_keys, is_active, load_order | `pl_ingest`, `nb_brz_load` |
| `cfg_pipeline_step` | layer, step, notebook, depends_on, is_active, timeout | `nb_run_layer` |
| `cfg_dq_rule` | rule_id, layer, table_name, check_type, sql_expression, severity, action, is_active | runner → DQ |
| `ref_order_status` | status, sequence, is_final, is_sales_recognized | Silver/Gold |
| `ref_value_mapping` | domain, source_value, standard_value | Silver |
| `ref_holiday_vn` | holiday_date, holiday_name | `gld_d_date` |
| `state_watermark` | step, source_system, entity, watermark_value, run_id, updated_at | ingest |
| `state_file_manifest` | file_id, path, size, modified_at, checksum, batch_id, status, event_at | file ingest (append-only) |
| `log_pipeline_run` | run_id, pipeline, load_date, status, message, event_at | append-only |
| `log_task_run` | task_id, run_id, layer, step, status, rows_*, delta_version, error, started/ended_at | append-only (RUNNING → SUCCESS/FAILED) |
| `dq_result_log` | run_id, rule_id, checked_rows, failed_rows, status | |
| `recon_result` | run_id, check_name, left/right count & amount, diff, status | |
| `schema_registry` | table, column, type, status (DETECTED/APPROVED) | Q16 |

Log & manifest **append-only** (không UPDATE): nhiều bước song song ghi cùng lúc không xung đột; trạng thái hiện tại = sự kiện mới nhất.

---

## 7. Source (đã xong)

| Item | Mô tả |
|---|---|
| `sqldb_erp_wholesale` | ERP giả lập "hybrid" (ADR 008): ép kiểu timestamp/số + PK; giữ nguyên giá trị bẩn; FK khai báo nhưng không enforce. `orders` PK `order_line_id`, index `updated_at`. Schema `sim.stg_*` = staging nội bộ simulator (Platform không đọc). DDL = SQL project trong Git |
| `lh_retail_drop` | `Files/inbound/<source>/<entity>/` — CSV nguyên trạng (retail, reference) |
| `lh_sim` | seed 9 CSV, `seed_*`, `sim_state` (virtual clock), `sim_release_log`, `sim_reject_log` |
| Nguồn | `wholesale` (erp): customers, products, sales_hierarchy, orders · `retail` (file): như trên · `reference` (file): categories |

**Virtual clock (ADR 009):** initial load tới 2025-12-31 (~42k dòng/nguồn, kèm ~40 dòng ngày sai 2027); mỗi lần `nb_00_sim_daily` tiến 1 ngày (~62 dòng/nguồn) tới ~2026-05-10; exit value = ngày giả lập → `p_load_date` cho Platform. Idempotent; pipeline luôn truyền `p_sim_date` cụ thể.

**Dữ liệu cố ý bẩn (Platform phải xử lý):** format ngày lẫn lộn (file), status sai chính tả, tax `five percent`, qty âm/thập phân, orphan (`CUS099`, `PRD999`, `CAT010`, `CAT999`), `"NULL"` dạng chữ, ngày tương lai, **trùng** (`CAT005` 2 bản; cùng khách ở 2 nguồn khác thuộc tính; master gửi lại dạng snapshot).

**Còn lại (Phase 10, cho drill):** generate khi hết seed, bơm lỗi file (trùng/rỗng/trễ), `master_change` (org chart cho SCD2), drill Q14/Q16.

---

## 8. Ingest & Bronze

### 8.1 `pl_ingest(p_load_date, p_run_id)` — không code

```
Set variable  run_start = utcNow()                     ← mốc trên cố định cho cả run
Lookup        meta.cfg_source_entity (is_active) ⋈ meta.state_watermark
ForEach entity (song song 4–8)
  Switch source_type
    db   → Copy: SELECT … FROM dbo.<entity>
                 [incremental] WHERE updated_at > wm − lookback AND updated_at <= run_start
           → Files/landing/<source>/<entity>/load_date=<d>/batch=<id>/*.parquet
    file → Copy (binary): inbound/<source>/<entity>/*  lọc LastModified (wm, run_start]
           → Files/landing/<source>/<entity>/load_date=<d>/batch=<id>/
nb_brz_load                                            ← load landing → brz.* rồi ghi watermark
                                                         (CHỈ khi copy + load thành công;
                                                          db = max(updated_at) dữ liệu, file = run_start;
                                                          1 dòng / load_date → chạy lại ngày D dùng lại cửa sổ cũ)
                                                         chi tiết: guide/01-ingest/05-watermark.md
```

### 8.2 `nb_brz_load` — 1 notebook cho mọi entity

Với mỗi batch mới trong landing (đọc từ `cfg_source_entity`):
1. File: tính checksum → `state_file_manifest`; checksum đã `LOADED` → `DUPLICATE`, bỏ qua.
2. Đọc: parquet (db) / CSV `header`, **mọi cột là chuỗi** (file) → temp view.
3. `DELETE FROM brz.<t> WHERE _batch_id = …` rồi `INSERT INTO brz.<t> SELECT *, <metadata>` → idempotent.
4. Cột mới chưa có → thêm cột (schema evolution) + ghi `schema_registry` (DETECTED).

Bronze giữ **mọi thứ** (kể cả dòng bẩn); snapshot master giữ vĩnh viễn (nguồn duy nhất cho lịch sử SCD2).

---

## 9. Silver

**Mỗi entity 1 notebook `%%sql`**, cùng khuôn:

```sql
-- 1. Batch Bronze chưa xử lý (hoặc toàn bộ nếu p_full_reload)
CREATE OR REPLACE TEMP VIEW src AS SELECT … FROM brz.brz_wholesale_x WHERE _batch_id IN (…)
UNION ALL SELECT … FROM brz.brz_retail_x  WHERE _batch_id IN (…);

-- 2. Làm sạch bằng SQL function + ref mapping
CREATE OR REPLACE TEMP VIEW cleaned AS
SELECT clean_code(customer_code) AS customer_code, parse_ts(updated_at) AS updated_at, m.standard_value AS gender, …
FROM src LEFT JOIN meta.ref_value_mapping m ON m.domain = 'gender' AND m.source_value = upper(trim(src.gender));

-- 3. Dòng lỗi → quarantine ; dòng tốt → dedup (bản mới nhất theo key)
INSERT INTO slv.quarantine_x SELECT … FROM cleaned WHERE <lỗi>;
-- 4. MERGE theo business key, chỉ update khi _record_hash đổi
MERGE INTO slv.slv_x t USING (…dedup…) s ON t.key = s.key
WHEN MATCHED AND t._record_hash <> s._record_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

| Bảng | Grain | Điểm riêng |
|---|---|---|
| `slv_category` | category_code | dedup `CAT005` (survivorship: chuẩn hoá rồi lấy bản mới nhất) |
| `slv_product` | product_code | brand chuẩn hoá; wholesale ưu tiên (nguồn master) |
| `slv_customer` | customer_code | gộp 2 nguồn: survivorship theo cột (ADR 003), `source_flags` |
| `slv_sales_hierarchy` | source_system, salesman_code | chuẩn hoá position; self-reference → manager NULL |
| `slv_orders_history` | order_no, product_code, order_status, updated_at | append idempotent (MERGE insert-only) |
| `slv_orders_current` | order_no, product_code | **PySpark (Q10)**; chỉ update khi `updated_at` mới hơn (chống out-of-order) |
| `quarantine_<entity>` | — | dòng gốc + `dq_reason`, `_batch_id` |

> Q10 nói "drop duplicates on order_no" — grain đúng là **order_no + product_code**; trình bày cả hai.

---

## 10. Gold

| Bảng | Grain | Key | Load |
|---|---|---|---|
| `gld_d_date` | 1 ngày | `date_key` YYYYMMDD | **PySpark (Q9)**, generate; `ref_holiday_vn`, fiscal year từ tháng 7 |
| `gld_d_customer` | 1 khách | `customer_sk = xxhash64(customer_code)` | SCD1 `MERGE` |
| `gld_d_product` | 1 sản phẩm (+ category lvl1–4) | `product_sk` | SCD1 `MERGE` |
| `gld_d_salesman` | 1 **phiên bản** salesman | `xxhash64(source, code, _valid_from)` | **SCD2** + hierarchy flatten (Q7, Q11) |
| `gld_f_sales_line` | 1 dòng sản phẩm của đơn đã ghi nhận doanh thu | order_no, product_code | `MERGE` incremental theo đơn thay đổi |
| `gld_a_sales_month` | tháng × product × customer × salesman | — | xoá-chèn các tháng thay đổi |

- **Unknown member `-1`** ở mọi dim → fact không có FK NULL; orphan được gắn cờ.
- **SCD2 (Q11):** nguồn chỉ có trạng thái hiện tại → mỗi ngày so snapshot với bản `_is_current` theo `_record_hash`; đổi → đóng bản cũ (`_valid_to = p_load_date − 1`) + mở bản mới; flatten salesman → team lead → manager → director **tại thời điểm snapshot** → report không cần đệ quy. Rebuild được từ snapshot Bronze.
- **Fact** chỉ chứa doanh thu đã ghi nhận (`ref_order_status.is_sales_recognized`), lookup salesman **point-in-time** theo `order_date`.

---

## 11. Orchestration, log, DQ, recon

### 11.1 Pipeline

```
pl_master_daily(p_load_date)
  nb_ops_run_start            → log_pipeline_run STARTED
  pl_ingest                   → Copy + nb_run_layer('brz') + watermark
  nb_run_layer('slv')
  nb_run_layer('gld')
  nb_ops_recon
  refresh sm_sales
  nb_ops_run_end              → log + thông báo (Teams/Outlook)
  (on failure) nb_ops_run_end(FAILED) → thông báo

pl_backfill(p_from, p_to)     → lặp ngày gọi pl_master_daily (tuần tự)
pl_sim_drive(p_days)          → [Source] nb_00_sim_daily → pl_master_daily(p_load_date = exit value)
pl_maintenance                → OPTIMIZE / VACUUM theo lịch
```

### 11.2 `nb_run_layer(layer)` — runner duy nhất (Python mỏng)

1. Đọc `meta.cfg_pipeline_step` của layer → DAG (`depends_on`).
2. Chạy các notebook bằng `notebookutils.notebook.runMultiple` (1 Spark session, song song theo DAG).
3. Quanh mỗi bước: ghi `log_task_run` (RUNNING → SUCCESS/FAILED), lấy số dòng từ Delta history bảng đích, chạy DQ rule của bảng đó (`cfg_dq_rule`), ghi `dq_result_log`; rule `critical` FAIL → dừng.

→ Notebook nghiệp vụ chỉ có SQL; thêm bảng = thêm notebook + 1 dòng `cfg_pipeline_step`.

### 11.3 DQ (Q8, Q13)

| Layer | Rule tối thiểu | Khi fail |
|---|---|---|
| Bronze | file/batch đã về (freshness) · header đúng · số dòng bất thường | alert |
| Silver | key not null & unique · qty > 0, price > 0, ngày parse được, status hợp lệ | quarantine |
| Silver | FK tồn tại | flag (→ `-1` ở Gold) |
| Gold | fact không trùng grain, không FK NULL · SCD2 không chồng lấp · tổng Gold = Silver | dừng + alert |

### 11.4 Recon

Source → Bronze (rows Copy = rows Bronze) · Bronze → Silver (`in = out + quarantine + dedup`) · Silver → Gold (tổng doanh thu/tháng) · Gold → report (DAX query).

---

## 12. Semantic model & report

- `sm_sales`: **Direct Lake** trên `gld.*`; star schema; date table; ẩn SK/cột kỹ thuật.
- Measures: Sales, Qty, Net Sales, Sales LY, YoY %, MTD/YTD, Top N Customer (+ Discount sau Q16).
- `rpt_sales`: Overview · Product · Customer/Country · Sales hierarchy (point-in-time).
- `rpt_pipeline_health` (trên `meta.*`): trạng thái run, thời gian, rows/ngày, DQ, recon.

---

## 13. Performance & vận hành

| Bảng | Partition / cluster | Ghi chú |
|---|---|---|
| `brz_*_orders` | `_ingest_date` | append |
| `slv_orders_*` | `order_month` | MERGE có điều kiện partition (pruning) |
| `gld_f_sales_line` | `order_month` + Z-order `customer_sk, product_sk` | V-Order cho Direct Lake |
| dim / master | không partition | nhỏ |

- **Q12 — đọc:** partition pruning, Z-order / liquid clustering, V-Order + OPTIMIZE, aggregate table.
- **Q12 — ghi:** chỉ xử lý phần thay đổi, MERGE có pruning, Deletion Vectors, optimized write, không over-partition.
- **Vận hành:** `pl_maintenance` (OPTIMIZE/VACUUM), Monitoring hub + `rpt_pipeline_health`, rollback `RESTORE TABLE … VERSION AS OF <delta_version trong log>`.
- **Thứ tự debug (Q15):** report → refresh → Gold history → Silver → Bronze → watermark/manifest → Copy output → Source.

---

## 14. Scenario drills

| Drill | Làm | Bằng chứng |
|---|---|---|
| **Q14** late-arriving | 15/01 chèn đơn `order_date = 02/01` | watermark bắt được, partition tháng 1 cập nhật, recon khớp |
| **Q15** mất tháng 5 | xoá partition tháng 5 ở Gold | runbook debug → `RESTORE` hoặc `pl_backfill` |
| **Q16** schema evolution | thêm `discount_amount` từ 01/06 | Bronze thêm cột → registry → DDL Silver/Gold → measure mới |
| **Q11** SCD2 | đổi org chart giữa tháng 3 | report team tháng 3 đúng cơ cấu cũ |
| **Q12** scale | sinh dữ liệu lớn | số đo trước/sau tối ưu |

---

## 15. Roadmap từng bước

> Mỗi bước nhỏ, xong → kiểm → bước tiếp. Lát cắt dọc **orders** chạy được trước, mở rộng sau.

| Bước | Việc | Xong khi | Trạng thái |
|---|---|---|---|
| **0. Nền** | `lh_platform` · `nb_setup_ddl` (schema + bảng meta) · `nb_setup_config` (cfg/ref) · `nb_common` (SQL function) · **spike** (§17.2) | bảng meta có dữ liệu config; SQL function gọi được từ `%%sql` | ✅ 2026-10-06 |
| **1. Ingest** | connection SQL DB · `pl_ingest` · `nb_brz_load` · watermark/manifest · spike K4 | chạy 2 lần cùng ngày → Bronze không đổi | ▶ đang làm |
| **2. Khám phá (Q1)** | `docs/dq_findings.md` — EDA trên `brz.*` (đúng dữ liệu Platform nhận); query phát hiện dùng lại làm DQ rule | mỗi lỗi có query + cách xử lý + rule; `ref_value_mapping` đủ | |
| **3. Orders end-to-end** | `slv_orders_*` · Gold tối thiểu (`gld_d_date`, dim chỉ unknown member, `gld_f_sales_line`) · `nb_run_layer` · log · DQ cơ bản · 1 trang report | 7 ngày giả lập chạy tự động; **tag `v0.1`** | |
| **4. Master** | customer, product, category, hierarchy (Silver + Gold SCD1) | recon xanh | |
| **5. SCD2 + Gold đủ** | `gld_d_salesman` (Q7, Q11), `gld_a_sales_month` | SCD2 không chồng lấp | |
| **6. DQ + recon đủ** | rule §11.3, recon §11.4, thông báo | lỗi bơm vào bị bắt đúng | |
| **7. Vận hành** | `pl_backfill`, `pl_maintenance`, `pl_sim_drive`, kiểm idempotency/rebuild | chạy lại & rebuild ra cùng kết quả | |
| **8. Report** | `sm_sales`, `rpt_sales`, `rpt_pipeline_health` | report = SQL = recon | |
| **9. Prod** | PR `dev → main`, Prod Update, setup, backfill | **tag `v1.0`** | |
| **10. Drill** | simulator nâng cao + drill §14 + runbook | **tag `v1.1`** | |
| **11. SQL đề** | `sql/answers/` Q5, Q6, Q7, Q17 (+ Part II) | | |
| **12. Nộp bài** | PPT, diagram, README | | |

---

## 16. Mapping câu hỏi → phần

| Q | Phần | Q | Phần |
|---|---|---|---|
| Q1 | Bước 2, `dq_findings.md` | Q10 | §9 `slv_orders_current` (PySpark) |
| Q2 | §2, §8–10 | Q11 | §10 SCD2 |
| Q3, Q4 | §10, §8–9, ADR 001 | Q12 | §13 |
| Q5–Q7 | `sql/answers/` | Q13 | §6, §11 |
| Q8 | §11.3 | Q14–Q16 | §14 |
| Q9 | §10 `gld_d_date` (PySpark) | Q17 | `sql/answers/q17` |

---

## 17. Rủi ro, spike cần kiểm chứng & ADR

### 17.1 Rủi ro

| Rủi ro | Giảm thiểu |
|---|---|
| Làm dở dang | lát cắt dọc orders trước; từng bước nhỏ |
| Trial capacity giới hạn (đã gặp `TooManyRequestsForCapacity`) | 1 phiên Spark mỗi lúc; runner dùng chung session; dừng session xong việc (core trả về chậm ~1–2 phút) |
| Data thật ≠ mô tả đề | ghi nhận ở Q1, xử lý ở Silver |
| Notebook Git sync / conflict | quy tắc §3 |
| Runtime 2.0 (Spark 4.1, ANSI bật) | luôn `try_cast`, `try_to_timestamp` với dữ liệu bẩn |

### 17.2 Spike — kiểm ngay ở Bước 0 (quyết định cách làm)

| # | Cần biết | Nếu không được | Kết quả (2026-10-06) |
|---|---|---|---|
| K1 | Spark 4.1 trên Fabric có **SQL function** (`CREATE TEMPORARY FUNCTION … RETURN <biểu thức>`) | đăng ký cùng tên bằng Python UDF trong `nb_common` (SQL gọi y hệt) | ✅ chạy được |
| K2 | `%%configure` default lakehouse theo **tên** chạy được, kể cả notebook con trong `runMultiple` | gắn lakehouse trong runner, notebook con kế thừa session | ⚠️ chạy được ở **standard session**; **lỗi trong high-concurrency session** → tắt HC cho notebook; pipeline/runMultiple còn phải kiểm |
| K3 | Định dạng Git của cell `%%sql` | tạo 1 notebook mẫu trên UI → Commit → xem file | ✅ cell magic lưu dạng `# MAGIC %%sql …`, metadata `"language": "sparksql"`; `%run` để nguyên dạng thô |
| K4 | Pipeline Lookup đọc `meta.*` của lakehouse; Copy đọc Fabric SQL DB | dùng notebook nhỏ thay Lookup | ⏳ kiểm ở Bước 1 |

### 17.3 ADR

| ADR | Chủ đề |
|---|---|
| 001 | Gold model: grain, key, cách load (Q3/Q4) |
| 003 | Survivorship customer giữa 2 nguồn |
| 008 | ERP giả lập "hybrid"; `categories` là file `reference` |
| 009 | Virtual clock cho simulator |
| 010 | 1 lakehouse `lh_platform` + schema theo layer |
| 011 | **SQL-first**: Spark SQL cho biến đổi; Python chỉ cho runner/ingest generic/Q9/Q10; bỏ wheel/Docker/unit test Python (bản lưu: nhánh `archive/python-framework`) |
