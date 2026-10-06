# Plan — Fabric DE Project (Company A Sales)

> Mô phỏng 1 dự án Data Engineering thật trên Microsoft Fabric, dựa trên đề *Data Engineer Case Study*.
> Làm việc trên **Fabric UI** + **Git (GitHub)**. Local dùng cho package Python, test, review.
> Trạng thái: **PLANNING** · Cập nhật: 2026-10-06

---

## Mục lục

0. [Mục tiêu & phạm vi](#0-mục-tiêu--phạm-vi)
1. [Nguyên tắc cốt lõi](#1-nguyên-tắc-cốt-lõi)
2. [Kiến trúc tổng thể](#2-kiến-trúc-tổng-thể)
3. [Môi trường, Git & CI/CD](#3-môi-trường-git--cicd)
4. [Engineering standards](#4-engineering-standards)
5. [Conventions](#5-conventions)
6. [Config, reference data & state](#6-config-reference-data--state)
7. [Source simulator](#7-source-simulator)
8. [Ingest & Bronze](#8-ingest--bronze)
9. [Silver](#9-silver)
10. [Gold](#10-gold)
11. [Log & observability](#11-log--observability)
12. [Data quality & reconciliation](#12-data-quality--reconciliation)
13. [Orchestration](#13-orchestration)
14. [Semantic model & report](#14-semantic-model--report)
15. [Performance & scale playbook](#15-performance--scale-playbook)
16. [Vận hành, bảo trì & phục hồi](#16-vận-hành-bảo-trì--phục-hồi)
17. [Scenario drills](#17-scenario-drills)
18. [Roadmap](#18-roadmap)
19. [Mapping câu hỏi → phần](#19-mapping-câu-hỏi--phần)
20. [Rủi ro & ADR](#20-rủi-ro--adr)

---

## 0. Mục tiêu & phạm vi

**Mục tiêu**
1. Trả lời đầy đủ Part I (Q1–Q17), Part II (Q18–Q22) nếu còn thời gian — mỗi câu có **bằng chứng chạy thật**.
2. Xây 1 data platform chạy **mỗi ngày với data mới**, thiết kế như thể ở **quy mô lớn** (orders hàng tỷ dòng).
3. Thể hiện chuẩn làm việc của một team DE: Git, config-driven, test, DQ, observability, runbook.

**Ngoài phạm vi:** streaming real-time, ML, Purview/governance đầy đủ, multi-tenant.

**Tiêu chí thành công**

| # | Tiêu chí | Đo bằng |
|---|---|---|
| S1 | Pipeline chạy tự động hằng ngày, end-to-end, ≥ 7 ngày liên tục | `log_pipeline_run` |
| S2 | Rerun bất kỳ ngày nào → kết quả không đổi | idempotency test |
| S3 | Thêm 1 entity mới **không cần viết code** | thêm config + PR |
| S4 | Số trên report = số SQL = số reconcile | `recon_result` |
| S5 | Mọi drill (Q11, Q12, Q14, Q15, Q16) có runbook + bằng chứng | `docs/runbook.md` |
| S6 | CI xanh: lint, unit test, không hardcode, không notebook mồ côi | GitHub Actions |

---

## 1. Nguyên tắc cốt lõi

> Đây là luật của dự án. Mọi thiết kế, code, review đều đối chiếu với danh sách này. Vi phạm → không merge.

| # | Nguyên tắc | Nghĩa là | Kiểm chứng bằng |
|---|---|---|---|
| **P1** | **Git là nguồn sự thật** | Code, config, DDL, semantic model đều nằm trong repo, thay đổi qua PR. Workspace có thể dựng lại hoàn toàn từ repo + data nguồn. | Dựng lại Prod từ `main` |
| **P2** | **Config-driven, code generic** | Thêm entity/cột/rule = thêm config, không thêm code. Code riêng chỉ cho **logic nghiệp vụ đặc thù** (survivorship, SCD2 hierarchy, fact). | S3 |
| **P3** | **Không hardcode** | Môi trường → Variable Library + `paths`. Luật nghiệp vụ → bảng `ref_*`. Tham số vận hành → config. Không GUID, không path tuyệt đối, không magic string nghiệp vụ trong code. | CI `check_hardcode` |
| **P4** | **Một logic — một chỗ (DRY)** | Mọi hàm dùng chung nằm trong package `companya_de`. Không copy-paste giữa notebook. SQL/DAX không lặp lại logic ETL. | Review + CI |
| **P5** | **Incremental mặc định, full reload luôn có** | Mỗi bước xử lý phần thay đổi (watermark / batch / CDF). Mỗi bảng có chế độ `p_full_reload` để rebuild khi cần. | test 2 chế độ ra cùng kết quả |
| **P6** | **Idempotent & tất định** | Rerun cùng tham số → cùng kết quả. Không dùng `now()` trong logic nghiệp vụ. Surrogate key tất định (hash). Watermark chỉ tiến khi bước thành công. | idempotency test (S2) |
| **P7** | **Raw bất biến, downstream rebuild được** | Landing & Bronze là append-only, giữ lâu dài (snapshot master giữ **vĩnh viễn** vì nguồn không có lịch sử). Silver/Gold luôn tái tạo được từ Bronze. | drill rebuild |
| **P8** | **Không mất dữ liệu, không im lặng** | Dòng lỗi → quarantine + lý do. Lỗi → log + alert. Không `dropna()`/`filter` âm thầm. | `in = out + rejected + deduped` |
| **P9** | **Reconcile mọi bước** | Count & sum khớp Source → Bronze → Silver → Gold → Semantic model. | `recon_result` (S4) |
| **P10** | **Observable & truy vết được** | Mọi task ghi log; `_run_id`, `_batch_id` đi xuyên các layer; biết mỗi dòng đến từ file/batch nào. | `log_task_run` |
| **P11** | **Contract giữa các layer** | Schema chỉ thay đổi qua migration + `schema_registry`. Không layer nào đọc "lách" layer khác (Gold không đọc Bronze). | migration log |
| **P12** | **Thiết kế cho quy mô lớn** | Không full scan khi không cần; partition pruning; không `collect()`/`toPandas()` dữ liệu lớn; không loop Python trên dòng; file size hợp lý. | review + drill Q12 |
| **P13** | **Mọi thứ phải có lý do tồn tại** | Không dead code. Notebook phải thuộc 1 pipeline/DAG hoặc nhóm `setup`/`ops`/`sim`. Notebook nháp không commit. | CI `check_orphans` |
| **P14** | **Test trước khi promote** | Unit test cho hàm, data test cho bảng, idempotency + recon trước khi merge `main`. | CI + PR checklist |
| **P15** | **Đơn giản trước, tối ưu có đo đạc** | Làm đúng trước, tối ưu sau; mọi tối ưu phải có số liệu trước/sau. Quyết định lớn ghi ADR. | `docs/adr/`, runbook |

---

## 2. Kiến trúc tổng thể

```
┌──────────────────── CompanyA-Source (giả lập hệ thống của team khác) ───────────────────┐
│  sqldb_erp_wholesale  (Fabric SQL Database)      lh_retail_drop  (Lakehouse, Files only) │
│   dbo.orders / customers / products /             Files/inbound/<source>/<entity>/       │
│   sales_hierarchy — PK/FK, UTC, index updated_at      <entity>_YYYYMMDD[_vN].csv         │
│                         ▲                                    ▲                           │
│                         └──────── nb_00_sim_* (05:00) ───────┘                           │
└─────────────────────────┬────────────────────────────────────┬───────────────────────────┘
                          │ Copy activity (watermark)          │ Copy activity (LastModified)
                          ▼                                    ▼
┌──────────────── CompanyA-DataPlatform-<Dev|Prod> ───────────────────────────────────────┐
│ lh_platform  (1 lakehouse, schema-enabled — mỗi layer 1 schema)                          │
│  Files/landing/<src>/<entity>/load_date=…/batch=…/   (immutable raw)                     │
│  brz.  brz_<src>_<entity>  (append, all-string, + metadata)                              │
│      │  map · cast · standardize · dedup · conform · quarantine                          │
│      ▼                                                                                   │
│  slv.  slv_* · quarantine_*                                                              │
│      │  model · SCD · surrogate key · point-in-time                                      │
│      ▼                                                                                   │
│  gld.  gld_d_* · gld_f_* · gld_a_*                                                       │
│      ▼                                                                                   │
│ sm_sales (Direct Lake → gld.*) → rpt_sales      sm_ops (→ meta.*) → rpt_pipeline_health  │
│                                                                                          │
│  meta. cfg_* (config) · ref_* (luật nghiệp vụ) · state_* · log_* · dq_* · recon_*        │
│ pl_master_daily · pl_ingest · pl_backfill · pl_maintenance                               │
│ nb_run_layer · nb_* đặc thù · env_common (wheel companya_de) · vl_config                 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ranh giới**

| Quy tắc | Lý do |
|---|---|
| Platform chỉ **đọc** Source; simulator không chạm Platform | mô phỏng ranh giới 2 team |
| Ingest bằng **copy** (không shortcut/mirroring) | thể hiện ingest thật: watermark, landing, manifest |
| Dev & Prod đọc cùng Source, **state riêng** (watermark, manifest) | chạy lại Dev không ảnh hưởng Prod |
| **1 lakehouse `lh_platform`, mỗi layer 1 schema** (`brz`, `slv`, `gld`, `meta`) | ít item, 1 SQL endpoint, join `schema.table` tự nhiên, khớp quy ước đề (`gld.gld_f_orders` ở Q17). Phân quyền theo schema bằng OneLake data access roles; analyst dùng semantic model. Tách thành nhiều lakehouse sau vẫn dễ vì path dựng từ config (ADR 010) |
| Layer chỉ đọc schema của layer ngay trước nó (+ `meta`) | contract rõ (P11) — không còn ranh giới vật lý nên kiểm bằng review + CI (`check_layer_reads`) |

---

## 3. Môi trường, Git & CI/CD

### 3.1 Workspace ↔ Git

| Workspace | Branch | Git folder | Ghi chú |
|---|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` | 1 bản duy nhất |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` | làm việc hằng ngày |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` | chỉ nhận qua PR |

Source nối `dev` vì Fabric commit thẳng lên branch; `main` có branch protection.

### 3.2 Cấu trúc repo

```
Fabric-DE-Prj/
├── .github/workflows/ci.yml        lint · test · checks · build wheel
├── src/companya_de/                package dùng chung (P4)
│   ├── env.py                      paths, Variable Library, runtime context
│   ├── config.py                   đọc cfg_* / ref_* ; validate
│   ├── io.py                       read_incremental, write_batch, merge_delta
│   ├── scd.py                      scd1_merge, scd2_merge
│   ├── transforms.py               parse_multi_date, apply_mapping, standardize, dedup_latest
│   ├── keys.py                     surrogate key tất định
│   ├── dq.py                       rule engine, quarantine
│   ├── recon.py
│   ├── logging.py                  run/task log, manifest
│   ├── runner.py                   build DAG từ cfg_pipeline_step → runMultiple
│   └── config/                     ⇦ CONFIG AS CODE (YAML) — đóng gói theo wheel
│       ├── source_entity.yml  column_mapping.yml  table.yml
│       ├── pipeline_step.yml  dq_rule.yml
│       └── ref_order_status.yml  ref_value_mapping.yml  ref_holiday_vn.yml
├── tests/
│   ├── unit/                       pytest + Spark/Delta trong Docker
│   └── golden/                     số liệu kỳ vọng cho simulator seed cố định
├── migrations/                     V001__meta.sql, V002__silver.sql, …  (P11)
├── tools/                          check_hardcode.py · check_orphans.py · check_outputs.py
├── fabric/
│   ├── source/                     sqldb_erp_wholesale.SQLDatabase · lh_retail_drop · nb_00_sim_*
│   └── platform/                   lh_platform · nb_* · pl_* · env_common · vl_config · sm_* · rpt_*
├── sql/answers/                    q05 … q07, q17, q18 …
├── docs/
│   ├── PLAN.md  architecture.md  conventions.md  runbook.md  dq_findings.md
│   ├── adr/                        001-…md
│   └── diagrams/
└── README.md
```

### 3.3 Luồng làm việc

```
feat/<x> từ dev
  ├─ code package (local) → pytest → push → CI build wheel (artifact, version = git tag/sha)
  ├─ upload wheel vào env_common (Dev) → publish
  └─ sửa notebook/pipeline trên ws Dev → Commit
PR feat/<x> → dev     (CI xanh + checklist)
PR dev → main          (idempotency + recon xanh trên Dev)
ws Prod: Update all → env_common dùng wheel cùng version → nb_setup (migrate + config) → chạy
tag vX.Y
```

### 3.4 Tách cấu hình theo môi trường

- Path: `env.paths()` lấy workspace hiện tại từ `notebookutils.runtime.context` → abfss tới lakehouse theo **tên**. Không dựa vào default lakehouse.
- Giá trị khác nhau theo môi trường (connection Source, email alert, scale factor, lịch) → **Variable Library `vl_config`** (value set `dev` / `prod`).
- Tên item giống hệt nhau ở Dev và Prod.
- Hướng mở rộng (ADR): `fabric-cicd` deploy từ repo + `parameter.yml` thay ID theo môi trường, thay cho cơ chế workspace sync branch.

### 3.5 CI (GitHub Actions)

| Job | Làm gì | Fail khi |
|---|---|---|
| `lint` | ruff trên `src/`, `tests/`, `fabric/**/notebook-content.py` | lỗi lint |
| `unit` | pytest trong cùng image Docker với local (Java 21, Python 3.13, Spark 4.1.1, Delta 4.2.0) | test fail |
| `config` | validate YAML theo schema (pydantic): key tồn tại, mapping đủ cột key, rule tham chiếu bảng có thật | config sai |
| `check_hardcode` | quét code cell: GUID, `abfss://`, `onelake.dfs`, literal nghiệp vụ trong danh sách cấm (`'Delivered'`, …) | tìm thấy |
| `check_orphans` | notebook không thuộc `cfg_pipeline_step`/pipeline và không thuộc nhóm `setup`/`ops`/`sim` | có mồ côi |
| `check_outputs` | notebook commit kèm output lớn | có output |
| `build` | build wheel `companya_de-<ver>.whl` | — |

### 3.6 PR checklist

- [ ] Tuân thủ P1–P15 (đặc biệt P2, P3, P4, P13)
- [ ] Logic mới nằm trong package, có unit test
- [ ] Thay đổi schema có migration; config có PR riêng hoặc cùng PR, đã validate
- [ ] Đã chạy trên Dev: 2 lần cùng `p_load_date` → không đổi; recon xanh
- [ ] Docs/runbook/ADR cập nhật nếu thay đổi hành vi

---

## 4. Engineering standards

### 4.1 Phân vai code

| Lớp | Chứa | Không chứa |
|---|---|---|
| **Package `companya_de`** | mọi logic tái sử dụng: IO, merge, SCD, transform, DQ, log, runner | tên bảng/cột cụ thể của 1 entity |
| **Config YAML** | entity, mapping cột, thuộc tính bảng, DAG, rule, luật nghiệp vụ | logic |
| **Notebook generic** (`nb_run_layer`, `nb_slv_generic`, `nb_brz_load`) | gọi package theo config | if/else theo tên entity |
| **Notebook đặc thù** (`nb_slv_customer`, `nb_gld_d_salesman`, `nb_gld_f_sales_line`, …) | logic nghiệp vụ riêng, gọi hàm package | hàm tiện ích tự viết lại |
| **Pipeline** | thứ tự, song song, retry, copy data, alert | transform |

### 4.2 Notebook template (bắt buộc)

```python
# CELL 1 — Header (markdown)
#   Purpose · Inputs · Outputs · Grain · Load method · Owner · Related ADR

# CELL 2 — Parameters (parameter cell)
p_load_date = None        # 'YYYY-MM-DD' — logical date
p_run_id = None
p_entity = None
p_full_reload = False

# CELL 3 — Setup
from companya_de import env, config, io, dq, logging as dlog
ctx = env.context(p_load_date, p_run_id)          # paths, vl_config, timezone, run_id
task = dlog.task_start(ctx, layer=..., entity=p_entity)

# CELL 4 — Body
try:
    df = io.read_incremental(ctx, source=..., full=p_full_reload)
    df = ...                                       # chỉ gọi hàm package
    df_ok, df_bad = dq.validate(ctx, df, ruleset=...)
    metrics = io.merge_delta(ctx, df_ok, target=...)
    dq.quarantine(ctx, df_bad, ...)
    dlog.task_end(task, "SUCCESS", metrics)
except Exception as e:
    dlog.task_end(task, "FAILED", error=e)
    raise
```

### 4.3 Coding rules

- Spark DataFrame API / Spark SQL; **không** pandas cho dữ liệu lớn; **không** UDF Python khi có hàm built-in.
- Hàm thuần (input DataFrame → output DataFrame), không side effect ngoài `io`/`logging`.
- Không `collect()`, `count()` thừa, `cache()` vô tội vạ; mọi action có lý do.
- Type hints + docstring ngắn cho hàm public; ruff mặc định.
- Tên hàm/biến tiếng Anh; comment/docs tiếng Việt hoặc Anh đều được nhưng nhất quán theo file.
- Notebook nháp: folder `sandbox/` trong workspace, **không commit** (P13).

### 4.4 Chiến lược test

| Tầng | Công cụ | Nội dung |
|---|---|---|
| Unit | pytest + Spark/Delta trong Docker (`docker compose run --rm test`) | transforms, keys, scd1/scd2, merge idempotent, mapping, dq rule → SQL |
| Config | pydantic schema | YAML hợp lệ, tham chiếu chéo đúng |
| Data | DQ engine | mỗi run, mỗi layer |
| Idempotency | `nb_ops_test_idempotency` | chạy 1 ngày 2 lần → so `count` + checksum từng bảng |
| Full vs incremental | `nb_ops_test_rebuild` | rebuild full từ Bronze == kết quả incremental |
| Golden | simulator seed cố định N ngày | tổng orders / sales / số SCD2 version khớp `tests/golden/` |

---

## 5. Conventions

### 5.1 Naming

| Loại | Pattern | Ví dụ |
|---|---|---|
| Lakehouse | `lh_<scope>` | `lh_platform` (Platform), `lh_sim`, `lh_retail_drop` (Source) |
| Schema (layer) | `brz` · `slv` · `gld` · `meta` | `gld.gld_f_sales_line` |
| Notebook generic | `nb_<verb>_<scope>` | `nb_run_layer`, `nb_brz_load` |
| Notebook đặc thù | `nb_<layer>_<entity>` | `nb_gld_d_salesman` |
| Notebook nhóm khác | `nb_setup_*` · `nb_ops_*` · `nb_00_sim_*` | `nb_setup_migrate` |
| Pipeline | `pl_<scope>` | `pl_master_daily` |
| Bảng | `brz_<src>_<entity>` · `slv_<entity>` · `gld_d_` / `gld_f_` / `gld_a_` — giữ prefix layer dù đã có schema (khớp đề, không nhầm khi bảng hiện không kèm schema trong semantic model / Power BI) | `gld.gld_f_sales_line` |
| Meta | `cfg_*` (config) · `ref_*` (luật nghiệp vụ) · `state_*` (runtime) · `log_*` · `dq_*` · `recon_*` | `state_watermark` |
| Cột | snake_case · `*_sk` surrogate · `*_code` natural · `*_key` date · `is_*` · `*_at` timestamp UTC · `*_date` date nghiệp vụ | |

### 5.2 Metadata columns

| Cột | Bronze | Silver | Gold | Ý nghĩa |
|---|---|---|---|---|
| `_source_system` | ✓ | ✓ | ✓ | `wholesale` / `retail` |
| `_source_file` | ✓ | ✓ | | file gốc (nguồn file) |
| `_batch_id` | ✓ | ✓ | ✓ | `<src>_<entity>_<load_date>_<run_id>` |
| `_run_id` | ✓ | ✓ | ✓ | lần chạy pipeline |
| `_ingested_at` | ✓ | | | thời điểm vào Bronze |
| `_record_hash` | | ✓ | ✓ | hash cột nghiệp vụ → phát hiện thay đổi |
| `_inserted_at` / `_updated_at` | | ✓ | ✓ | ghi / cập nhật dòng |
| `_valid_from` / `_valid_to` / `_is_current` | | | SCD2 | lịch sử |

### 5.3 Thời gian

- Timestamp lưu **UTC** (`*_at`). Nguồn retail không có timezone → `source_timezone` trong `cfg_source_entity` để quy đổi.
- Ngày nghiệp vụ (`order_date`, `date_key`, tháng báo cáo) theo **`business_timezone = Asia/Ho_Chi_Minh`** (config).
- `p_load_date` là **logical date**, không phải ngày máy chạy.

### 5.4 Keys

- Surrogate key **tất định**: `xxhash64(<natural_key>[, _valid_from])` → rebuild bao nhiêu lần cũng cùng SK, không phụ thuộc thứ tự (P6). DQ rule `unique` trên SK để phát hiện va chạm.
- Unknown member `sk = -1` cho mọi dim.
- `date_key = YYYYMMDD` (int).

---

## 6. Config, reference data & state

### 6.1 Ba loại bảng meta — tách rõ

| Loại | Ai ghi | Nguồn sự thật | Ví dụ |
|---|---|---|---|
| **Config** `cfg_*` | `nb_setup_config` (từ YAML) | **Git** | entity, mapping, table, DAG, DQ rule |
| **Reference** `ref_*` | `nb_setup_config` (từ YAML) | **Git** | trạng thái đơn, mapping giá trị, ngày lễ |
| **State** `state_*` | pipeline lúc chạy | runtime | watermark, CDF version, manifest |
| **Log** `log_*`, `dq_result_log`, `recon_result` | pipeline lúc chạy | runtime | — |

Config/ref **không bao giờ sửa tay** trên Fabric — sửa YAML → PR → `nb_setup_config` MERGE vào bảng (idempotent). Runtime không bao giờ ghi config.

### 6.2 Config tables

**`cfg_source_entity`** — mỗi entity × nguồn

| Cột | Ví dụ |
|---|---|
| `source_system`, `entity`, `source_type` | `retail`, `orders`, `file` |
| `source_object` | `dbo.orders` / `inbound/orders/` |
| `load_type` | `incremental` / `full_snapshot` |
| `watermark_column`, `lookback_days` | `updated_at`, `2` |
| `source_timezone`, `date_formats` | `Asia/Ho_Chi_Minh`, `["M/d/yyyy H:mm", …]` |
| `bronze_table` | `brz_retail_orders` |
| `is_active`, `load_order` | `true`, `20` |

**`cfg_column_mapping`** — map nguồn → schema chuẩn Silver (nền tảng của Silver generic)

| Cột | Ví dụ |
|---|---|
| `entity`, `source_system` | `customer`, `retail` |
| `source_column`, `target_column` | `created_at`, `inserted_at` |
| `data_type` | `timestamp` |
| `transform` | `parse_multi_date` / `std_text:title` / `map:gender` / `percent` |
| `is_business_key`, `is_nullable`, `is_tracked` | `false`, `false`, `true` |

**`cfg_table`** — thuộc tính vật lý mỗi bảng đích (nguồn cho DDL, maintenance, P12)

`table_name, layer, partition_cols, cluster_cols, v_order, enable_cdf, deletion_vectors, optimize_schedule, vacuum_retention_hours, load_method (append|merge|scd1|scd2|replace_partition), business_keys`

**`cfg_pipeline_step`** — DAG cho `nb_run_layer`

`step_id, layer, notebook, args (json), depends_on (list), is_active, timeout_sec, retry`

**`cfg_dq_rule`** — xem §12.

### 6.3 Reference tables (luật nghiệp vụ — P3)

| Bảng | Cột | Dùng cho |
|---|---|---|
| `ref_order_status` | `status, sequence, is_final, is_sales_recognized` | "Delivered sales" = `is_sales_recognized` → không viết `'Delivered'` trong code/SQL/DAX |
| `ref_value_mapping` | `domain, source_value, standard_value` | gender, position, brand, country |
| `ref_holiday_vn` | `holiday_date, holiday_name` | `gld_d_date.is_holiday` |
| `ref_fiscal` | `fiscal_start_month` (=7) | fiscal year |

### 6.4 State tables

- `state_watermark(step, entity, watermark_type [timestamp|delta_version], value, run_id, updated_at)`
- `state_file_manifest(file_id, source_system, entity, file_path, file_name, size, checksum, modified_at, batch_id, status [NEW|LOADED|DUPLICATE|FAILED], task_id, registered_at, loaded_at)`
- `schema_registry(table, column, data_type, layer, status [DETECTED|APPROVED|ACTIVE], first_seen_batch, approved_at)`
- `meta_schema_migrations(version, script, checksum, applied_at)`

### 6.5 Migrations

`migrations/V###__<mô tả>.sql` — đánh số tăng dần, chỉ thêm không sửa. `nb_setup_migrate` áp dụng các version chưa có trong `meta_schema_migrations` (giống Flyway). Mọi DDL của Silver/Gold/meta đi qua đây (P11).

---

## 7. Source simulator

**Mục tiêu:** 2 nguồn "sống", data mới mỗi ngày, scale được, tái lập được (seed cố định).

### 7.1 Item

| Item | Thiết kế |
|---|---|
| `sqldb_erp_wholesale` | Fabric SQL Database, mô hình **hybrid** (ADR 008): ép `datetime2` UTC, PK, NOT NULL trên key; giữ nguyên giá trị bẩn (status, tax_rate, tên…); FK **khai báo nhưng NOCHECK**. `orders` có `order_line_id` IDENTITY làm PK (append theo status) + **index `updated_at`**. Schema `sim.stg_*` = staging nội bộ simulator. DDL: SQL project `fabric/source/sqldb_erp_wholesale.SQLDatabase/` (Git sync) là nguồn sự thật — thay đổi schema sửa trên DB rồi Commit. |
| `lh_retail_drop` | drop zone của các nguồn file: `Files/inbound/<source>/<entity>/` — CSV giữ nguyên byte dữ liệu gốc (free-form). |
| `lh_sim` | nội bộ simulator: `Files/seed/` (9 CSV đề bài), `seed_<src>_<entity>`, `sim_state`, `sim_release_log`, `sim_reject_log`. Platform **không** đọc. |

**Nguồn** (`SOURCES` trong `nb_00_sim_common`):

| Source | Kiểu | Entity | Đích |
|---|---|---|---|
| `wholesale` | `erp` | customers, products, sales_hierarchy, orders | `sqldb_erp_wholesale.dbo.*` |
| `retail` | `file` | customers, products, sales_hierarchy, orders | `inbound/retail/<entity>/` |
| `reference` | `file` | categories | `inbound/reference/categories/` |

> **Vì sao hybrid:** đề mô tả wholesale "schema-enforced, FK enforced" nhưng data mẫu wholesale bẩn như retail (orphan `CUS099`/`PRD999`, status sai chính tả, qty âm, tax `five percent`, `CAT010` không tồn tại…). Enforce thật → lỗi bị chặn ở nguồn, platform không còn gì để xử lý/demo Q1/Q5.
>
> **Vì sao `categories` là file `reference`, không thuộc ERP:** đề chỉ liệt kê bảng ERP là orders, customers, products, sales_hierarchy; file `categories.csv` không có hậu tố nguồn → file tham chiếu dùng chung. Đặt trong ERP thì PK chặn mất dòng trùng `CAT005` (2 phiên bản: `electronics/All-in-One` vs `Electronics/AllInOne`) — đó là issue Q1 mà Platform phải tự phát hiện (DQ `unique`) và xử lý bằng survivorship ở Silver. Với data seed hiện tại, ERP không từ chối dòng nào; cơ chế reject giữ cho generate/drill.

### 7.2 Timeline

**Virtual clock** — ngày giả lập (`lh_sim.sim_state.released_until`) độc lập với ngày thật; data giữ nguyên ngày gốc.

- Mỗi dòng seed có `_release_date` = ngày nó "xuất hiện" ở nguồn: orders theo `updated_at`; master theo `max(created/inserted, updated)`.
- Không parse được hoặc **sau `future_date_cutoff` (2026-12-31)** → coi là ngày sai đã nằm sẵn trong nguồn → release ở initial load (platform bắt bằng DQ `future_date`). Đây là ~40 dòng năm 2027 mỗi nguồn.
- **Initial load** tới `initial_until = 2025-12-31`: ~42k dòng/nguồn (retail: 1 file `orders_history_until_20251231.csv`).
- **Replay** mỗi lần tiến `p_days` (mặc định 1): 2026-01-01 → ~2026-05-10, ~130 ngày, median ~62 dòng/ngày/nguồn (retail 1 file/ngày).
- Kịch bản đề rơi vào giai đoạn replay: Q14 (02/01 → 15/01/2026) ✅; Q15 (05/2026) chỉ có 1–10/05 từ seed; Q16 (01/06/2026) cần **generate**.
- `p_sim_date` = nhảy tới đúng ngày (tua nhanh); exit value = ngày giả lập mới → `p_load_date` cho Platform. `pl_sim_drive(p_days)` lặp: sim 1 ngày → `pl_master_daily(p_load_date)`.
- **Idempotency:** chạy lại sau lỗi → cùng cửa sổ (state chỉ tiến ở bước cuối), mọi ghi đều lặp được (MERGE · DELETE+INSERT 1 transaction · file trùng tên · log `replaceWhere window_to`). Gọi `p_days` nhiều lần = đồng hồ tiến nhiều lần (chủ đích). **Quy ước: pipeline luôn truyền `p_sim_date` cụ thể** → retry cùng ngày là no-op. Không chạy 2 phiên `nb_00_sim_daily` song song (không có lock; lịch/pipeline đảm bảo tuần tự).
- Hết seed (~05/2026) → chuyển sang generate (7.4) — cần làm trước drill Q15/Q16.

### 7.3 Notebook

| Notebook | Làm gì | Trạng thái |
|---|---|---|
| `nb_00_sim_common` | config + hàm dùng chung (`%run`) | ✅ draft |
| `nb_00_sim_setup` | seed CSV → `seed_*` + `_release_date` | ✅ draft |
| `nb_00_sim_daily(p_days, p_sim_date)` | virtual clock: release cửa sổ `(released_until, released_until + p_days]`; nguồn `erp` → MERGE / xoá-chèn theo cửa sổ (1 transaction), vi phạm NOT NULL/PK → `sim_reject_log`; nguồn `file` → CSV nguyên trạng; chạy lại không nhân đôi; exit value = ngày giả lập | ✅ chạy được (initial load 2026-10-06) |
| `nb_00_sim_reset` | xoá dữ liệu Source để replay từ đầu (`p_confirm = "RESET"`) | ✅ draft |
| `nb_00_sim_master_change(p_sim_date)` | đổi master tại chỗ: địa chỉ khách, sản phẩm mới, **org chart** (promote/đổi manager/nghỉ) — không giữ lịch sử (đúng Q11) | P10 |
| `nb_00_sim_late_orders` · `nb_00_sim_schema_v2` | drill Q14 · Q16 | P10 |

### 7.4 Sinh order hằng ngày

- **Giai đoạn 1 — replay:** phát lại 50k dòng gốc mỗi nguồn theo `_release_date` (7.2).
- **Giai đoạn 2 — generate:** sinh đơn mới theo phân phối học từ data gốc (khách, sản phẩm, giá, số dòng/đơn, mùa vụ).
- **Vòng đời status:** mỗi ngày một phần đơn cũ chuyển `Pending → Shipped → Delivered | Cancelled` → **dòng mới** (audit trail). Pipeline buộc phải xử lý thay đổi trên đơn cũ.
- **`scale_factor`** (Variable Library): SF=1 → ~1–2k dòng/ngày (hằng ngày); SF=1000 → ~1–2M dòng/ngày (drill Q12).
- **Bơm lỗi** theo `sim_scenario` (tỉ lệ): file trùng (khác tên), qty NULL/âm, price 0, orphan customer/salesman, ngày sai locale, cột thừa, dòng trùng, file rỗng, file đến trễ.
- **Seed cố định** → kết quả tái lập được, dùng cho golden test.
- Tối ưu: sinh bằng Spark (`range` + random seed), JDBC batch insert, CSV `coalesce` theo kích thước file thực tế.

---

## 8. Ingest & Bronze

**Lands:** dữ liệu thô đúng như nguồn + metadata. **Leaves:** batch mới (`_batch_id`) cho Silver.

### 8.1 Phân vai

| Bước | Công cụ | Lý do |
|---|---|---|
| **Extract** Source → `lh_platform/Files/landing/` | **Pipeline Copy activity** | connector, incremental filter, retry, song song, output `rowsCopied/filesWritten`, không tốn Spark |
| **Load** landing → `brz_*` | **1 notebook `nb_brz_load`** chạy mọi entity của run bằng `runMultiple` | 1 Spark session cho tất cả, thay vì N session |

### 8.2 `pl_ingest`

```
pl_ingest(p_load_date, p_run_id)
  run_start = utcNow()                                ← upper bound cố định cho cả run
  Lookup cfg_source_entity (is_active) ⋈ state_watermark
  ForEach entity (concurrency 4–8)                    ← chỉ Copy, không Spark
    Switch source_type
      db   → Copy: SELECT … WHERE updated_at >  wm − lookback
                              AND updated_at <= run_start
              → landing/wholesale/<entity>/load_date=<d>/batch=<id>/*.parquet
      file → Copy (binary): inbound/<source>/<entity>/*  filter LastModified (wm, run_start]
              → landing/<source>/<entity>/load_date=<d>/batch=<id>/      (source = retail | reference)
    ghi kết quả copy (rows/files, batch_id) vào biến mảng
  nb_brz_load(batches = [...])                        ← 1 notebook, runMultiple
  nb_ops_set_watermark(batches)                       ← CHỈ khi copy + load SUCCESS
```

- **Upper bound `run_start`** → dòng commit giữa lúc extract vào run sau, không mất.
- **Lookback** (vd 2 ngày) → bắt dòng commit trễ; trùng được dedup ở Silver.
- **Watermark chỉ tiến khi thành công** → run lỗi thì run sau tự lấy lại đúng khoảng (P6).
- Master nhỏ: `full_snapshot` mỗi ngày → **giữ vĩnh viễn** ở Bronze (nguồn lịch sử duy nhất cho SCD2 — P7).
- Scale/backfill DB: Copy **partition option = dynamic range** + tăng degree of parallelism.

### 8.3 `nb_brz_load` (mỗi batch)

1. File: tính checksum → `state_file_manifest`; checksum đã `LOADED` → `DUPLICATE`, bỏ qua.
2. Đọc: parquet (DB) / CSV `header=True, inferSchema=False` (**all string**), `PERMISSIVE`, `_corrupt_record`.
3. Thêm metadata (§5.2).
4. Cột mới so với `schema_registry` → `mergeSchema` + ghi `DETECTED` + alert (không fail).
5. Ghi `brz_<src>_<entity>` bằng `replaceWhere _batch_id = '<id>'` (idempotent).
6. Manifest → `LOADED`; log metrics.

### 8.4 Phương án đã cân nhắc (ADR 002)

| Phương án | Khi nào |
|---|---|
| Copy job (incremental tự quản lý) | cần ít cấu hình; khó gắn log/manifest/DQ theo chuẩn riêng |
| Spark streaming `availableNow` + checkpoint | rất nhiều file, cần exactly-once theo file |
| Shortcut / mirroring | không cần landing copy — không dùng để mô phỏng ingest thật |

---

## 9. Silver

**Lands:** dữ liệu đã map, cast, chuẩn hoá, dedup, conform giữa 2 nguồn; lỗi → quarantine.
**Leaves:** bảng sạch theo entity, có `_record_hash`, CDF bật cho bảng Gold cần đọc incremental.

### 9.1 Đọc incremental

Đọc **batch Bronze chưa xử lý** (`_batch_id` có trong log Bronze SUCCESS, chưa có ở Silver). `p_full_reload` → đọc toàn bộ Bronze.

### 9.2 Silver generic — `nb_slv_generic(entity)`

```
batches của mọi source_system của entity
→ apply_mapping(cfg_column_mapping)       đổi tên, cast, transform → schema chuẩn chung
→ union các nguồn                          (không có if source == …)
→ dq.validate(ruleset silver)              → ok / quarantine
→ dedup_latest(business_keys, updated_at)
→ add_hash(tracked cols)
→ merge_delta theo cfg_table.load_method   (chỉ update khi hash khác)
```

Transform được tham chiếu trong mapping (mỗi cái là 1 hàm package, có unit test): `parse_multi_date`, `std_text:{upper|title|trim}`, `map:<domain>` (→ `ref_value_mapping`), `percent`, `to_utc:<source_timezone>`.

### 9.3 Bảng Silver

| Bảng | Grain / key | Notebook | Ghi |
|---|---|---|---|
| `slv_product` | `product_code` | generic | MERGE (hash) |
| `slv_category` | `category_code` | generic | MERGE |
| `slv_sales_hierarchy` | `source_system, salesman_code` | generic + hook chuẩn hoá self-reference | MERGE (snapshot hiện tại) |
| `slv_customer` | `customer_code` | **đặc thù** `nb_slv_customer`: survivorship 2 nguồn (rule trong config: thuộc tính nào nguồn nào thắng, fallback `updated_at` mới nhất), `source_flags` (`W`/`R`/`WR`) | MERGE |
| `slv_orders_history` | `order_no, product_code, order_status, updated_at` | generic | MERGE insert-only |
| `slv_orders_current` | `order_no, product_code` | **đặc thù** `nb_slv_orders_current` (Q10) | MERGE, chỉ update khi `src.updated_at > tgt.updated_at` (chống out-of-order) |
| `quarantine_<entity>` | — | `dq.quarantine` | append: dòng gốc + `rule_id`, `reason`, `_batch_id` |

> Đề Q10 nói "drop duplicates on order_no" — grain đúng là **order_no + product_code**. Trình bày cả hai và giải thích.

### 9.4 Tối ưu

- Orders: partition `order_month` (hoặc liquid clustering `order_date`) theo `cfg_table`.
- MERGE có **partition pruning**: thêm `tgt.order_month IN (<tháng trong batch>)`.
- **Deletion Vectors** bật; **CDF** bật trên `slv_orders_current`, `slv_customer`, `slv_product`, `slv_sales_hierarchy`.
- Dedup source **trước** MERGE.

---

## 10. Gold

**Lands:** star schema cho *delivered sales by product, customer, country, sales hierarchy*.
**Leaves:** semantic model Direct Lake, SQL endpoint cho analyst.

### 10.1 Bảng

| Bảng | Grain | Key | Load | Lý do key |
|---|---|---|---|---|
| `gld_d_date` | 1 ngày | `date_key` YYYYMMDD | generate (Q9), mở rộng hằng năm | smart key dễ đọc, partition-friendly |
| `gld_d_customer` | 1 khách | `customer_sk = xxhash64(customer_code)` | SCD1 (generic `scd1_merge`) | SK tách khỏi code nguồn, có unknown member |
| `gld_d_product` | 1 sản phẩm (+ category lvl1–4) | `product_sk` | SCD1 | nhất quán, unknown member |
| `gld_d_salesman` | 1 **phiên bản** của 1 salesman | `salesman_sk = xxhash64(source_system, salesman_code, _valid_from)` | **SCD2** + hierarchy flatten | 1 code nhiều phiên bản |
| `gld_f_sales_line` | 1 dòng sản phẩm của đơn đã ghi nhận doanh thu (`ref_order_status.is_sales_recognized`) | `order_no, product_code` | MERGE incremental (CDF) | — |
| `gld_a_sales_month` | tháng × product × customer × salesman | — | replace partition bị ảnh hưởng | tăng tốc báo cáo |

**Measures fact:** `quantity`, `unit_price`, `gross_amount`, `tax_amount`, `net_amount` (+ `discount_amount` sau Q16).
**FK:** `order_date_key`, `recognized_date_key`, `customer_sk`, `product_sk`, `salesman_sk` (point-in-time), `_source_system`.

### 10.2 Kỹ thuật

- **Unknown / inferred member:** FK không khớp → `-1` + flag; khi master về muộn → tạo inferred member, cập nhật khi master đến.
- **SCD2 `gld_d_salesman` (Q11)** — `scd2_merge` generic:
  - so snapshot `slv_sales_hierarchy` với `_is_current` theo `_record_hash`
  - đổi → đóng dòng cũ (`_valid_to = p_load_date − 1`) + mở dòng mới (`_valid_from = p_load_date`)
  - biến mất ở nguồn → đóng dòng (nghỉ việc)
  - **flatten hierarchy tại thời điểm snapshot** (self-join 4 cấp, chặn self-loop) → report point-in-time không cần đệ quy
  - snapshot đầu tiên `_valid_from = 1900-01-01`
  - **rebuild được** từ snapshot hằng ngày ở Bronze (P7)
- **Fact incremental:** đọc CDF `slv_orders_current` từ version trong `state_watermark` → keys thay đổi → build lại → MERGE (đơn bị huỷ sau khi ghi nhận → xoá). SCD2 lookup: `order_date BETWEEN _valid_from AND _valid_to` (broadcast dim).
- **CDF hết hạn** (version cũ đã bị VACUUM) → tự chuyển sang rebuild các partition bị ảnh hưởng / `p_full_reload` + alert.
- **Aggregate:** `replaceWhere order_month IN (<tháng có thay đổi>)`.

### 10.3 Tối ưu

Fact: partition `order_month` + Z-order `customer_sk, product_sk` (hoặc liquid clustering), **V-Order**, OPTIMIZE sau load lớn. Dim: không partition, V-Order.

---

## 11. Log & observability

```
log_pipeline_run      1 dòng / lần chạy pl_master_daily        (run_id)
 └── log_task_run     1 dòng / step / entity                    (task_id)
      ├── state_file_manifest
      ├── dq_result_log
      └── recon_result
```

**`log_task_run`:** `task_id, run_id, p_load_date, layer, step_id, source_system, entity, target_table, notebook, load_mode (incremental|full), watermark_from, watermark_to, rows_read, rows_inserted, rows_updated, rows_deleted, rows_rejected, rows_deduped, status (RUNNING|SUCCESS|FAILED|SKIPPED), error_message, started_at, ended_at, duration_sec, delta_version`

- Số insert/update/delete lấy từ `operationMetrics` của Delta history — không đếm lại.
- `delta_version` → rollback bằng `RESTORE TABLE … VERSION AS OF`.
- Ghi `RUNNING` khi bắt đầu, cập nhật khi kết thúc (try/finally). Task `RUNNING` quá timeout → alert.
- Một hàm duy nhất trong package (P4); log append/MERGE theo `task_id`, không ghi đè.

**Alert** (1 thông báo tổng hợp / run, tránh spam): run FAILED · DQ critical · freshness trễ · recon lệch · task treo.

---

## 12. Data quality & reconciliation

### 12.1 `cfg_dq_rule` (YAML → bảng)

`rule_id, layer, table_name, check_type, column_name, expression, threshold_pct, severity (critical|warning), action (reject|quarantine|flag|alert), is_active, description`

| check_type | Ví dụ |
|---|---|
| `not_null` · `unique` | business key |
| `fk_exists` | `orders.customer_code → slv_customer` |
| `range` | `quantity > 0`, `price > 0`, `tax_rate BETWEEN 0 AND 0.2` |
| `accepted_values` | `order_status ∈ ref_order_status` |
| `parsable` / `regex` | ngày parse được, `customer_code ~ CUS\d{3}` |
| `freshness` | batch hôm nay đã về |
| `volume` | count trong ±50% trung bình 7 ngày |
| `schema` | không có cột chưa đăng ký |
| `scd2_no_overlap` | khoảng hiệu lực không chồng lấp |

### 12.2 Theo layer (tối thiểu)

| Layer | Rule | On failure |
|---|---|---|
| Bronze | freshness theo entity | alert |
| Bronze | header khớp registry | mergeSchema + DETECTED + alert |
| Bronze | volume bất thường | alert |
| Silver | business key not null & unique | quarantine |
| Silver | qty > 0, price > 0, ngày parse được, status hợp lệ | quarantine |
| Silver | FK tồn tại | flag (→ `-1` ở Gold) |
| Gold | fact không trùng grain, không FK NULL | critical → dừng |
| Gold | SCD2 không chồng lấp | critical → dừng |
| Gold | tổng doanh thu Gold = Silver | critical → dừng + alert |

### 12.3 Engine

`dq.validate()` sinh SQL từ rule, chạy **trên batch hiện tại** (không quét toàn bảng) → tách ok / bad → ghi `dq_result_log(run_id, rule_id, table, checked_rows, failed_rows, fail_pct, status, sample_keys, executed_at)` (MERGE theo `run_id, rule_id`) → `critical` FAIL → raise.

### 12.4 Reconciliation

`recon_result(run_id, check_name, left_layer, right_layer, left_count, right_count, left_amount, right_amount, diff, status)`

- Source → Bronze: rows Copy = rows Bronze theo batch.
- Bronze → Silver: `bronze = silver_new + quarantined + deduped`.
- Silver → Gold: tổng `net_amount` doanh thu theo tháng.
- Gold → Semantic model: DAX query qua semantic-link so với SQL.

---

## 13. Orchestration

### 13.1 `pl_master_daily(p_load_date = hôm qua theo business_timezone)`

```
nb_ops_run_start                     run_id, log_pipeline_run
→ pl_ingest                          Copy (ForEach) → nb_brz_load → watermark
→ nb_run_layer(layer='silver')       DAG từ cfg_pipeline_step → runMultiple (1 session)
→ nb_run_layer(layer='gold')         dims → facts → aggregates
→ nb_ops_recon
→ Semantic model refresh (sm_sales)
→ nb_ops_run_end → notify (tóm tắt)
On failure (mọi bước) → nb_ops_run_end(FAILED) → notify
```

- **`nb_run_layer`** đọc `cfg_pipeline_step` → dựng DAG `runMultiple` (dependencies, timeout, retry) → DQ chạy bên trong từng step. Thêm bảng = thêm step trong YAML (P2).
- Pipeline lo thứ tự giữa các layer, retry (2 lần/60s), timeout, alert; Spark lo song song trong layer.
- Lịch: simulator 05:00 → master 06:00 → maintenance (tuần) 02:00 CN.

### 13.2 `pl_backfill(p_from, p_to, p_mode)`

- `daily`: ForEach ngày **tuần tự** gọi `pl_master_daily` (giữ thứ tự watermark/SCD2).
- `rebuild`: `p_full_reload=True` → rebuild Silver/Gold từ Bronze theo partition tháng, song song theo tháng.

### 13.3 Idempotency từng bước

| Bước | Cơ chế |
|---|---|
| Copy DB | upper bound cố định; watermark chỉ tiến khi thành công |
| Copy file | LastModified window + manifest checksum |
| Bronze | `replaceWhere _batch_id` |
| Silver | dedup + MERGE theo business key, điều kiện `updated_at` / hash |
| Gold SCD2 | so hash; cùng `p_load_date` chạy lại không tạo version mới |
| Gold fact / agg | MERGE theo grain / replace partition |
| Log / DQ / recon | MERGE theo `task_id` / `(run_id, rule_id)` |

---

## 14. Semantic model & report

- `sm_sales`: **Direct Lake** trên schema `gld` của `lh_platform`; star schema; `gld_d_date` mark as date table; ẩn SK & cột kỹ thuật.
- Measures (định nghĩa 1 lần, display folder rõ ràng): `Sales`, `Qty`, `Tax`, `Net Sales`, `Sales LY`, `YoY %`, `MTD/QTD/YTD`, `Top N Customer`, `Avg Order Value`; sau Q16 `Discount`, `Net After Discount`. Fact chỉ chứa doanh thu đã ghi nhận → DAX không lọc status (P3, P4).
- Hierarchy point-in-time qua `salesman_sk` → DAX không cần logic thời gian.
- (tuỳ chọn) RLS theo hierarchy.
- `rpt_sales`: Overview · Product · Customer/Country · Sales hierarchy.
- `sm_ops` + `rpt_pipeline_health` (trên schema `meta`): run status, duration, rows/ngày, DQ trend, freshness, recon.
- Tối ưu Direct Lake: V-Order, OPTIMIZE, chỉ cột cần, aggregate table, theo dõi fallback DirectQuery.
- Power BI Free xem được report trong workspace có Fabric capacity (trial).

---

## 15. Performance & scale playbook

### 15.1 Thuộc tính bảng (khai báo trong `cfg_table`)

| Bảng | Partition / cluster | V-Order | DV | CDF | OPTIMIZE | VACUUM |
|---|---|---|---|---|---|---|
| `brz_*_orders` | `_ingest_date` | ✗ | — | ✗ | tuần | 7 ngày |
| `brz_*` master | — | ✗ | — | ✗ | tuần | 7 ngày (data giữ vĩnh viễn) |
| `slv_orders_history` | `order_month` | ✗ | ✓ | ✗ | ngày | 7 ngày |
| `slv_orders_current` | `order_month` | ✗ | ✓ | ✓ | ngày | ≥ 7 ngày (> max downtime) |
| `slv_*` master | — | ✗ | ✓ | ✓ | tuần | ≥ 7 ngày |
| `gld_f_sales_line` | `order_month` + Z-order `customer_sk, product_sk` | ✓ | ✓ | ✗ | sau load | 7 ngày |
| `gld_d_*`, `gld_a_*` | — | ✓ | — | — | sau load | 7 ngày |

### 15.2 Đọc (Q12 — query)

Partition pruning · data skipping (Z-order / liquid clustering) · V-Order + OPTIMIZE (file ~128MB–1GB) · aggregate table · broadcast dim nhỏ · filter trước join · không `SELECT *`.

### 15.3 Ghi (Q12 — write)

Chỉ xử lý delta (watermark → batch → CDF) · MERGE có pruning predicate · Deletion Vectors · optimized write / auto-compaction · tránh over-partition (không partition theo ngày khi < 1GB/ngày, không theo cột cardinality cao) · Native Execution Engine · resource profile theo layer (write-heavy Bronze/Silver, read-heavy Gold) · AQE · backfill song song theo tháng · 1 Spark session / layer (`runMultiple`, high concurrency).

### 15.4 Drill scale (bằng số thật)

SF=1000 → vài chục–vài trăm triệu dòng, đo:
1. Query sales/tháng: trước vs sau partition + Z-order + V-Order.
2. MERGE 1 ngày vào fact: có vs không pruning predicate.
3. Số file/kích thước trước vs sau OPTIMIZE.
4. Thời gian `pl_master_daily`: N notebook riêng vs `runMultiple`.

---

## 16. Vận hành, bảo trì & phục hồi

| Việc | Cách làm | Tần suất |
|---|---|---|
| OPTIMIZE / VACUUM | `pl_maintenance` → `nb_ops_maintenance` đọc `cfg_table` | theo lịch trong config |
| Monitoring | Monitoring hub + `rpt_pipeline_health` | hằng ngày |
| Alert | §11 | real-time |
| Capacity | Fabric Capacity Metrics app (CU, throttling) | hằng tuần |
| Rollback bảng | `RESTORE TABLE … VERSION AS OF <delta_version>` | sự cố |
| Rebuild layer | `pl_backfill(mode=rebuild)` từ Bronze | sự cố / thay đổi logic |
| Runbook | `docs/runbook.md`: triệu chứng → kiểm tra → xử lý | cập nhật liên tục |

**Thứ tự debug chuẩn** (dùng cho Q15): report → semantic refresh → Gold (`DESCRIBE HISTORY`, `log_task_run`) → Silver → Bronze → `state_watermark` / `state_file_manifest` → Copy output → Source.

---

## 17. Scenario drills

| Drill | Chuẩn bị | Hệ thống xử lý | Bằng chứng |
|---|---|---|---|
| **Q14** late-arriving | ngày 2026-01-15 insert đơn `order_date = 2026-01-02` | watermark `updated_at` bắt được; fact MERGE vào partition tháng 1; aggregate tháng 1 rebuild; SCD2 lookup theo `order_date` | log, recon, report tháng 1 |
| **Q15** mất tháng 5 | xoá partition tháng 5 ở Gold / làm hỏng watermark | thứ tự debug §16 → `RESTORE` hoặc `pl_backfill 2026-05-01..31` | timeline + recon |
| **Q16** schema evolution | `nb_00_sim_schema_v2` từ 2026-06-01 | Bronze mergeSchema → registry DETECTED → PR: migration + mapping + APPROVED → Silver/Gold có cột, lịch sử NULL (không backfill giả) → measure mới | PR, registry, report |
| **Q11** SCD2 | đổi org chart giữa tháng 3 | version mới; fact point-in-time | report team tháng 3 đúng cơ cấu cũ |
| **Q12** scale | SF=1000 | §15.4 | bảng số liệu |
| Rebuild | xoá Silver + Gold | `pl_backfill(mode=rebuild)` | kết quả = trước khi xoá |
| File lỗi | `sim_scenario` | DUPLICATE / quarantine / alert | log |

---

## 18. Roadmap

**Chiến lược: lát cắt dọc trước, mở rộng bằng config sau.** Entity `orders` đi xuyên Source → Report trước; entity thứ 2 trở đi phải **không cần code mới** — nếu cần, framework sai và sửa framework.

### Phase 0 — Foundation (1 ngày)
- [x] 3 workspace (đổi "Prob" → "Prod")
- [ ] Gỡ extension VS Code; xoá `.stubs/ .vfscache/ .vfsmeta/ .work-folder-info`
- [ ] Git integration (§3.1); branch protection `main`
- [ ] Source: `sqldb_erp_wholesale`, `lh_retail_drop`; Platform-Dev: `lh_platform` (schema-enabled; schema `brz`, `slv`, `gld`, `meta`), `env_common`, `vl_config`, connection tới SQL DB
- [ ] Repo skeleton (§3.2), `README`, `docs/conventions.md` (= §1, §4, §5)
- [ ] Package skeleton + CI (`lint`, `unit`, `build`) chạy xanh
- [ ] `nb_setup_migrate`, `nb_setup_config`; `V001__meta.sql`

**DoD:** wheel build từ CI, gắn vào `env_common`, notebook `import companya_de` chạy được; commit Fabric → GitHub → Update Prod với 1 item thử.

### Phase 1 — Discovery: profiling & DQ scan (Q1) (1 ngày)
- [ ] `sandbox/nb_profiling` (không commit) → `docs/dq_findings.md`: issue | bảng/cột | dòng mẫu | loại | query detect | xử lý | rule_id
- [ ] Từ findings: viết `column_mapping.yml`, `dq_rule.yml`, `ref_*.yml` bản đầu

**DoD:** mỗi issue có query detect + cách xử lý + rule/mapping tương ứng.

### Phase 2 — Simulator tối thiểu (1 ngày)
- [ ] DDL ERP (PK/FK/index), `nb_00_sim_setup`, `nb_00_sim_seed`
- [ ] `nb_00_sim_daily` chế độ **replay**
- [ ] Schedule 05:00

**DoD:** 7 ngày replay → nguồn tăng đúng; reject log có dòng vi phạm FK.

### Phase 3 — Vertical slice: orders end-to-end (3 ngày)
- [ ] Package: `env`, `config`, `io`, `transforms`, `keys`, `dq` (cơ bản), `logging`, `runner` + unit test
- [ ] `pl_ingest` (cả db & file) + `nb_brz_load`
- [ ] `nb_slv_generic` cho orders → `slv_orders_history`; `nb_slv_orders_current`
- [ ] Gold tối thiểu: `gld_d_date`, dims chỉ có unknown member + code, `gld_f_sales_line`
- [ ] `nb_run_layer`, `pl_master_daily` (bản đầu), log + DQ cơ bản + recon
- [ ] `sm_sales` tối thiểu: 1 trang Sales by month/product
- [ ] Idempotency test

**DoD:** 7 ngày liên tục chạy tự động; rerun 1 ngày không đổi; số report = SQL. **Tag `v0.1`.**

### Phase 4 — Breadth: mọi entity bằng config (2 ngày)
- [ ] Thêm product, category, hierarchy, customer **chỉ bằng YAML** (kiểm chứng S3)
- [ ] `nb_slv_customer` (survivorship — ADR 003)
- [ ] Gold dims SCD1 đầy đủ (`scd1_merge`)

**DoD:** PR thêm entity không chạm `src/` (trừ survivorship); recon xanh.

### Phase 5 — Gold đầy đủ (2 ngày)
- [ ] `scd2_merge` + `nb_gld_d_salesman` (flatten, chặn self-loop) — Q7, Q11
- [ ] `gld_d_date` đầy đủ (Q9) + `ref_holiday_vn`, `ref_fiscal`
- [ ] Fact incremental bằng CDF + fallback khi CDF hết hạn
- [ ] `gld_a_sales_month`
- [ ] ADR 001 (Gold model — Q3/Q4)

**DoD:** rebuild full == incremental; SCD2 không chồng lấp; không FK NULL.

### Phase 6 — DQ, recon & alert đầy đủ (1 ngày)
- [ ] Rule đủ §12.2; recon 4 cặp; alert tổng hợp

**DoD:** lỗi bơm từ simulator → log đúng; critical dừng pipeline.

### Phase 7 — Orchestration hardening (1 ngày)
- [ ] `pl_backfill` (daily / rebuild), `pl_maintenance`, timeout/retry, task treo
- [ ] `nb_ops_test_idempotency`, `nb_ops_test_rebuild`
- [ ] CI: `config`, `check_hardcode`, `check_orphans`, `check_outputs`

**DoD:** CI đầy đủ xanh; rebuild từ Bronze == hiện trạng.

### Phase 8 — Semantic model & report (1 ngày)
- [ ] `sm_sales` đầy đủ measures, `rpt_sales`; `sm_ops` + `rpt_pipeline_health`; (RLS)

**DoD:** report = SQL Q5 = recon.

### Phase 9 — Release Prod (½ ngày)
- [ ] PR `dev → main` → Update Prod → `nb_setup_migrate` + `nb_setup_config` → `pl_backfill` → schedule
- [ ] **Tag `v1.0`**

**DoD:** Prod chạy độc lập theo lịch.

### Phase 10 — Simulator nâng cao + drills (2 ngày)
- [ ] `generate` + vòng đời status + `scale_factor` + bơm lỗi + `nb_00_sim_master_change`
- [ ] Drills §17 → `docs/runbook.md`; golden test
- [ ] **Tag `v1.1`**

### Phase 11 — SQL answers (1 ngày)
- [ ] `q05` (+ breakdown theo loại lỗi, mỗi điều kiện 1 câu giải thích), `q06` (`DENSE_RANK`), `q07` (recursive CTE chặn self-loop), `q17` (review)
- [ ] (optional) Part II Q18–Q22

### Phase 12 — Deliverable (1 ngày)
- [ ] PPT theo thứ tự câu hỏi: trả lời → bằng chứng → link repo
- [ ] Diagram: kiến trúc (Q2), ERD Gold (Q3), DAG, lineage
- [ ] README hoàn chỉnh

### Timeline

```
P0  P1  P2  P3  P4  P5  P6  P7  P8  P9  P10 P11 P12
1   1   1   3   2   2   1   1   1   ½   2   1   1     ≈ 17.5 ngày công
```

| Milestone | Sau | Tag |
|---|---|---|
| Lát cắt dọc orders chạy hằng ngày | P3 | `v0.1` |
| Mọi entity + Gold đầy đủ | P5 | `v0.5` |
| Release Prod | P9 | `v1.0` |
| Drills + scale | P10 | `v1.1` |

**Nếu gấp:** giữ P0–P3, P5 (SCD2), P11, P12; P4 chỉ customer + product; bỏ CDF (replace partition theo tháng); bỏ Prod (tag trên `main`); P10 chỉ Q14 + Q15.

---

## 19. Mapping câu hỏi → phần

| Q | Nội dung | Phần |
|---|---|---|
| Q1 | DQ scan | P1, `dq_findings.md` |
| Q2 | Pipeline medallion | §2, §8–10 |
| Q3 | Schema Gold | §10.1, ADR 001 |
| Q4 | Loading strategy | §8–10, §13.3 |
| Q5–Q7 | SQL | P11, `sql/answers/` |
| Q8 | DQ per layer | §12.2 |
| Q9 | dim_date | §10.1, `nb_gld_d_date` |
| Q10 | Silver orders | §9.3 |
| Q11 | SCD2 hierarchy | §10.2 |
| Q12 | Scale | §15 |
| Q13 | DQ automation | §11, §12.3 |
| Q14–Q16 | Late data / backfill / schema evolution | §17, §16 |
| Q17 | Code review | P11 |
| Q18–Q22 | Forecast FIFO (optional) | P11 |

---

## 20. Rủi ro & ADR

### 20.1 Rủi ro

| # | Rủi ro | Giảm thiểu |
|---|---|---|
| R1 | Phạm vi lớn → làm dở dang | lát cắt dọc trước, milestone có tag, kế hoạch "nếu gấp" |
| R2 | Trial capacity giới hạn CU → drill scale bị throttle | SF lớn chạy ngoài giờ; đo tương đối |
| R3 | Default lakehouse trỏ sai môi trường | `env.paths()` + Variable Library (§3.4) |
| R4 | Data thật ≠ mô tả đề (format ngày ngược, `created_at` vs `inserted_at`, products retail > wholesale) | ghi nhận ở Q1; xử lý bằng mapping/config |
| R5 | Grain orders (`order_no` vs `order_no + product_code`) | dùng grain đúng, giải thích ở Q10 |
| R6 | Mất lịch sử hierarchy nếu Gold hỏng | snapshot Bronze giữ vĩnh viễn; SCD2 rebuild được (P7) |
| R7 | CDF hết hạn khi dừng lâu | fallback tự động sang rebuild partition + alert |
| R8 | Framework quá phức tạp so với lợi ích | P15: chỉ trừu tượng hoá khi có ≥ 2 nơi dùng; kiểm chứng bằng S3 |
| R9 | Cập nhật wheel vào Environment mất thời gian | gom thay đổi package theo đợt; version rõ ràng |
| R10 | Một số tính năng Fabric (liquid clustering, resource profile, SQL Database) khác nhau theo runtime/trial | Đã xác nhận: workspace chạy **Runtime 2.0 (Spark 4.1, Delta 4.2, Python 3.13)**, SQL Database dùng được. Spark 4 bật **ANSI mặc định** → luôn dùng `try_cast` / `try_to_timestamp` khi parse dữ liệu bẩn |

### 20.2 ADR dự kiến

| ADR | Chủ đề |
|---|---|
| 001 | Gold model: grain, key, load method (Q3/Q4) |
| 002 | Ingest: Copy activity + notebook load (vs Copy job / streaming / shortcut) |
| 003 | Customer survivorship giữa 2 nguồn |
| 004 | Surrogate key tất định bằng hash |
| 005 | Config as code (YAML trong package) vs bảng sửa tay |
| 006 | Partition vs liquid clustering cho orders |
| 007 | Git sync theo workspace vs `fabric-cicd` |
| 008 | ERP giả lập "hybrid" (ép timestamp/PK, giữ giá trị bẩn, FK NOCHECK) vs strict vs raw; `categories` là file `reference` |
| 009 | Virtual clock cho simulator (initial load tới 2025-12-31, replay theo ngày giả lập) vs đồng hồ thật |
| 010 | 1 lakehouse `lh_platform` + schema theo layer (`brz`/`slv`/`gld`/`meta`) vs 1 lakehouse mỗi layer |
