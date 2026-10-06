# Plan — Fabric DE Project (Company A Sales)

> Mô phỏng 1 dự án Data Engineering **thật** trên Microsoft Fabric (đề *Data Engineer Case Study*), để sau này map 1-1 sang dự án thật.
> Làm trên **Fabric web UI** + **Git (GitHub)**. **SQL-first**: biến đổi bằng Spark SQL; Python chỉ là keo dán.
> Trạng thái: **Source ✅ · Platform: Bước 0 ✅ · Bước 1 Ingest ▶ (nhánh db chạy được)** · Bản kế hoạch v3 · 2026-10-06
> Thao tác click-by-click: [docs/guide/](guide/README.md) · Hiểu Source: [guide/source.md](guide/source.md)

## Mục lục
1. [Đánh giá hiện trạng & điều chỉnh](#1-đánh-giá-hiện-trạng--điều-chỉnh)
2. [Mục tiêu & tiêu chí thành công](#2-mục-tiêu--tiêu-chí-thành-công)
3. [Nguyên tắc](#3-nguyên-tắc)
4. [Kiến trúc](#4-kiến-trúc)
5. [Môi trường, Git & cách làm việc](#5-môi-trường-git--cách-làm-việc)
6. [Conventions](#6-conventions)
7. [Schema `meta`](#7-schema-meta)
8. [Ingest & Bronze](#8-ingest--bronze)
9. [Silver](#9-silver)
10. [Gold](#10-gold)
11. [Điều phối, log, DQ, recon, cảnh báo](#11-điều-phối-log-dq-recon-cảnh-báo)
12. [Semantic model & report](#12-semantic-model--report)
13. [Vận hành & hiệu năng](#13-vận-hành--hiệu-năng)
14. [Roadmap end-to-end](#14-roadmap-end-to-end)
15. [Câu hỏi đề → nơi trả lời](#15-câu-hỏi-đề--nơi-trả-lời)
16. [Rủi ro & ADR](#16-rủi-ro--adr)

---

## 1. Đánh giá hiện trạng & điều chỉnh

### 1.1 Đã có và chạy được

| Hạng mục | Trạng thái |
|---|---|
| Source: ERP + drop zone + simulator (virtual clock, đang ở 2026-01-01) | ✅ |
| Dev ws nối Git (`dev` / `fabric/platform`), Runtime 2.0, HC notebook tắt | ✅ |
| `lh_platform` (schema `brz/slv/gld/meta`) · `nb_setup_ddl` · `nb_setup_config` · `nb_common` | ✅ |
| `pl_ingest`: Set variable → Lookup (T-SQL) → ForEach → Switch → **nhánh db** ghi parquet vào landing | ✅ đã chạy |
| Spike: SQL function (K1) · `%%configure -f` theo tên (K2, standard session) · định dạng Git `%%sql` (K3) · Lookup T-SQL trên lakehouse (K4) | ✅ |

### 1.2 Chỗ chưa ổn → cách sửa

| # | Vấn đề | Vì sao chưa ổn | Sửa | Áp dụng |
|---|---|---|---|---|
| **A1** | Luật chuẩn hoá tách 2 nơi: bảng `ref_value_mapping` + SQL JOIN | Sửa 1 luật phải nghĩ "bảng hay code?"; liệt kê biến thể trong bảng không bao giờ đủ | **Bỏ bảng.** Mỗi miền giá trị = **1 SQL function** trong `nb_common` (`std_order_status`, `std_gender`, `std_position`, `std_tax_rate`…): chuẩn hoá bằng `norm_key` + `CASE`; không nhận ra → NULL → quarantine | Bước 2 |
| **A2** | `ref_order_status` cũng là luật ở dạng bảng | Cùng lý do A1 | **Bỏ bảng.** `is_sales_recognized(status)` + `status_sequence(status)` là SQL function trong `nb_common`. Report chỉ đọc Gold (đã lọc) nên không cần bảng | Bước 2 |
| **A3** | Config chỉ code đọc nhưng lại để ở bảng: `cfg_pipeline_step`, `cfg_dq_rule`, `ref_holiday_vn` | Thêm 1 bước phải sửa 2 nơi (notebook + `nb_setup_config`) | **Đưa vào code** ở đúng nơi dùng: DAG trong runner, rule DQ trong `nb_dq`, ngày lễ trong `nb_gld_date`. **Quy tắc mới (P4):** chỉ dùng bảng khi *pipeline/report* (không phải Spark) cần đọc, hoặc là dữ liệu runtime | Bước 3 |
| **A4** | 13 bảng meta tạo trước, 10 bảng rỗng | Rối, không biết bảng nào đang dùng | Giữ **6 bảng**, mỗi bảng tạo **đúng bước cần** (§7). Xoá bảng thừa ở Dev 1 lần | Bước 1 |
| **A5** | Watermark ERP định lấy theo giờ chạy | `updated_at` ERP là **giờ giả lập** → ngày sau không lấy được gì | ERP: `max(updated_at)` dữ liệu; file: `run_start`; 1 dòng / `load_date` (§8.3) | Bước 1 |
| **A6** | Master ERP lấy incremental theo `updated_at` (cho phép NULL) | Dòng `updated_at` NULL không bao giờ được lấy; không có snapshot cho SCD2 | Master ERP → **full snapshot mỗi ngày** (vài chục dòng); orders giữ incremental | Bước 1 |
| **A7** | `state_file_manifest` + checksum | Phức tạp, chưa cần: Bronze đã idempotent theo batch, Silver dedup theo khoá | **Bỏ.** Cột `_source_file` ở Bronze là đủ để truy vết file. Xem lại ở drill "file trùng" nếu cần | Bước 1 |
| **A8** | Cách làm việc nhảy cóc, thiết kế đổi giữa chừng | Bạn bị ngợp, phải sửa lại | Mỗi bước theo vòng **Thiết kế → Bạn duyệt → Làm → Kiểm → Ghi guide** (§5.3) | Từ giờ |
| **A9** | Guide 01-ingest/04–05 và 00-nen/03 mô tả bảng sắp bỏ | Lệch với plan v3 | Cập nhật guide khi làm từng bước | Bước 1–2 |

**Còn giữ nguyên** (đã cân nhắc, vẫn hợp lý): ingest bằng pipeline Copy (giống dự án thật); 1 lakehouse nhiều schema; landing + Bronze raw; SQL-first cho Silver/Gold; Q9/Q10 PySpark.

---

## 2. Mục tiêu & tiêu chí thành công

1. Trả lời Part I (Q1–Q17) bằng **bằng chứng chạy thật**; Part II (Q18–Q22) nếu còn thời gian.
2. Platform chạy **mỗi ngày với data mới** (virtual clock), thiết kế như ở quy mô lớn.
3. **Đơn giản:** ai biết SQL đọc/sửa được; mỗi logic nằm đúng 1 chỗ.

| # | Tiêu chí | Đo bằng |
|---|---|---|
| S1 | Chạy end-to-end tự động ≥ 7 ngày giả lập liên tục | `meta.log_pipeline_run` |
| S2 | Chạy lại bất kỳ ngày nào → kết quả không đổi | chạy 2 lần, so count + tổng tiền |
| S3 | Thêm 1 nguồn mới → chỉ thêm 1 dòng config (tới Bronze) | PR chỉ đụng `nb_setup_config` |
| S4 | Report = SQL = recon | `meta.recon_result` |
| S5 | Drill Q11, Q12, Q14, Q15, Q16 có runbook + bằng chứng | `docs/runbook.md` |

---

## 3. Nguyên tắc

| # | Nguyên tắc | Nghĩa là |
|---|---|---|
| P1 | **Git là nguồn sự thật** | Mọi notebook, pipeline, DDL, config, model qua Git; workspace dựng lại được từ repo |
| P2 | **Phần cơ học generic, phần nghiệp vụ tường minh** | Ingest/Bronze/log/DQ chạy chung cho mọi bảng; Silver/Gold là SQL viết rõ từng bảng |
| P3 | **Không hardcode môi trường** | Lakehouse gắn theo **tên**; không GUID/đường dẫn tuyệt đối |
| P4 | **Một logic — một chỗ, ưu tiên code** | Luật nghiệp vụ & chuẩn hoá = SQL function trong `nb_common`. **Bảng chỉ dùng khi** pipeline/report cần đọc (vd `cfg_source_entity`) hoặc là dữ liệu runtime (state, log) |
| P5 | **Incremental mặc định, full reload luôn có** | Tham số `p_full_reload` để rebuild từ Bronze |
| P6 | **Idempotent** | Chạy lại cùng `p_load_date` → cùng kết quả; không dùng `current_date()` trong logic |
| P7 | **Raw bất biến** | Landing + Bronze giữ nguyên dữ liệu gốc, chỉ thêm → downstream luôn rebuild được |
| P8 | **Không mất dữ liệu, không im lặng** | Dòng lỗi → quarantine kèm lý do; lỗi → log + cảnh báo |
| P9 | **Reconcile mọi bước** | Count & tổng tiền khớp Source → Bronze → Silver → Gold → report |
| P10 | **Truy vết được** | `_batch_id`, `_run_id`, `_load_date` đi xuyên layer |
| P11 | **Layer chỉ đọc layer ngay trước** (+ `meta`) | |
| P12 | **Thiết kế cho quy mô lớn** | Không full scan khi không cần, partition pruning, không `collect()` dữ liệu lớn |
| P13 | **Không code chết** | Mọi notebook thuộc pipeline/runner hoặc nhóm `setup`/`ops`/`sim` |
| P14 | **Kiểm trước khi promote** | DQ + recon + chạy lại 2 lần xanh trên Dev mới merge `main` |
| P15 | **Đơn giản trước** | Tạo thứ gì khi bước đó cần; quyết định lớn ghi ADR |

---

## 4. Kiến trúc

```
┌──────────── CompanyA-Source (xong — hệ thống "team khác", chỉ đọc) ───────────────┐
│ sqldb_erp_wholesale.dbo.{customers,products,sales_hierarchy,orders}   (wholesale) │
│ lh_retail_drop/Files/inbound/{retail/*, reference/categories}  (retail, reference)│
│ ▲ nb_00_sim_daily: +1 ngày giả lập mỗi lần chạy                                   │
└──────────┬──────────────────────────────────────────┬─────────────────────────────┘
           │ Copy (SQL query)                          │ Copy (binary, lọc LastModified)
           ▼                                           ▼
┌──────────── CompanyA-DataPlatform-<Dev|Prod> / lh_platform ───────────────────────┐
│ Files/landing/<source>/<entity>/load_date=<d>/batch=<RunId>/    raw, bất biến       │
│ brz.brz_<source>_<entity>        mọi cột chuỗi + cột kỹ thuật   ← nb_brz_load       │
│ slv.slv_<entity>, quarantine_*   sạch, chuẩn hoá, gộp nguồn      ← nb_slv_* (%%sql)  │
│ gld.gld_d_*, gld_f_*, gld_a_*    star schema                     ← nb_gld_* (%%sql)  │
│ meta.*                           config ingest, watermark, log, DQ, recon           │
│                                                                                    │
│ pl_master_daily → pl_ingest → nb_run_layer(slv) → nb_run_layer(gld) → nb_ops_recon │
│                 → refresh sm_sales → cảnh báo                                       │
│ sm_sales (Direct Lake → gld.*) → rpt_sales      rpt_pipeline_health (→ meta.*)      │
└────────────────────────────────────────────────────────────────────────────────────┘
```

| Quyết định | Lý do |
|---|---|
| Ingest bằng **pipeline Copy** | Dự án thật: nguồn sau firewall chỉ đi qua data gateway; credential trong Connection; retry/song song sẵn; không tốn Spark |
| **1 lakehouse, mỗi layer 1 schema** (ADR 010) | 1 SQL endpoint, query xuyên layer, tên khớp đề |
| Dev & Prod đọc chung Source, **state riêng** | Chạy lại Dev không ảnh hưởng Prod |

---

## 5. Môi trường, Git & cách làm việc

### 5.1 Môi trường

| Workspace | Nhánh | Thư mục Git | Trạng thái |
|---|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` | ✅ |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` | ✅ |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` | chưa nối — Bước 9 |

Cài đặt Spark mỗi ws Platform: Runtime **2.0**, High concurrency **tắt cho notebook** (để `%%configure` chạy).

### 5.2 Git
- Notebook: Claude viết trong repo → push `dev` → bạn **đóng tab → Update all**.
- Pipeline: bạn dựng trên UI (chứa ID connection) → **Ctrl+S mỗi activity** → Commit → Claude review JSON.
- Commit: `feat(slv): …`, `fix(ingest): …`, `docs: …`. Tag mốc `v0.1`, `v1.0`, `v1.1`.
- PR `dev → main` chỉ khi đạt P14.

### 5.3 Vòng làm việc mỗi bước

```
① Thiết kế   Claude: 1 mục ngắn trong guide (mục tiêu · thiết kế · kết quả mong đợi)
② Duyệt      Bạn đọc, hỏi, chốt                                  ← không code trước khi chốt
③ Làm        Claude: notebook/SQL · Bạn: pipeline/UI theo guide click-by-click
④ Kiểm       Chạy theo ca kiểm thử của bước → chụp kết quả
⑤ Ghi        Cập nhật guide (lỗi đã gặp) + trạng thái §14 → Commit
```

---

## 6. Conventions

### 6.1 Đặt tên

| Loại | Mẫu | Ví dụ |
|---|---|---|
| Bảng | `brz_<source>_<entity>` · `slv_<entity>` · `gld_d_/gld_f_/gld_a_` | `gld.gld_f_sales_line` |
| Notebook | `nb_<layer>_<entity>` · `nb_setup_*` · `nb_ops_*` · `nb_run_layer` · `nb_common` · `nb_dq` | `nb_slv_customer` |
| Pipeline / activity | `pl_<scope>` · `set_/lkp_/fe_/sw_/cp_/nb_/fail_` | `pl_ingest`, `cp_db_to_landing` |
| SQL function | `clean_*` · `parse_*` · `to_*` · `std_<miền>` · `is_*` | `std_order_status` |
| Cột | snake_case · `*_sk` surrogate · `*_code` natural · `*_key` ngày `YYYYMMDD` · `is_*` · `*_at` UTC · `*_date` ngày nghiệp vụ | |

### 6.2 Cột kỹ thuật

| Cột | brz | slv | gld | Ý nghĩa |
|---|---|---|---|---|
| `_source_system` | ✓ | ✓ | ✓ | `wholesale` / `retail` / `reference` |
| `_source_file` | ✓ | | | file landing chứa dòng |
| `_load_date` | ✓ | ✓ | ✓ | ngày logic (virtual clock) |
| `_batch_id` | ✓ | ✓ | | `<source>_<entity>_<yyyymmdd>` |
| `_run_id` | ✓ | ✓ | ✓ | RunId pipeline |
| `_ingested_at` | ✓ | | | lúc ghi Bronze |
| `_record_hash` | | ✓ | ✓ | hash cột nghiệp vụ → chỉ update khi đổi |
| `_inserted_at` / `_updated_at` | | ✓ | ✓ | |
| `_valid_from` / `_valid_to` / `_is_current` | | | SCD2 | |
| `dq_flags` | | ✓ | | mảng cờ cảnh báo (dòng vẫn dùng được) |

### 6.3 Thời gian & khoá
- Timestamp lưu **UTC**. File không có múi giờ → `cfg_source_entity.source_timezone`.
- Ngày nghiệp vụ (`order_date`, `date_key`) theo **Asia/Ho_Chi_Minh**. `p_load_date` = ngày giả lập.
- Surrogate key **tất định**: `xxhash64(natural_key[, _valid_from])`. Unknown member `-1`.

---

## 7. Schema `meta`

Chỉ 6 bảng, tạo đúng lúc cần (tất cả DDL ở `nb_setup_ddl`):

| Bảng | Loại | Ai ghi · ai đọc | Vì sao phải là bảng | Tạo ở |
|---|---|---|---|---|
| `cfg_source_entity` | config | `nb_setup_config` · **pipeline Lookup**, `nb_brz_load` | Pipeline (không phải Spark) cần đọc | ✅ Bước 0 |
| `state_watermark` | runtime | `nb_brz_load` · pipeline Lookup | Bộ nhớ giữa các lần chạy | ✅ (thêm `load_date` ở Bước 1) |
| `log_pipeline_run` | log | pipeline · `rpt_pipeline_health` | Report đọc | Bước 3 |
| `log_task_run` | log | runner · report | Report đọc; `delta_version` cho RESTORE | Bước 3 |
| `dq_result_log` | log | `nb_dq` · report | Report đọc | Bước 3 |
| `recon_result` | log | `nb_ops_recon` · report | Report đọc | Bước 6 |

**Bỏ:** `cfg_pipeline_step` (→ DAG trong `nb_run_layer`), `cfg_dq_rule` (→ `nb_dq`), `ref_order_status` + `ref_value_mapping` (→ function `nb_common`), `ref_holiday_vn` (→ `nb_gld_date`), `state_file_manifest` (A7), `schema_registry` (→ Q16 dùng Delta history + `log_task_run`; thêm lại ở drill nếu cần).

`cfg_source_entity` (9 dòng):

| source_system | entity | source_type | source_object | load_type | watermark |
|---|---|---|---|---|---|
| reference | categories | file | `inbound/reference/categories` | full_snapshot | file LastModified |
| wholesale | customers / products / sales_hierarchy | db | `dbo.<entity>` | **full_snapshot** (A6) | — |
| retail | customers / products / sales_hierarchy | file | `inbound/retail/<entity>` | full_snapshot | file LastModified |
| wholesale | orders | db | `dbo.orders` | incremental | `updated_at`, lookback 1 ngày |
| retail | orders | file | `inbound/retail/orders` | incremental | file LastModified |

Log append-only: không UPDATE/DELETE; trạng thái hiện tại = dòng mới nhất.

---

## 8. Ingest & Bronze

### 8.1 `pl_ingest(p_load_date)`

```
set_run_start      v_run_start = utcNow()                    mốc trên chung
lkp_source_entity  cfg_source_entity ⋈ watermark của ngày gần nhất < p_load_date
fe_source_entity   ForEach 9 dòng, song song 4
  sw_source_type
    db   → cp_db_to_landing    full_snapshot: SELECT * FROM <obj>
                               incremental : … WHERE wm_col > wm − lookback AND wm_col <= run_start
                               → landing/…/<entity>.parquet
    file → cp_file_to_landing  Binary, wm ≤ LastModified < run_start → giữ nguyên tên file
    default → fail_unknown_source_type
nb_brz_load        (p_load_date, p_run_id = RunId, p_run_start)  → Bronze + watermark
```

### 8.2 `nb_brz_load` — 1 notebook cho mọi nguồn
Với mỗi dòng `cfg_source_entity` (theo `load_order`):
1. Thư mục `landing/<s>/<e>/load_date=<d>/batch=<RunId>/` không có → bỏ qua.
2. Đọc: parquet (db) / CSV header, **mọi cột string** (file). Ép mọi cột về string.
3. Thêm `_source_system, _source_file, _load_date, _batch_id, _run_id, _ingested_at`.
4. `DELETE FROM brz.<t> WHERE _batch_id = '<s>_<e>_<yyyymmdd>'` rồi append (`mergeSchema` → cột mới tự thêm, ghi vào `log_task_run` khi có — Q16).
5. Mọi entity xong → ghi watermark (§8.3).

Bronze giữ **mọi thứ**, kể cả dòng bẩn và mọi snapshot master (lịch sử cho SCD2).

### 8.3 Watermark

| | ERP orders (incremental) | File | ERP master (full) |
|---|---|---|---|
| Lấy | `updated_at > wm − 1 ngày AND <= run_start` | `wm ≤ LastModified < run_start` | toàn bảng |
| Watermark mới | `max(updated_at)` trong batch (0 dòng → giữ cũ) | `run_start` | không cần (ghi `run_start` cho đồng nhất) |

- Lưu **1 dòng / (nguồn, `load_date`)**; chạy ngày D đọc mốc của ngày gần nhất **trước** D → chạy lại D cùng cửa sổ (P6).
- Chỉ ghi khi Copy + Bronze thành công → lỗi thì lần sau lấy lại, không mất.
- Dòng ERP `updated_at` năm 2027 bị cận trên loại → không đẩy watermark; lộ ra ở recon.

---

## 9. Silver

### 9.1 Chuẩn hoá — 3 tầng, đều là code trong `nb_common`

| Tầng | Function | Ví dụ |
|---|---|---|
| Làm sạch chung | `clean_text`, `clean_code`, `norm_key`, `to_amount`, `parse_ts`, `parse_date` | `norm_key(' Deliverd. ')` → `DELIVERD` |
| Chuẩn hoá theo miền | `std_order_status`, `std_gender`, `std_position`, `std_tax_rate`, `std_brand`… (danh sách lấy từ EDA Bước 2) | `std_order_status('deliverd')` → `Delivered`; lạ → NULL |
| Luật nghiệp vụ | `is_sales_recognized(status)`, `status_sequence(status)` | `is_sales_recognized('Delivered')` → true |

Thêm biến thể mới = sửa **1 function** → Commit → chạy lại Silver từ Bronze. `levenshtein()` chỉ dùng ở EDA để **gợi ý** biến thể, không tự sửa.

### 9.2 Dòng có vấn đề

| Mức | Khi nào | Dòng đi đâu |
|---|---|---|
| **flag** | Dùng được nhưng đáng ngờ: outlier qty/price, orphan FK, ngày tương lai | Vào Silver, `dq_flags` ghi lý do; orphan → Gold `-1` |
| **quarantine** | Không dùng được: thiếu khoá, ngày/số không parse được, giá trị chuẩn hoá ra NULL | `slv.quarantine_<entity>`: dòng gốc + `dq_reason` + `_batch_id` |
| **stop** | Sai toàn cục: trùng grain fact, recon lệch | Pipeline dừng, cảnh báo |

### 9.3 Bảng

| Bảng | Grain | Cách làm |
|---|---|---|
| `slv_category` | `category_code` | Snapshot mới nhất; dedup `CAT005` theo `norm_key(tên)`; tách đường dẫn → `lvl1..lvl4` |
| `slv_product` | `product_code` | Gộp 2 nguồn; `std_brand`; wholesale ưu tiên |
| `slv_customer` | `customer_code` | Gộp 2 nguồn, survivorship **theo cột** (ADR 003): giá trị khác NULL mới nhất, hoà → wholesale; `source_flags` |
| `slv_sales_hierarchy` | `source_system, salesman_code` | `std_position`; tự trỏ chính mình → manager NULL |
| `slv_orders_history` | `order_no, product_code, order_status, updated_at` | Mọi dòng trạng thái, MERGE insert-only |
| `slv_orders_current` | `order_no, product_code` | **PySpark (Q10)**: bản `updated_at` mới nhất; chống dữ liệu đến trễ (chỉ update khi mới hơn) |
| `quarantine_<entity>` | — | dòng gốc + `dq_reason` |

Khung notebook Silver/Gold:
```
Cell 0  %%configure -f  (lh_platform)
Cell 1  markdown: Mục đích · Input · Output · Grain · Cách load
Cell 2  parameters: p_load_date, p_run_id, p_full_reload
Cell 3  %run nb_common
Cell 4+ %%sql: TEMP VIEW src (batch mới hoặc toàn bộ) → cleaned → INSERT quarantine → MERGE đích
```

---

## 10. Gold

| Bảng | Grain | Key | Load |
|---|---|---|---|
| `gld_d_date` | 1 ngày | `date_key` | **PySpark (Q9)**; ngày lễ VN khai báo trong notebook; năm tài chính từ tháng 7 |
| `gld_d_customer` | 1 khách | `xxhash64(customer_code)` | SCD1 MERGE |
| `gld_d_product` | 1 sản phẩm + category lvl1–4 | `xxhash64(product_code)` | SCD1 MERGE |
| `gld_d_salesman` | 1 **phiên bản** salesman | `xxhash64(source, code, _valid_from)` | **SCD2** từ snapshot Bronze + flatten salesman → team lead → manager → director (Q7, Q11) |
| `gld_f_sales_line` | 1 dòng sản phẩm của đơn có doanh thu | `order_no, product_code` | MERGE theo đơn thay đổi; lọc `is_sales_recognized`; salesman **point-in-time** theo `order_date` |
| `gld_a_sales_month` | tháng × product × customer × salesman | — | xoá-chèn các tháng thay đổi |

Unknown member `-1` ở mọi dim → fact không có FK NULL.

---

## 11. Điều phối, log, DQ, recon, cảnh báo

```
pl_master_daily(p_load_date)
  log START → pl_ingest → nb_run_layer('slv') → nb_run_layer('gld') → nb_ops_recon
  → refresh sm_sales → log SUCCESS + email/Teams tóm tắt
  (failure) → log FAILED + email/Teams cảnh báo
pl_backfill(p_from, p_to)   lặp ngày → pl_master_daily (tuần tự)
pl_sim_drive(p_days)        nb_00_sim_daily → pl_master_daily(exit value)
pl_maintenance              OPTIMIZE / VACUUM
```

| Thành phần | Logic ở đâu | Ghi gì |
|---|---|---|
| `nb_run_layer(layer)` | **DAG khai báo trong chính notebook** (dict: step → notebook, depends_on); chạy `runMultiple` (1 session) | `log_task_run` (RUNNING → SUCCESS/FAILED, rows, `delta_version`) |
| `nb_dq(layer)` | **Danh sách rule trong notebook** (rule_id, bảng, điều kiện dòng lỗi, severity) | `dq_result_log`; `critical` FAIL → dừng |
| `nb_ops_recon` | Danh sách phép đối chiếu trong notebook | `recon_result` |
| Cảnh báo | Activity Office 365 Outlook / Teams trong `pl_master_daily` | — |

Recon: Source → Bronze (count) · Bronze → Silver (`vào = ra + quarantine + dedup`) · Silver → Gold (tổng doanh thu/tháng) · Gold → report (DAX).

---

## 12. Semantic model & report

- `sm_sales`: **Direct Lake** trên `gld.*`; star schema; date table; ẩn SK & cột kỹ thuật.
- Measures: Sales, Qty, Net Sales, Sales LY, YoY %, MTD/YTD, Top N Customer (+ Discount sau Q16).
- `rpt_sales`: Overview · Product · Customer/Country · Sales hierarchy (point-in-time).
- `rpt_pipeline_health` trên `meta.*`: trạng thái run, thời gian, rows/ngày, DQ, recon.

---

## 13. Vận hành & hiệu năng

| Bảng | Partition / cluster |
|---|---|
| `brz_*_orders` | `_load_date` |
| `slv_orders_*` | `order_month`; MERGE có điều kiện partition |
| `gld_f_sales_line` | `order_month` + Z-order `customer_sk, product_sk`; V-Order |
| dim / master | không partition |

- Q12 đọc: pruning, Z-order, V-Order + OPTIMIZE, bảng aggregate. Ghi: chỉ phần thay đổi, MERGE có pruning, deletion vectors.
- Rollback: `RESTORE TABLE … VERSION AS OF <delta_version trong log_task_run>`.
- Debug (Q15): report → refresh → Gold history → Silver → Bronze → watermark → landing → Source.

---

## 14. Roadmap end-to-end

> Mỗi việc nhỏ đi đủ vòng §5.3. **Ai:** C = Claude (code/doc), B = Bạn (UI Fabric/chạy/kiểm).

### Bước 1 — Ingest ▶

| # | Việc | Ai | Kết quả / xong khi |
|---|---|---|---|
| 1.1 | Khung `pl_ingest` + nhánh db | B | ✅ landing/wholesale có 4 entity |
| 1.2 | Áp A4/A6/A7: `nb_setup_ddl` (thêm `load_date` vào `state_watermark`; bỏ bảng thừa), `nb_setup_config` (master ERP = full_snapshot; bỏ ref) | C | Update all → chạy 2 notebook → `meta` còn 2 bảng |
| 1.3 | Dọn Dev: chạy cell xoá bảng thừa + xoá `Files/landing` thử | B | `meta` = `cfg_source_entity`, `state_watermark` |
| 1.4 | Nhánh file `cp_file_to_landing` | B | landing/retail + reference có file |
| 1.5 | Sửa Copy db: rẽ `full_snapshot` / `incremental` trong câu query; sửa Lookup đọc watermark theo `load_date` | B (C đưa biểu thức) | Preview Lookup 9 dòng |
| 1.6 | Viết `nb_brz_load` | C | notebook trong repo |
| 1.7 | Nối Notebook activity `nb_brz_load` vào pipeline | B | |
| 1.8 | Kiểm thử T1–T4 (guide 01-ingest/06) | B+C | Chạy 2 lần không đổi; ngày sau chỉ tăng phần mới |

### Bước 2 — Khám phá dữ liệu (Q1) + chuẩn hoá

| # | Việc | Ai | Kết quả |
|---|---|---|---|
| 2.1 | `nb_ops_eda` trên `brz.*`: profile từng cột (null, distinct, top giá trị, định dạng ngày, số không parse được, orphan, trùng, outlier) + `levenshtein` gợi ý biến thể | C | notebook |
| 2.2 | Chạy, gửi kết quả | B | |
| 2.3 | `docs/dq_findings.md`: mỗi lỗi = query + số lượng + cách xử lý (flag/quarantine/stop) | C | **Q1** |
| 2.4 | `nb_common`: `norm_key`, `std_*`, `is_sales_recognized`, `status_sequence` theo đúng dữ liệu thật | C | test SELECT ra đúng |

### Bước 3 — Orders end-to-end (lát cắt dọc) → `v0.1`

| # | Việc | Ai |
|---|---|---|
| 3.1 | `nb_setup_ddl`: `log_pipeline_run`, `log_task_run`, `dq_result_log` | C |
| 3.2 | `nb_slv_orders_history` (%%sql) · `nb_slv_orders_current` (PySpark, Q10) · `quarantine_orders` | C |
| 3.3 | `nb_gld_date` (PySpark, Q9) · dim tối thiểu (chỉ unknown `-1`) · `nb_gld_sales_line` | C |
| 3.4 | `nb_run_layer` (DAG trong code, `runMultiple`, log) · `nb_dq` (rule orders) | C |
| 3.5 | `pl_master_daily` (ingest → slv → gld, log start/end) | B |
| 3.6 | `sm_sales` + 1 trang report doanh thu theo ngày | B (C hướng dẫn) |
| 3.7 | Chạy 7 ngày giả lập liên tục (sim daily → master daily) | B |

Xong khi: 7 ngày xanh, Gold doanh thu = SQL tay trên Silver. **Tag `v0.1`.**

### Bước 4 — Master (Silver + Gold SCD1)
`nb_slv_category/product/customer/sales_hierarchy` · `nb_gld_customer/product` · thêm vào DAG + rule DQ. Xong khi: fact có FK thật, orphan → `-1`, recon xanh.

### Bước 5 — SCD2 + Gold đủ (Q7, Q11)
`nb_gld_salesman` (SCD2 từ snapshot Bronze + flatten) · `nb_gld_sales_month`. Xong khi: không chồng lấp phiên bản; report team tháng 3 đúng cơ cấu tháng 3.

### Bước 6 — DQ + recon + cảnh báo đủ
Đủ rule §9.2/§11 · `recon_result` + `nb_ops_recon` · email/Teams. Xong khi: bơm lỗi vào → bị bắt đúng mức.

### Bước 7 — Vận hành
`pl_backfill`, `pl_sim_drive`, `pl_maintenance`, `p_full_reload`. Xong khi: rebuild từ Bronze == incremental; chạy lại bất kỳ ngày nào không đổi (S2).

### Bước 8 — Report
`rpt_sales` đủ trang · `rpt_pipeline_health`. Xong khi: report = SQL = recon (S4).

### Bước 9 — Prod → `v1.0`
PR `dev → main` · nối Prod (`main` / `fabric/platform`) · Update all · `nb_setup_ddl` + `nb_setup_config` · `pl_backfill` · lịch chạy.

### Bước 10 — Drill → `v1.1`
| Drill | Làm | Bằng chứng |
|---|---|---|
| Q14 late-arriving | chèn đơn `order_date` 02/01 vào ngày 15/01 | watermark bắt, tháng 1 cập nhật, recon khớp |
| Q15 mất tháng 5 | xoá tháng 5 ở Gold | runbook → RESTORE hoặc backfill |
| Q16 schema evolution | thêm `discount_amount` từ 01/06 | Bronze tự thêm cột → Silver/Gold → measure |
| Q11 SCD2 | đổi org chart giữa tháng 3 | report tháng 3 đúng cơ cấu cũ |
| Q12 scale | sinh dữ liệu lớn | số đo trước/sau tối ưu |

### Bước 11 — SQL của đề: `sql/answers/` Q5, Q6, Q7, Q17 (+ Part II)
### Bước 12 — Nộp bài: PPT, diagram, README

---

## 15. Câu hỏi đề → nơi trả lời

| Q | Nơi | Q | Nơi |
|---|---|---|---|
| Q1 | Bước 2, `dq_findings.md` | Q10 | `slv_orders_current` (PySpark) |
| Q2 | §4, §8–10 | Q11 | §10 SCD2, Bước 5 |
| Q3, Q4 | §10, ADR 001 | Q12 | §13, drill |
| Q5–Q7 | `sql/answers/` | Q13 | §7, §11 |
| Q8 | §9.2, §11 | Q14–Q16 | Bước 10 |
| Q9 | `gld_d_date` (PySpark) | Q17 | `sql/answers/q17` |

---

## 16. Rủi ro & ADR

| Rủi ro | Giảm thiểu |
|---|---|
| Capacity trial nhỏ (lỗi 430) | 1 session mỗi lúc; runner `runMultiple`; Stop session khi xong |
| SQL endpoint đồng bộ trễ → Lookup đọc watermark cũ khi chạy liên tiếp (backfill) | Idempotent nên chỉ lấy dư, không sai; nếu cần: refresh metadata endpoint đầu pipeline |
| `%%configure` lỗi trong high-concurrency session | Tắt HC cho notebook; kiểm lại khi chạy qua pipeline / `runMultiple` (Bước 1.7, 3.4) |
| Pipeline không tự lưu | Ctrl+S mỗi activity; Commit sớm |
| Dữ liệu thật ≠ mô tả đề | Ghi ở Q1, xử lý ở Silver |
| Runtime 2.0 bật ANSI | Luôn `try_cast`, `try_to_timestamp` với dữ liệu bẩn |

| ADR | Chủ đề |
|---|---|
| 001 | Gold model: grain, key, cách load |
| 003 | Survivorship customer giữa 2 nguồn |
| 008 | ERP giả lập "hybrid"; `categories` là file `reference` |
| 009 | Virtual clock cho simulator |
| 010 | 1 lakehouse `lh_platform` + schema theo layer |
| 011 | SQL-first; bỏ framework Python (bản lưu: nhánh `archive/python-framework`) |
| **012** | **Ingest bằng pipeline Copy** (không đọc nguồn bằng notebook) |
| **013** | **Luật nghiệp vụ & chuẩn hoá bằng SQL function, không dùng bảng ref** (A1–A3) |
| **014** | **Watermark:** ERP theo dữ liệu, file theo `run_start`, 1 dòng/`load_date`; master ERP full snapshot (A5–A7) |
