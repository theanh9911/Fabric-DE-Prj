# Plan — Fabric DE Project (Company A Sales)

> Mô phỏng 1 dự án Data Engineering **thật** trên Microsoft Fabric (đề *Data Engineer Case Study*) để luyện các năng lực production, rồi map 1-1 sang dự án thật.
> Trạng thái: **Source ✅ · Bước 0 ✅ · Bước 1 Ingest ▶ (nhánh db đã chạy)** · Plan v4 · 2026-10-06
>
> | Tài liệu | Dùng để |
> |---|---|
> | **PLAN.md** (file này) | Mục tiêu, nguyên tắc, kiến trúc, đánh giá hiện trạng, roadmap |
> | [design/](design/README.md) | Thiết kế chi tiết từng phần — **nguồn sự thật về thiết kế** |
> | [guide/](guide/README.md) | Thao tác click-by-click, lỗi đã gặp |

---

## 1. Mục tiêu

Tập đúng các năng lực cần trong production — **không** phải dùng nhiều tính năng Fabric:

| Năng lực | Ở đâu |
|---|---|
| Tách **source system** khỏi **data platform**; không ghi ngược vào nguồn | [design/01](design/01-source-contract.md), [02](design/02-simulator.md) |
| Ingest chạy lại được, nhận ra dữ liệu mới/đổi, lưu trạng thái, truy vết từng lượt | [design/03](design/03-ingest.md), [04](design/04-control-tables.md) |
| Quy tắc rõ cho đến trễ, cập nhật, xoá, schema change, dữ liệu lỗi | [design/03](design/03-ingest.md), [05](design/05-bronze.md), [06](design/06-silver.md) |
| Tách logic nguồn, nghiệp vụ, mô hình báo cáo | [design/05](design/05-bronze.md)–[07](design/07-gold-semantic.md) |
| DQ, đối soát, log, cảnh báo, khôi phục | [design/08](design/08-dq-recon.md), [09](design/09-orchestration-ops.md) |
| Code/config qua Git, quy trình promote | [design/10](design/10-environments-cicd.md) |

Và trả lời đề Part I (Q1–Q17) bằng bằng chứng chạy thật (§6).

**Tinh thần:** giữ chuẩn production, nhưng mô phỏng **cơ chế và failure modes quan trọng** thay vì dựng một enterprise platform thu nhỏ.

### Tiêu chí thành công

| # | Tiêu chí | Đo bằng |
|---|---|---|
| S1 | Chạy tự động ≥ 7 ngày giả lập liên tục | `meta.pipeline_run` |
| S2 | **Rerun** (cùng input + cùng code) bất kỳ ngày nào → kết quả không đổi | chạy 2 lần, so count + amount |
| S3 | Thêm 1 nguồn mới → chỉ thêm 1 dòng `cfg_source_entity` (tới Bronze) | PR chỉ đụng `nb_setup_config` |
| S4 | Report = SQL = recon | `meta.reconciliation_result` |
| S5 | Mỗi failure drill có runbook + bằng chứng khôi phục | `docs/runbook.md` |

---

## 2. Đánh giá hiện trạng

### 2.1 Đã có

| Hạng mục | |
|---|---|
| Source: ERP + drop zone + simulator (virtual clock, đang ở 2026-01-01) | ✅ |
| Dev ws + Git (`dev` / `fabric/platform`), Runtime 2.0 | ✅ |
| `lh_platform` (`brz/slv/gld/meta`) · `nb_setup_ddl` · `nb_setup_config` · `nb_common` | ✅ (sẽ sửa theo v4) |
| `pl_ingest`: khung + nhánh db ghi landing | ✅ |
| Spike K1–K4 (SQL function, `%%configure` theo tên, định dạng Git, Lookup T-SQL) | ✅ |

### 2.2 Điều chỉnh (v3 → v4, sau phản biện)

| # | Trước | Sau | Chi tiết |
|---|---|---|---|
| D1 | Luật chuẩn hoá ở bảng `ref_*` + SQL | SQL function trong `nb_common`; bảng chỉ khi người vận hành cần sửa không qua release (hiện không có) | design/06 §2–3 |
| D2 | Config chỉ code đọc vẫn để bảng | Code ở đúng nơi dùng (DAG trong runner, rule trong `nb_dq`) | design/09, 08 |
| D3 | 13 bảng meta tạo sẵn | **7 bảng** có mục đích riêng, tạo đúng bước: `cfg_source_entity`, `watermark_state`, `ingestion_batch`, `pipeline_run`, `task_run`, `dq_result`, `reconciliation_result` | design/04 |
| D4 | Bỏ hẳn manifest file | **`ingestion_batch`** ghi cửa sổ nguồn / từng file (path, size, modified); checksum giai đoạn sau | design/03 §5, 04 |
| D5 | Watermark 1 dòng / `load_date` | `watermark_state` = trạng thái hiện hành; lịch sử + cửa sổ ở `ingestion_batch`; `load_date` **không** thay watermark | design/03 §2 |
| D6 | "Chạy lại ra cùng kết quả" tuyệt đối | Phân biệt **normal / rerun / reprocess** | design/03 §3 |
| D7 | Master ERP incremental | `db_full_snapshot` mỗi ngày; ba chiến lược ingest theo loại nguồn | design/03 §1 |
| D8 | Bronze mọi cột string | **Raw theo khả năng nguồn:** ERP giữ kiểu, CSV string; drift ghi nhận ở Bronze, không tự lan xuống | design/05 |
| D9 | File: chỉ lọc LastModified | + biên an toàn `settle_minutes`, nhận diện file gửi lại | design/03 §2, §5 |
| D10 | Simulator luôn "suôn sẻ" | Thêm scenario lỗi: late arrival, resend, duplicate, missing, partial, bad rows, schema change, org change, delete | design/02 |
| D11 | Chốt partition/Z-order/V-Order trước | **Đo rồi mới tối ưu**; Liquid Clustering là ứng viên | design/09 §5 |
| D12 | RESTORE luôn được | Phụ thuộc retention; VACUUM có kiểm | design/09 §4 |
| D13 | Nguyên tắc tuyệt đối | Quyết định có điều kiện (§3) | |

---

## 3. Nguyên tắc

| # | Nguyên tắc |
|---|---|
| P1 | **Git là nguồn sự thật** — workspace dựng lại được từ repo |
| P2 | **Source chỉ đọc**; simulator không biết Platform; Platform không ghi ngược |
| P3 | **Generic ở điều phối & logging; chiến lược ingest theo loại nguồn** |
| P4 | **Logic ở code, config ở bảng khi runtime/pipeline cần đọc** — mỗi logic một chỗ |
| P5 | **Raw bất biến theo khả năng nguồn** (landing + Bronze, append-only) → luôn replay được |
| P6 | **Incremental mặc định, full reload luôn có**; watermark chỉ tiến khi batch COMMITTED |
| P7 | **Rerun idempotent**; reprocess có chủ đích và được ghi nhận |
| P8 | **Không mất, không im lặng** — quarantine có lý do; log FAILED nhưng pipeline vẫn báo lỗi |
| P9 | **Đối soát count + amount** mọi bước, có ngưỡng |
| P10 | **Truy vết:** `run_id`, `batch_id`, `load_date` xuyên layer |
| P11 | **Không hardcode môi trường** — lakehouse theo tên; giá trị theo môi trường ở Variable Library |
| P12 | **Đo rồi mới tối ưu** |
| P13 | **Không code chết; tạo thứ gì khi bước cần** |
| P14 | **Kiểm trước khi promote** (DQ + recon + rerun xanh) |
| P15 | **Đơn giản trước**; quyết định lớn ghi ADR |

---

## 4. Kiến trúc

```
CompanyA-Source  (ranh giới hệ thống — chỉ đọc)
  ERP sqldb_erp_wholesale ── orders, customers, products, sales_hierarchy
  Drop zone lh_retail_drop ─ retail/* (CSV hằng ngày), reference/categories
  Simulator ─ chỉ thêm/sửa dữ liệu nguồn + scenario lỗi
        │  Pipeline Copy qua Connection
        ▼
CompanyA-DataPlatform-<Dev|Test|Prod>  (vòng đời triển khai)
  lh_platform
    Files/landing/…      raw, bất biến
    brz.*                Bronze: bằng chứng nguồn + metadata
    slv.*, quarantine_*  Silver: parse, chuẩn hoá, grain, dedup, hợp nhất, history
    gld.*                Gold: dimensions, facts, aggregates
    meta.*               config, watermark, batch, run/task log, DQ, recon
  pl_master_daily: ingest → silver → gold → recon → refresh → log/cảnh báo
  sm_sales (Direct Lake) → rpt_sales · rpt_pipeline_health
GitHub: notebook, pipeline, SQL, docs, config không bí mật
```

1 lakehouse nhiều schema là đủ để luyện luồng (ADR 010); tách lakehouse/workspace theo layer chỉ khi cần phân quyền/ownership/capacity riêng.

---

## 5. Roadmap end-to-end

> Vòng mỗi việc: **Thiết kế (design/) → Bạn duyệt → Làm → Kiểm → Ghi guide**. Ai: **C** = Claude, **B** = Bạn.

### Bước 1 — Ingest happy path ▶
| # | Việc | Ai | Xong khi |
|---|---|---|---|
| 1.1 | Khung `pl_ingest` + nhánh db | B | ✅ |
| 1.2 | `nb_setup_ddl` v4: tạo `watermark_state`, `ingestion_batch`, `pipeline_run`, `task_run`; cột `load_strategy`… cho `cfg_source_entity`; xoá bảng bỏ | C | notebook trong repo |
| 1.3 | `nb_setup_config` v4: 9 dòng với 3 chiến lược; bỏ `ref_*` | C | |
| 1.4 | Dọn Dev (xoá bảng bỏ, xoá landing thử) → chạy 1.2, 1.3 | B | `meta` đúng 5 bảng |
| 1.5 | Pipeline: Lookup "ingest plan" mới · Switch theo `load_strategy` · nhánh file · biểu thức Copy theo cửa sổ | B (C đưa biểu thức) | Preview Lookup đúng cửa sổ |
| 1.6 | `nb_brz_load` (Bronze + `ingestion_batch` + `task_run` + `watermark_state`) · `nb_ops_run_end` (`pipeline_run`) | C | |
| 1.7 | Nối notebook vào pipeline; spike K5 (metadata file nguồn), K6 (`%%configure` qua pipeline) | B+C | |
| 1.8 | Kiểm: normal → rerun (không đổi) → sim +1 ngày → normal (chỉ tăng phần mới) | B+C | 4 ca xanh |

### Bước 2 — Source contract + EDA (Q1)
`nb_ops_eda` trên Bronze → chốt các mục "Còn mở" của design/01 → `docs/dq_findings.md` (Q1) → `nb_common`: `norm_key`, `std_*`, `is_sales_recognized`, `status_sequence`.

### Bước 3 — Orders end-to-end (lát cắt dọc) → `v0.1`
`slv_orders_history` · `slv_orders_current` (PySpark, Q10) · quarantine · `gld_d_date` (PySpark, Q9) · dim tối thiểu · `gld_f_sales_line` · `nb_run_layer` · `nb_dq` (rule orders) · `dq_result` · recon cơ bản · `pl_master_daily` · 1 trang report · chạy 7 ngày giả lập.

### Bước 4 — Master data
customer / product / category / hierarchy: survivorship, phát hiện xoá từ snapshot, Gold SCD1, conformed dimension.

### Bước 5 — SCD2 + Gold đủ (Q7, Q11)
`gld_d_salesman` SCD2 từ snapshot Bronze, effective date chốt rõ, join point-in-time · `gld_a_sales_month`.

### Bước 6 — Simulator scenario + DQ/recon/cảnh báo đủ
Scenario `bad_rows`, `missing_file`, `file_resend`, `duplicate_file`, `partial_file` → chứng minh bắt đúng mức · `reconciliation_result` đủ (count + amount, ngày/tháng, ngưỡng) · email/Teams.

### Bước 7 — Vận hành
`pl_backfill` · `pl_sim_drive` · `pl_maintenance` (retention) · `p_mode` rerun/reprocess · `p_full_reload` · runbook khôi phục.

### Bước 8 — Report
`sm_sales` · `rpt_sales` · `rpt_pipeline_health` → S4.

### Bước 9 — Promote → `v1.0`
(Test) → Prod: PR, Update all, setup, Variable Library, backfill, kiểm sau deploy, lịch chạy.

### Bước 10 — Failure drills → `v1.1`
`late_arrival` (Q14) · mất dữ liệu tháng 5 + khôi phục (Q15) · `schema_change` (Q16) · `org_change` (Q11) · đo hiệu năng trước/sau (Q12).

### Bước 11 — SQL đề `sql/answers/` (Q5, Q6, Q7, Q17 + Part II) · Bước 12 — Nộp bài (PPT, diagram, README)

---

## 6. Câu hỏi đề → nơi trả lời

| Q | Nơi | Q | Nơi |
|---|---|---|---|
| Q1 | Bước 2, `dq_findings.md` | Q10 | `slv_orders_current` (PySpark) |
| Q2 | §4, design/03–07 | Q11 | design/07 §3, Bước 5 |
| Q3, Q4 | design/07, ADR 001 | Q12 | design/09 §5, Bước 10 |
| Q5–Q7 | `sql/answers/` | Q13 | design/04, 08 |
| Q8 | design/08 | Q14–Q16 | design/02, Bước 10 |
| Q9 | `gld_d_date` (PySpark) | Q17 | `sql/answers/q17` |

---

## 7. Rủi ro & spike

| Rủi ro | Giảm thiểu |
|---|---|
| Capacity trial nhỏ (lỗi 430) | 1 phiên Spark mỗi lúc; `runMultiple`; `pipeline_run` ghi 1 lần cuối lượt |
| SQL endpoint đồng bộ trễ → Lookup đọc watermark cũ khi chạy liên tiếp | Rerun/idempotent nên chỉ lấy dư; nếu đo thấy vấn đề → control tables sang Fabric SQL Database (design/04) |
| `%%configure` lỗi trong high-concurrency session | Tắt HC cho notebook; spike K6 |
| Không lấy được metadata file nguồn | Spike K5 (Copy session log / Get Metadata) |
| Pipeline không tự lưu | Ctrl+S mỗi activity; Commit sớm |
| Runtime 2.0 bật ANSI | `try_cast`, `try_to_timestamp` |

## 8. ADR

| ADR | Chủ đề |
|---|---|
| 001 | Gold: grain, key, cách load (design/07) |
| 003 | Survivorship customer giữa 2 nguồn (design/06 §4) |
| 008 | ERP giả lập "hybrid"; `categories` là file `reference` |
| 009 | Virtual clock cho simulator |
| 010 | 1 lakehouse `lh_platform` + schema theo layer |
| 011 | SQL-first; bỏ framework Python (nhánh `archive/python-framework`) |
| 012 | Ingest bằng pipeline Copy; ba chiến lược theo loại nguồn (design/03) |
| 013 | Logic ở code, config ở bảng khi runtime cần (design/06 §2) |
| 014 | Control tables: watermark hiện hành + ingestion_batch lịch sử; normal/rerun/reprocess (design/03–04) |
