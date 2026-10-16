# 06 — Backlog

Làm theo vòng lặp trong [CLAUDE.md](../CLAUDE.md#4-vòng-lặp-một-task). Truy vấn kiểm chứng `Qxx` ở [07 §4](07-van-hanh-kiem-thu.md#4-thư-viện-sql-kiểm-chứng).

Cách dựng: dựng dọc từng lớp. Nền tảng và pipeline khung có trước; mỗi task Silver/Gold sau đó bật object của mình (`is_active = True`), thêm activity vào `pl_master_daily`, thêm check và đối soát của mình. Nhờ vậy sau mỗi task, `pl_master_daily` chạy được từ đầu tới cuối và kiểm chứng được trên dữ liệu thật.

Làm lại từ đầu, không dựa vào dữ liệu hay code v1 (D38): không có baseline v1, EDA đọc trực tiếp nguồn. Nguồn (`CompanyA-Source`) coi như đã sẵn sàng: đã reset và đã chạy `nb_00_sim_daily` initial load theo đúng layout contract.

## Phụ thuộc và thứ tự chạy

Cột "Cần sẵn" là điều kiện phải có **trước khi** bắt đầu task; cột bảng trạng thái bên dưới là phụ thuộc giữa các task.

| Pha | Cần sẵn trên Fabric | Làm được local (không cần Fabric) |
|---|---|---|
| P0 | Source có dữ liệu (initial load xong); workspace Platform nối Git, `lh_platform`; quyền đọc workspace Source | T0.4 |
| P1 | — | T1.1; T1.2 cần kết quả EDA (T0.3) |
| P2 | `lh_platform` gắn default cho từng notebook; K01–K10 đã kiểm (T0.2) | viết code T2.1–T2.4 |
| P3 | Connection tới `sqldb_erp_wholesale` và `lh_retail_drop` (Copy activity); `nb_setup_ddl` đã chạy (T2.4) | viết `nb_plan`, `nb_land`, pipeline |
| P4–P5 | Landing có delivery (M1) | viết Silver, Gold |
| P6 | kênh cảnh báo (Teams hoặc Outlook, theo K07) | |
| P7 | workspace Source | `nb_00_sim_inject` |
| P8 | M4 đạt | |

**Làm T0.2 (spike) trước tiên.** Thiết kế dựa trên các giả định runtime chưa kiểm chứng (CDF `table_changes`, `fs.mv`, `modifyTime`, `try_to_timestamp`, Copy activity với ID động, nhánh lỗi của pipeline); sai giả định nào thì sửa tài liệu trước khi viết code.

## Mốc

Không bắt đầu pha kế tiếp khi mốc trước chưa được chứng minh chạy trên Dev (W10).

| Mốc | Đạt khi | Sau task |
|---|---|---|
| M0 | giả định runtime đã kiểm; EDA xong; contract `active` | T0.2, T0.3, T1.2 |
| M1 | `pl_master_daily` chạy trên Dev: Copy + `nb_land` nạp Landing cả 9 source; check của contract và RC-01 ghi kết quả | T3.4 |
| M2 | Silver orders và master nạp; đối soát Landing ↔ Silver PASS | T4.2 |
| M3 | Gold đủ; đối soát Silver ↔ Gold PASS; full và incremental cho cùng số | T5.5 |
| M4 | cảnh báo, SLA, bảo trì, thao tác park chạy được | T6.4 |
| M5 | diễn tập đủ sự cố; 7 ngày liên tục xanh | T8.3 |

## Bảng trạng thái

| Task | Tên | Trạng thái | Phụ thuộc |
|---|---|---|---|
| T0.1 | Baseline số liệu v1 | BỎ (D38) | — |
| T0.2 | Spike runtime K01–K12 | IN_PROGRESS (còn K11c; K06 bỏ, K07 hoãn tới T6.1) | — |
| T0.3 | EDA nguồn | DONE | — |
| T0.4 | `pyproject.toml` + `tools/check_repo.py` | REVIEW | — |
| T1.1 | `tools/build_contracts.py` + `nb_contracts` | REVIEW | T0.4 |
| T1.2 | Hoàn thiện contract và từ điển theo EDA | REVIEW | T0.3, T1.1 |
| T2.1 | `nb_config` | DONE | T1.2, T0.2 |
| T2.2 | `nb_std` | DONE | T0.2 |
| T2.3 | `nb_common` | DONE | T2.1, T2.2 |
| T2.4 | `nb_setup_ddl` | DONE | T2.1 |
| T2.5 | `nb_test_unit` | DONE | T2.3 |
| T2.6 | Reset Dev | DONE | T2.4 |
| T3.1 | `nb_plan` | DONE | T2.5, T2.6 |
| T3.2 | `nb_land` | DONE | T3.1 |
| T3.3 | `pl_master_daily` (khung: plan, ingest, close) + Connection | DONE | T3.2, T0.2 |
| T3.4 | `nb_dq`, `nb_recon` (khung, contract check, RC-01) | DONE | T3.3 |
| T4.1 | `nb_slv_orders` | REVIEW (full-load OK trên Dev, RC-02/03 PASS; chờ incremental) | T3.4 |
| T4.2 | `nb_slv_master` | REVIEW (full-load OK, RC-04 PASS; chờ kịch bản 3 ngày) | T3.4 |
| T5.1 | `nb_gld_dim_date` | REVIEW | T3.4 |
| T5.2 | `nb_gld_dim_customer` | REVIEW | T4.1, T4.2 |
| T5.3 | `nb_gld_dim_product` | REVIEW | T4.1, T4.2 |
| T5.4 | `nb_gld_dim_salesman` | REVIEW | T4.1, T4.2 |
| T5.5 | `nb_gld_fct_order_line` | REVIEW (full-load OK, RC-05 PASS; chờ incremental) | T5.1–T5.4 |
| T6.1 | Cảnh báo lỗi + `pl_backfill` | REVIEW (cảnh báo Teams chờ K07; hiện dùng activity Fail) | T5.5, T0.2 |
| T6.2 | `nb_sla_watch` + `pl_sla_watch` | REVIEW | T6.1 |
| T6.3 | `nb_maintenance` + `pl_maintenance` | REVIEW | T5.5 |
| T6.4 | `nb_ops_delivery` | REVIEW | T4.1 |
| T7.1 | `nb_00_sim_inject` (workspace Source) | REVIEW | — |
| T8.1 | Chạy toàn bộ lịch sử, đối chiếu nguồn | TODO | T6.1 |
| T8.2 | Diễn tập sự cố + `nb_test_e2e` | IN_PROGRESS (`nb_test_e2e` đã viết; còn diễn tập) | T7.1, T8.1 |
| T8.3 | 7 ngày giả lập liên tục | TODO | T8.2 |
| T8.4 | Dọn item tạm | TODO | T8.3 |
| T8.5 | Promote Prod | TODO | T8.4 |

Trạng thái: `TODO`, `IN_PROGRESS`, `REVIEW` (code + docs xong, chờ kiểm chứng Fabric), `DONE`, `BLOCKED` (ghi chờ gì).

---

## P0 — Chuẩn bị

### T0.1 — Baseline số liệu v1 — BỎ
Quyết định D38: làm lại từ đầu, không so với v1. Số liệu v2 được đối chiếu với chính nguồn ở T8.1.

### T0.2 — Spike runtime K01–K12
- **Mục đích:** xác nhận giả định runtime trước khi viết code dựa trên chúng.
- **File:** `fabric/platform/nb_spike.Notebook/` và một pipeline thử `pl_spike` (xoá ở T8.4); kết quả vào [DECISIONS.md §3](DECISIONS.md#3-kết-quả-spike).
- **Việc làm:** mỗi spike một cell hoặc một activity thử; dùng schema `tst` cho bảng (tạo ở đầu, `DROP SCHEMA tst CASCADE` ở cuối).

| Spike | Câu hỏi | Nếu không đạt |
|---|---|---|
| K01 | `table_changes('t', a, b)` chạy; MERGE sinh `update_preimage`/`update_postimage`, DELETE sinh `delete`; overwrite sinh `delete` + `insert` cho mọi dòng | `spark.read.option("readChangeFeed","true").option("startingVersion",a).option("endingVersion",b).table(t)` |
| K02 | `MERGE … WHEN NOT MATCHED BY SOURCE THEN DELETE` | `sync_table` = MERGE upsert + `DELETE FROM t WHERE NOT EXISTS (…)` |
| K03 | `notebookutils.fs.ls` trên abfss của workspace nguồn trả `size`, `modifyTime` | activity Get Metadata trong pipeline |
| K04 | `notebookutils.fs.mv` một thư mục staging sang Landing (cùng Lakehouse) | copy rồi xoá staging |
| K05 | `spark.version` ≥ 3.5; có `try_to_timestamp`; giá trị `spark.sql.ansi.enabled`; `sum(bigint)` tràn ⇒ lỗi hay quay vòng | `try_to_timestamp` qua `F.expr` |
| K06 | `"concurrency": 1` trong `pipeline-content.json` được nhận khi Git sync và chặn lượt thứ hai | chấp nhận idempotent ([04 §10.5](04-kien-truc.md#105-chạy-chồng)); thêm khoá nếu cần |
| K07 | activity Teams có trong Data pipeline; connection cần gì | activity Office 365 Outlook |
| K08 | `@json(activity('plan').output.result.exitValue).sources` dùng làm `items` của ForEach; kích thước exit value tối đa | ghi kế hoạch ra `Files` rồi đọc bằng Lookup |
| K09 | `DESCRIBE HISTORY` có `numTargetRowsInserted/Updated/Deleted` (MERGE) và `numOutputRows` (WRITE) | đếm bằng CDF của commit đó |
| K10 | bảng DDL có `NOT NULL`; `overwrite` + `overwriteSchema=false` giữ ràng buộc; ghi NULL vào cột NOT NULL báo lỗi rõ | kiểm NOT NULL trong `align_to_target` |
| K11 | Copy activity: (a) SQL Database ⇒ Parquet vào `Files/_staging/<id>` của `lh_platform`; (b) Lakehouse khác workspace ⇒ Binary, wildcard + lọc last modified; (c) workspace/item ID lấy động từ expression; (d) output có `rowsRead`; (e) kết quả 0 dòng có tạo thư mục đích không | điều chỉnh [04 §3](04-kien-truc.md#3-landing) theo hành vi thật |
| K12 | nhánh lỗi: activity đặt `dependsOn` activity cuối với điều kiện `Failed` + `Skipped` bắt được lỗi của bất kỳ activity nào; lấy được thông điệp lỗi | gắn nhánh lỗi riêng cho từng activity chính |

- **Xong khi:** DECISIONS §3 có kết quả 12 spike, phiên bản runtime; tài liệu 03, 04, 05 đã sửa ở chỗ giả định sai.

### T0.3 — EDA nguồn
- **Mục đích:** chốt các giá trị phụ thuộc dữ liệu: tên cột file, định dạng thời gian, múi giờ ERP, biến thể trạng thái/thuế/gender/position/brand.
- **File:** `docs/EDA.md`, `fabric/platform/nb_eda.Notebook/` (xoá ở T8.4).
- **Việc làm:** làm đủ checklist [EDA.md](EDA.md) trực tiếp trên nguồn (CSV ở drop zone, bảng ERP), chỉ đọc. Cần nguồn đã có dữ liệu.
- **Xong khi:** mọi mục E01–E20 có kết quả và kết luận; mục "Điền vào" của từng mục đã rõ giá trị.

### T0.4 — `pyproject.toml` + `tools/check_repo.py`
- **File:** `pyproject.toml`, `tools/check_repo.py`.
- **Việc làm:** theo [05 §6.2](05-thu-vien.md#62-toolscheck_repopy). C10 bỏ qua khi `tools/build_contracts.py` chưa có; C11, C12 bỏ qua khi `nb_setup_ddl`, `nb_config` chưa phải bản v2 (bật ở T2.4).
- **Xong khi:** `uv run tools/check_repo.py` chạy trên repo; cố ý thêm `%run` chung cell vào một file tạm ⇒ C02 báo lỗi.
---

## P1 — Contract

### T1.1 — `tools/build_contracts.py` + `nb_contracts`
- **File:** `tools/build_contracts.py`, `fabric/platform/nb_contracts.Notebook/` (sinh).
- **Việc làm:** theo [03 §6](03-contract.md#6-toolsbuild_contractspy). Bật C10 trong `check_repo.py`.
- **Xong khi:** chạy trên 9 contract hiện có ⇒ sinh `nb_contracts`; `--check` sau đó PASS; sửa một giá trị YAML mà không sinh lại ⇒ `--check` FAIL; xoá `watermarkColumn` của `wholesale.orders` ⇒ BC04 báo lỗi.

### T1.2 — Hoàn thiện contract và từ điển theo EDA
- **File:** `contracts/**/*.yaml`, `docs/DECISIONS.md`.
- **Việc làm:** điền kết quả EDA: tên cột (`properties[].name`), `timestampFormats`, `dateFormats`, `sourceTimezone`, giá trị trong `quality`; bump `version`; `status: active`. Ghi danh sách từ điển (dùng ở T2.1) vào DECISIONS mục tương ứng.
- **Xong khi:** `uv run tools/build_contracts.py --strict` PASS.

---

## P2 — Nền tảng

### T2.1 — `nb_config`
- **File:** `fabric/platform/nb_config.Notebook/notebook-content.py` (viết lại).
- **Việc làm:** theo [05 §2](05-thu-vien.md#2-nb_config): `SETTINGS`, từ điển, `SILVER` (5 phần tử, `is_active = False`), `GOLD` (5 phần tử, `is_active = False`), `CHECKS` (rỗng; từng task sau thêm check của mình), `validate_config()` V01–V10.
- **Xong khi:** chạy `nb_config` in `Validated 9 contracts, 5 silver, 5 gold, 0 checks`; cố ý tạo chu trình trong GOLD ⇒ V06 báo lỗi rõ.

### T2.2 — `nb_std`
- **File:** `fabric/platform/nb_std.Notebook/` (mới).
- **Việc làm:** mọi hàm [05 §3](05-thu-vien.md#3-nb_std): sinh biểu thức SQL và `conform_sql`.
- **Xong khi:** test của `nb_std` trong T2.5 PASS.

### T2.3 — `nb_common`
- **File:** `fabric/platform/nb_common.Notebook/notebook-content.py` (viết lại).
- **Việc làm:** mọi hàm [05 §4](05-thu-vien.md#4-nb_common).
- **Xong khi:** test của `nb_common` trong T2.5 PASS.

### T2.4 — `nb_setup_ddl`
- **File:** `fabric/platform/nb_setup_ddl.Notebook/notebook-content.py` (viết lại).
- **Việc làm:**
  1. Tham số và hành vi reset theo [04 §10.9](04-kien-truc.md#109-notebook-kiểm-tra-và-vận-hành); view xoá bằng `DROP VIEW`, bảng bằng `DROP TABLE`.
  2. Tạo schema và mọi bảng, view của [04 §4–§7](04-kien-truc.md#4-schema-meta), mỗi nhóm một cell `%%sql`, `CREATE … IF NOT EXISTS` / `CREATE OR REPLACE VIEW`.
- **Xong khi:** chạy hai lần liên tiếp không lỗi, lần hai không đổi gì; Q18 thấy đủ object, mọi bảng `slv`/`gld` có CDF; `check_repo.py` bật C11, C12 và PASS.

### T2.5 — `nb_test_unit`
- **File:** `fabric/platform/nb_test_unit.Notebook/` (mới).
- **Việc làm:** theo [07 §3.2](07-van-hanh-kiem-thu.md#32-nb_test_unit).
- **Xong khi:** mọi test PASS; cố ý làm sai `std_code` ⇒ test FAIL.

### T2.6 — Reset Dev
- **Việc làm:** theo [07 §1.2](07-van-hanh-kiem-thu.md#12-reset-dev), **chỉ phần platform** (bước 3, 4): nguồn đã sẵn sàng nên không reset. Workspace platform mới tinh thì chỉ cần Q18 xác nhận không còn object lạ.
- **Xong khi:** Q18 chỉ còn object v2 rỗng.

---

## P3 — Ingest và pipeline khung

### T3.1 — `nb_plan`
- **File:** `fabric/platform/nb_plan.Notebook/` (mới).
- **Việc làm:** [04 §3.2–3.4](04-kien-truc.md#32-nguồn-db), [04 §9.1](04-kien-truc.md#91-nb_plan-và-nb_land), preflight [04 §11.2](04-kien-truc.md#112-kiểm-tra-trước-khi-chạy). Hàm `resolve_source_ids`, `list_drop_zone`, `plan_ingest` trong `nb_common`.
- **Xong khi:**
  - chạy trực tiếp với tham số mặc định cho ngày simulator đầu ⇒ `sources` đủ 9 source (DB có `query`, file có `expected_files`), `ids` và `master_entities` đúng;
  - chạy ngay sau khi simulator ghi file ⇒ source file không vào kế hoạch, `run_log` có `settle_pending:<n>`;
  - `p_mode = rerun` ⇒ `sources = []`;
  - mỗi PF01–PF08 có một lần thử bị chặn đúng thông báo.

### T3.2 — `nb_land`
- **File:** `fabric/platform/nb_land.Notebook/` (mới).
- **Việc làm:** [04 §3.2–3.3](04-kien-truc.md#32-nguồn-db), [04 §3.5](04-kien-truc.md#35-checksum), [04 §3.7](04-kien-truc.md#37-manifest), [04 §9.1](04-kien-truc.md#91-nb_plan-và-nb_land). Hàm `promote_staging`, `write_manifest`, `record_delivery`, `record_landed_files` trong `nb_common`.
- **Xong khi:** chạy trực tiếp sau một Copy thủ công vào staging: DB ⇒ `DELIVERED`, Landing có Parquet + manifest đúng [04 §3.7](04-kien-truc.md#37-manifest), `rows_landed` khớp đếm lại (Q06); snapshot không đổi ⇒ `UNCHANGED`, staging bị xoá; staging có file không nằm trong `expected_files` ⇒ file bị loại.

### T3.3 — `pl_master_daily` (khung) + Connection
- **File:** `fabric/platform/pl_master_daily.DataPipeline/pipeline-content.json`, `fabric/platform/nb_close.Notebook/` (mới); **xoá** `nb_ingest_plan`, `nb_ingest`, `nb_slv_load`, `nb_gld_dims`, `nb_gld_fact`, `nb_ops_recon`, `nb_process`, `Notebook_1`, `pl_daily_core`.
- **Việc làm:**
  1. Tạo Connection cho SQL Database nguồn và Lakehouse drop zone ([07 §1.1](07-van-hanh-kiem-thu.md#11-dựng-workspace-platform-mới)).
  2. Pipeline theo [04 §10.1](04-kien-truc.md#101-pipeline-pl_master_daily): activity 1, 2, 12, 13 (các activity Silver, Gold, dq, recon được thêm ở các task sau); tham số [04 §11.1](04-kien-truc.md#111-tham-số-pl_master_daily); `concurrency: 1`, `retry: 0`.
- **Xong khi:**
  - chạy ngày simulator đầu ⇒ Landing có delivery + manifest cho cả 9 source; `delivery_log` có `LANDED`; Q03 khớp;
  - chạy lại ⇒ source file `NO_DATA`, snapshot DB `UNCHANGED`, DB incremental `NO_DATA`;
  - cố ý sai tên Connection ⇒ lượt FAILED, `close_failed` ghi `run_log` `pipeline FAILED`;
  - hai lượt cùng lúc (bắt đầu hai lượt cho cùng `p_load_date`) ⇒ số liệu cuối cùng không sai và không nhân đôi (ghi idempotent, D44).

### T3.4 — `nb_dq`, `nb_recon`
- **File:** `fabric/platform/nb_dq.Notebook/` (viết lại), `fabric/platform/nb_recon.Notebook/` (mới); `pl_master_daily` (activity `dq`, `recon`).
- **Việc làm:** tham số và kết quả theo [04 §10.9](04-kien-truc.md#109-notebook-kiểm-tra-và-vận-hành); `nb_dq`: chọn check của `CHECKS` theo phạm vi lượt, `run_check`, `run_contract_checks`; `nb_recon`: RC-01. Thêm vào `CHECKS`: DQ-ING-01.
- **Xong khi:** lượt ngày đầu có `check_result` cho contract check của mọi delivery và RC-01 của delivery DB (Q17).

---

## P4 — Silver

### T4.1 — `nb_slv_orders`
- **File:** `fabric/platform/nb_slv_orders.Notebook/` (mới); `nb_config` (bật `slv.orders_event`, thêm DQ-SLV-01, 02, 04, 05, 06, 07); `nb_recon` (RC-02, RC-03); `pl_master_daily` (activity `silver_orders`).
- **Việc làm:** [04 §5.2–5.3](04-kien-truc.md#52-slvorders_event), [04 §9.2](04-kien-truc.md#92-nb_slv_orders).
- **Xong khi:**
  - lượt `normal` ⇒ delivery orders `LOADED`; RC-02, RC-03 PASS;
  - chạy lại cùng ngày ⇒ node `SKIPPED up_to_date` hoặc `rows_inserted = 0`, `rows_updated = 0`;
  - dòng retail có `updated_at` dạng `M/d/yyyy H:mm` không bị quarantine;
  - quarantine đúng mã [04 §7.2](04-kien-truc.md#72-mã-lý-do) (Q07);
  - `rerun` + `p_full_reload` + `p_scope=slv.orders_event` hai lần ⇒ Q12 giống nhau;
  - delivery thiếu cột bắt buộc (sửa tay một bản sao file trong Dev) ⇒ delivery `FAILED`, delivery khác vẫn `LOADED`, activity `Failed`, Gold không chạy;
  - gửi lại file đúng cùng tên ⇒ delivery hỏng cũ tự hết chặn: lượt có file mới `SUCCEEDED`, không cần `park`.

### T4.2 — `nb_slv_master`
- **File:** `fabric/platform/nb_slv_master.Notebook/` (mới); `nb_config` (bật 4 bảng history, thêm DQ-SLV-03); `nb_recon` (RC-04); `pl_master_daily` (activity `for_each_master`).
- **Việc làm:** [04 §5.4–5.5](04-kien-truc.md#54-lịch-sử-master-slventity_history), [04 §9.3](04-kien-truc.md#93-nb_slv_master-p_entity).
- **Xong khi:** 3 ngày không đổi master ⇒ history chỉ có dòng ngày đầu (Q10); CAT005 ⇒ một dòng history + một FLAG `duplicate_key_in_snapshot`; RC-04 PASS; full rebuild hai lần ⇒ Q12 giống nhau.

---

## P5 — Gold

Mỗi task: viết notebook, bật object trong `GOLD`, thêm check của bảng vào `CHECKS`, thêm activity vào `pl_master_daily` theo [04 §10.1](04-kien-truc.md#101-pipeline-pl_master_daily).

### T5.1 — `nb_gld_dim_date`
- **File:** `fabric/platform/nb_gld_dim_date.Notebook/` (mới).
- **Việc làm:** [04 §6.1](04-kien-truc.md#61-glddim_date), [04 §9.4](04-kien-truc.md#94-nb_gld_dim_date). Check: DQ-GLD-01 (dim_date).
- **Xong khi:** số dòng = số ngày trong khoảng + 1; lượt sau `SKIPPED up_to_date`.

### T5.2 — `nb_gld_dim_customer`
- **File:** `fabric/platform/nb_gld_dim_customer.Notebook/` (mới).
- **Việc làm:** [04 §6.2](04-kien-truc.md#62-glddim_customer), [04 §9.5](04-kien-truc.md#95-nb_gld_dim_customer-nb_gld_dim_product). Check: DQ-GLD-01, 02, 08.
- **Xong khi:** mã có ở hai nguồn ⇒ một dòng, `source_systems = 'retail,wholesale'`; `CUS099` ⇒ `is_inferred`; lượt không đổi master ⇒ CDF của commit rỗng (Q13).

### T5.3 — `nb_gld_dim_product`
- **File:** `fabric/platform/nb_gld_dim_product.Notebook/` (mới).
- **Việc làm:** [04 §6.3](04-kien-truc.md#63-glddim_product), R-PRD-01, R-PRD-02. Check: DQ-GLD-01, 02, 08.
- **Xong khi:** `CAT010`, `CAT999` ⇒ `category_path = 'Unknown (pending)'`; `PRD999` inferred.

### T5.4 — `nb_gld_dim_salesman`
- **File:** `fabric/platform/nb_gld_dim_salesman.Notebook/` (mới).
- **Việc làm:** [04 §6.4](04-kien-truc.md#64-glddim_salesman-scd2), [04 §9.6](04-kien-truc.md#96-nb_gld_dim_salesman). Check: DQ-GLD-01, 03, 04, 08.
- **Xong khi:** mọi salesman có phiên bản đầu từ `1900-01-01`; `org_change` (T7.1 hoặc đổi tay ERP) ⇒ 2 phiên bản đúng ngày (Q14); lượt không đổi ⇒ CDF rỗng.

### T5.5 — `nb_gld_fct_order_line`
- **File:** `fabric/platform/nb_gld_fct_order_line.Notebook/` (mới); `nb_recon` (RC-05).
- **Việc làm:** [04 §6.5](04-kien-truc.md#65-gldfct_order_line-accumulating-snapshot), [04 §9.7](04-kien-truc.md#97-nb_gld_fct_order_line). Check: DQ-GLD-05, 06, 07.
- **Xong khi:**
  - full ở version V và incremental qua từng ngày tới V cho cùng Q15;
  - late arrival 2026-01-15 ⇒ chỉ dòng hàng bị chạm có `_run_id` mới (Q16);
  - đơn có `salesman_code` hợp lệ không có `salesman_sk = -1`;
  - `latest_event` cho cùng kết quả với `slv.v_orders_current` (cùng số dòng, cùng tổng băm `_record_hash`);
  - RC-05 PASS.

---

## P6 — Vận hành

### T6.1 — Cảnh báo lỗi + `pl_backfill`
- **File:** `pl_master_daily`, `pl_backfill` (`pipeline-content.json`).
- **Việc làm:** [04 §15](04-kien-truc.md#15-pipeline-lịch-cảnh-báo-bảo-trì): activity `alert` sau `close_failed`; `pl_backfill` gọi `pl_master_daily` theo từng ngày, `concurrency: 1`.
- **Xong khi:** Git sync và Validate không lỗi; lỗi cố ý ⇒ nhận cảnh báo; backfill 3 ngày ⇒ 3 lượt tuần tự xanh.

### T6.2 — `nb_sla_watch` + `pl_sla_watch`
- **File:** `nb_sla_watch` (viết lại), `pl_sla_watch`.
- **Việc làm:** DQ-OPS-01 theo `deliveryCadence`, `availableByLocalTime`; kết quả theo [04 §10.9](04-kien-truc.md#109-notebook-kiểm-tra-và-vận-hành); pipeline If Condition → cảnh báo.
- **Xong khi:** `missing_file` ⇒ cảnh báo nêu đúng source và owner.

### T6.3 — `nb_maintenance` + `pl_maintenance`
- **File:** `nb_maintenance` (viết lại), `pl_maintenance`.
- **Việc làm:** [04 §15](04-kien-truc.md#15-pipeline-lịch-cảnh-báo-bảo-trì) bảng `nb_maintenance`; ghi `run_log` (`layer = maint`).
- **Xong khi:** chạy bảo trì rồi lượt `normal` ⇒ node Gold `SKIPPED up_to_date` (OPTIMIZE không kích hoạt Gold).

### T6.4 — `nb_ops_delivery`
- **File:** `fabric/platform/nb_ops_delivery.Notebook/` (mới); `nb_config` (thêm DQ-OPS-03).
- **Việc làm:** [04 §10.7](04-kien-truc.md#107-thao-tác-của-người-vận-hành); ghi qua `record_delivery` và `log_run`. Không có khoá (D33); `p_operator` bắt buộc.
- **Xong khi:** `park` một delivery `FAILED` kèm lý do ⇒ lượt kế tiếp không còn bị chặn, DQ-OPS-03 WARN; `park` delivery `LOADED` ⇒ bị từ chối; `release` ⇒ delivery được xử lý lại ở lượt sau; `p_confirm` sai ⇒ không làm gì.

---

## P7 — Simulator

### T7.1 — `nb_00_sim_inject`
- **File:** `fabric/source/nb_00_sim_inject.Notebook/` (mới).
- **Việc làm:** theo [02 §3](02-nguon.md#3-tiêm-sự-cố-nguồn--nb_00_sim_inject-đặc-tả-cho-t71); dùng hàm có sẵn của `nb_00_sim_common`.
- **Xong khi:** mỗi kịch bản chạy được, ghi `sim_inject_log`; `p_confirm` sai ⇒ không làm gì.

---

## P8 — Đưa vào chạy

### T8.1 — Chạy toàn bộ lịch sử, đối chiếu nguồn
- **Việc làm:** simulator initial load + 14 ngày (`pl_backfill`); chạy Q20; đối chiếu độc lập với nguồn (số dòng và tổng `quantity * price` tính trực tiếp từ bảng ERP và file CSV); mọi chênh lệch phải giải thích được bằng quarantine hoặc rule. Ghi bảng giải thích vào WORKLOG.
- **Xong khi:** mọi check, đối soát PASS hoặc WARN có giải thích.

### T8.2 — Diễn tập sự cố + `nb_test_e2e`
- **File:** `fabric/platform/nb_test_e2e.Notebook/` (mới).
- **Việc làm:** chạy từng kịch bản [07 §5](07-van-hanh-kiem-thu.md#5-kịch-bản-sự-cố-nguồn); `nb_test_e2e` assert kết quả mong đợi, `test_current_matches_view` và RC-06.
- **Xong khi:** mọi kịch bản đạt; WORKLOG có bằng chứng.

### T8.3 — 7 ngày giả lập liên tục
- **Xong khi:** 7 lượt `normal` liên tiếp `SUCCEEDED`; Q21 cho thấy mỗi bảng chạy incremental; thời gian mỗi lượt, số lần khởi động Spark, số file Delta ghi vào WORKLOG (L12).

### T8.4 — Dọn item tạm
- **Việc làm:** xoá `nb_spike`, `pl_spike`, `nb_eda` (kết quả đã nằm trong DECISIONS/EDA.md).
- **Xong khi:** `uv run tools/check_repo.py` sạch; mọi item trong `fabric/platform/` có trong [07 §2](07-van-hanh-kiem-thu.md#2-danh-mục-item).

### T8.5 — Promote Prod
- **Việc làm:** theo [07 §1.3](07-van-hanh-kiem-thu.md#13-promote-prod).
- **Xong khi:** Prod chạy 3 ngày liên tiếp `SUCCEEDED`.
