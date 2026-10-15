# WORKLOG

Nhật ký append-only; mục mới ở cuối; không sửa mục cũ (thêm "Đính chính" bên dưới nếu cần). Ngày dạng `yyyy-MM-dd`.

## Mẫu

```markdown
## yyyy-MM-dd — T<x.y> <tên> — <IN_PROGRESS | REVIEW | DONE | BLOCKED>
Người/agent: <…>   Nhánh: feat/T<x.y>-<slug>   Commit: <sha>
Đã làm:
- …
File: …
Tài liệu đã sửa: …
Kiểm tra local: uv run tools/check_repo.py → <kết quả>
Kiểm chứng Fabric: <theo mẫu 07 §6.3, hoặc "chưa kiểm chứng">
Còn lại / câu hỏi: …
```

---

## 2026-10-14 — Viết lại bộ tài liệu — DONE
Người/agent: Claude (Opus 5.5)
Đã làm:
- Review bản v1 (`fabric/platform/*`), rút bài học vào [01 §8](01-nguyen-tac.md#8-bài-học-từ-v1).
- Thay toàn bộ tài liệu cũ bằng một bộ: `docs/README.md`, `01`–`07`, `DECISIONS.md`, `EDA.md`, `WORKLOG.md`; giữ `TROUBLESHOOTING.md`. Xoá `ARCHITECTURE.md`, `PLAN.md`, `DEVELOPER_GUIDE.md`, `GLOSSARY.md`, `SOURCE.md`, `SOURCE_SCHEMA.md`, `platform-implementation.md`, `docs/plan/`, `fabric/platform/{DEPENDENCIES,RUNBOOK,Readme}.md`.
- Viết 9 contract ODCS (`contracts/`), `status: draft` (D26).
- Viết lại `CLAUDE.md`, `README.md`.
Quyết định với người dùng: contract YAML + sinh code (D01); không đụng code v1, làm lại sau (D18); một bộ tài liệu duy nhất; lớp KPI để sau (D19); reset Dev được, Prod trống (D18); tiêm sự cố bằng notebook phía Source (D17); mức chuẩn: cơ bản, dễ bảo trì và mở rộng, không theo đủ đặc tả enterprise (D02).
Phát hiện khi đọc code: simulator chưa có chế độ sinh dữ liệu sau khi hết seed (khoảng 05/2026) — đủ cho diễn tập 7 ngày, không có task.
Kiểm chứng Fabric: không áp dụng (chỉ tài liệu).
Còn lại: bắt đầu T0.1–T0.4.

## 2026-10-14 — Rà soát và bổ sung tài liệu — DONE
Người/agent: Claude (Sonnet 5.5)
Đã làm:
- Thêm `00-tong-quan.md`.
- Sửa theo review: bỏ `first_returned_at_utc`; delivery hỏng chặn Gold và tự hết chặn khi nguồn gửi lại, `nb_ops_delivery` cho trường hợp không gửi lại được (D27, D32, D33); worker Gold dùng `latest_event` thay view (D28); Dev chờ settle (D29); `lookbackMinutes = 0` cho `wholesale.orders` (D30); ngày mơ hồ có DQ-SLV-07 (D31).
- Rà soát tham chiếu bằng script: mọi mã Q, D, E, R-, DQ-, RC-, PF, V, C, T, L, W, N đều có định nghĩa; đổi mã validator contract K → BC vì trùng mã spike K01–K10.
- Sửa lỗi thiết kế phát hiện khi rà soát: quyết định chạy từng node phải tính ngay trước khi chạy node đó (version Silver đổi ngay trong lượt) ⇒ `plan_node`; `plan_dry_run` chỉ ước lượng.
- Thêm 04 §10.8 (trình tự `nb_process`) và 04 §10.9 (notebook kiểm tra và vận hành); chuyển pseudocode từ 06 sang 04; thêm thuật ngữ vào 01 §7.
Kiểm chứng Fabric: không áp dụng.

## 2026-10-14 — Chốt điều phối bằng pipeline (P+) — DONE
Người/agent: Claude (Sonnet 5.5)
Đã làm:
- Thay notebook orchestrator `nb_process` bằng pipeline `pl_master_daily`: `nb_plan` → ForEach(Copy → `nb_land`) → notebook Silver/Gold nối bằng `dependsOn` → `nb_dq` → `nb_recon` → `nb_close` (D34).
- Ingest bằng Copy activity qua Connection; notebook không đọc nguồn (D35). `conform` sinh Spark SQL (D36).
- Bỏ `plan_node`/`plan_dry_run`/`build_nodes`/`topo_order`/`BLOCKED`/`run_lock`/`p_dry_run`; mỗi notebook tự `decide_run` bằng checkpoint.
- Viết lại 04 §1, §2, §3, §9, §10, §11, §15; 05 §2.1, §3, §4.10, §5, §6.2; 06 toàn bộ; sửa 00, 01, 03, 07, DECISIONS, CLAUDE.md, TROUBLESHOOTING.
- Thêm spike K11 (Copy) và K12 (nhánh lỗi).
Kiểm chứng Fabric: không áp dụng. Phiên sau: dọn lại toàn bộ và để agent dựng theo `docs/06-tasks.md`.
## 2026-10-15 — T0.4 + T1.1 (làm local, chưa tách nhánh) — REVIEW
Người/agent: Claude (Sonnet 5.5)   Nhánh: dev (theo yêu cầu: không đụng git)   Commit: chưa
Đã làm:
- T0.4: `pyproject.toml` (dependency `pyyaml`, `package = false`), `tools/check_repo.py` (C01–C12; C11, C12 chỉ bật khi `nb_setup_ddl`, `nb_config` là bản v2).
- T1.1: `tools/build_contracts.py` (BC01–BC10, `--check`, `--strict`), sinh `fabric/platform/nb_contracts.Notebook/` từ 9 contract.
- Xoá code v1 theo yêu cầu người dùng: `nb_ingest_plan`, `nb_ingest`, `nb_slv_load`, `nb_gld_dims`, `nb_gld_fact`, `nb_ops_recon`, `nb_process`, `Notebook_1`, `pl_daily_core` (danh sách của T3.3, làm sớm hơn). Còn lại để viết đè tại chỗ ở các task sau.
- Soạn sẵn `nb_spike` (K01–K05, K09, K10) cho T0.2; K06–K08, K11, K12 cần `pl_spike` dựng trên Fabric.
File: pyproject.toml, tools/check_repo.py, tools/build_contracts.py, fabric/platform/nb_contracts.Notebook/*, fabric/platform/nb_spike.Notebook/*
Tài liệu đã sửa: không.
Kiểm tra local: `uv run tools/build_contracts.py --check` → OK 9 contract. `uv run tools/check_repo.py` → 2 lỗi C08 của v1: `pl_master_daily`, `pl_backfill` còn trỏ notebook đã xoá (nb_process); sẽ hết khi viết lại ở T3.3, T6.1.
Thử âm: sửa `version` YAML không sinh lại → `--check` FAIL; xoá `watermarkColumn` của `wholesale.orders` → BC04; `%run` chung cell → C02; `--strict` → BC10 cho 9 contract draft.
Kiểm chứng Fabric: chưa kiểm chứng (nb_spike chưa chạy).
Còn lại / câu hỏi: người chạy `nb_spike` và dựng `pl_spike`, ghi kết quả vào DECISIONS §3 (T0.2); chạy T0.1, T0.3 trên Dev. Chưa bắt đầu T2.x vì phụ thuộc kết quả spike/EDA.
## 2026-10-15 — T0.2 lượt 1 (nb_spike) — IN_PROGRESS
Người/agent: Claude (Sonnet 5.5); chạy trên Fabric: anhnguyen   Runtime: Spark 4.1.1.5.5.20260930
Kết quả: K01, K02, K04, K05, K09, K10 PASS (chi tiết DECISIONS §3). Job `saveAsTable` Failed trong cell K10 là lỗi mong đợi (ghi NULL vào cột NOT NULL).
Phát hiện: `ansi = false` ⇒ `sum(bigint)` quay vòng im lặng; overwrite ghi `operation = CREATE OR REPLACE TABLE AS SELECT`; `fs.rm` ném lỗi khi path không tồn tại.
Tài liệu đã sửa: DECISIONS §3, 05 §4.6 (`last_commit_metrics`), TROUBLESHOOTING (dòng ANSI).
Còn lại: K03 (truyền hai ID), K06, K07, K08, K11, K12 cần `pl_spike`.
## 2026-10-15 — T0.2 lượt 2 (nb_spike, K03) — IN_PROGRESS
Người/agent: Claude (Sonnet 5.5); chạy trên Fabric: anhnguyen
Kết quả: K03 PASS; lặp lại K01, K02, K04, K05, K09, K10 cùng kết quả lượt 1.
Phát hiện: `fs.ls` ở `Files/inbound` của `lh_retail_drop` trả 6 mục, mục đầu tên `categories` — có thể cây thực tế khác `physicalName` trong contract (`inbound/reference/categories`, `inbound/retail/orders`). Đã mở rộng K03 để in cây thư mục (4 cấp); chờ chạy lại.
Sửa: bảng DECISIONS §3 bị hỏng ký tự ở lượt trước (lỗi escape khi ghi file), đã viết lại.
Còn lại: K06, K07, K08, K11, K12 cần `pl_spike`.
## 2026-10-15 — T0.1, T0.2, T0.3: notebook cho người chạy — IN_PROGRESS
Người/agent: Claude (Sonnet 5.5)
Đã làm:
- `nb_eda` (tạm): Q19 baseline v1 (T0.1) và E01–E20 (T0.3) trên Landing v1; chỉ đọc.
- `nb_spike`: thêm tham số `p_fail`, `p_exit_items`, `p_exit_pad`, trả `exitValue` (ID Source, danh sách `sources`) và hướng dẫn dựng `pl_spike` (K06, K07, K08, K11, K12) trong cell Markdown.
Phát hiện: drop zone Source có hai layout (phẳng `inbound/<entity>/` ghi 6/10 bởi simulator cũ — v1 cũng đọc đường dẫn này; `inbound/retail/<entity>/` ghi 12/10 chỉ có ngày 20260102). Contract trỏ layout mới; xử lý bằng reset Source ở T2.6.
Kiểm chứng Fabric: chưa kiểm chứng (chưa chạy `nb_eda`, `pl_spike`).
## 2026-10-15 — Đổi hướng: làm lại từ đầu, nguồn coi như sẵn sàng (D38) — DONE
Người/agent: Claude (Sonnet 5.5), theo yêu cầu người dùng.
Đã làm:
- Bỏ T0.1 (baseline v1); T2.6 không còn phụ thuộc T0.1, T0.3 và không reset nguồn; T8.1 đối chiếu với nguồn (Q19 mới = số tính từ ERP).
- `nb_eda` đọc trực tiếp nguồn (drop zone CSV + Delta của item SQL Database trên OneLake), bỏ baseline và Landing v1; không cần `lh_platform`.
- Thêm bảng "Phụ thuộc và thứ tự chạy" vào 06-tasks.
Chưa kiểm chứng: đọc `Tables/dbo/<bảng>` của item SQL Database bằng đường dẫn OneLake; kiểu item `SQLDatabase` cho `resolve_item_id`.
## 2026-10-15 — T0.3 EDA xong; T1.2 một phần — IN_PROGRESS
Người/agent: Claude (Sonnet 5.5); chạy trên Fabric: anhnguyen
Đã làm:
- Kết quả E01–E20 vào `docs/EDA.md`; D39–D42 vào DECISIONS.
- Contract: `retail.orders` (formats), `reference.categories` (bốn cột cấp thay `category_path`), ba master retail (bỏ ghi chú "chờ EDA"); các contract bị đổi bump `1.1.0`, vẫn `draft`.
- Tài liệu: 04 (§5.4 categories, §6.3 R-PRD-02, §6.5 thêm `first_returned_at_utc`), 05 §2.2 (từ điển).
Còn lại cho T1.2: E02b (định dạng thời gian master) rồi chuyển contract `active`; xác nhận múi giờ retail (D41); trả lời D42.
Kiểm chứng Fabric: EDA đã chạy; chưa có code v2 nào chạy.
## 2026-10-15 — T1.2 Hoàn thiện contract — REVIEW
Người/agent: Claude (Sonnet 5.5), theo chỉ đạo người dùng: giả định hợp lý khi mơ hồ, theo thực hành production.
Đã làm:
- 9 contract `status: active`; `build_contracts.py --strict` PASS.
- D42 (số âm: giữ nguyên + FLAG, R-ORD-05), D43 (giả định có ghi lại: định dạng thời gian master retail, `a/b/yyyy` đọc d/M, format lạ ⇒ REJECT).
- Tài liệu: 03 §4, 04 §7.2, §8 (R-ORD-05), §13 (DQ-SLV-05), 05 §2.3 (`flag_negative`), §3.3.
Chưa xác nhận với owner nguồn: múi giờ retail (D41), định dạng thời gian file master (D43). E02b không cần chạy nữa.
Kiểm chứng Fabric: không áp dụng (chỉ cấu hình và tài liệu).
## 2026-10-15 — T0.2 lượt 3 (pl_spike: K08, K11a, K11b, K11d) — IN_PROGRESS
Người/agent: Claude (Sonnet 5.5); chạy trên Fabric: anhnguyen
Kết quả: K08 PASS (ForEach tuần tự, `isSequential` phải đặt tường minh); K11a, K11b, K11d PASS; chi tiết DECISIONS §3.
Phát hiện thiết kế: Flatten hierarchy đổi tên file thành `data_<guid>.csv` ⇒ 04 §3.3 (khớp `expected_files` theo `(name, size)`) cần sửa sau khi thử Preserve hierarchy; staging cộng dồn nhiều lần chạy ⇒ thư mục staging theo `delivery_id`.
Sự cố vận hành: nhánh `dev` remote đi trước (commit từ workspace Source) làm 5 lần push bị từ chối mà không báo; đã rebase và push lại, sau đó kiểm `git status -sb`. Binding `lh_platform` của `nb_spike` phải nằm trong Git, nếu không Update from Git gỡ mất.
Còn lại: K11c, K11e, K06, K07, K12; thử Preserve hierarchy.
## 2026-10-15 — T0.2 K11b2: Preserve hierarchy — IN_PROGRESS
Kết quả: Copy behavior Preserve hierarchy giữ nguyên tên file ở staging (`orders_history_until_20251231.csv`). Đã sửa 04 §3.3 và DECISIONS K11.
Còn lại của T0.2: K11c (ID động), K11e (0 file), K06, K07, K12.
## 2026-10-15 — T0.2 K12 — IN_PROGRESS
Kết quả: K12 PASS (nhánh Failed + Skipped ở cuối chuỗi bắt được lỗi; `error.message` chứa `errorValue` của notebook). Chi tiết DECISIONS §3.
Còn lại của T0.2: K11c (ID động cho Connection), K11e (0 file), K06 (concurrency), K07 (Teams).
## 2026-10-15 — T0.2 K11e — IN_PROGRESS
Kết quả: khoảng last modified không khớp file ⇒ Copy Succeeded, `filesRead = 0`, `filesWritten = 0`.
Còn lại của T0.2: K11c (ID động), K11e phần "thư mục đích có được tạo không", K06, K07.
## 2026-10-15 — T0.2 K11e (tiếp)
Kết quả: khi 0 file, Copy không tạo thư mục đích. Đã sửa 04 §3.1 (nb_land coi staging không tồn tại là NO_DATA) và DECISIONS K11. Chưa thử Copy DB với 0 dòng.
## 2026-10-15 — T0.2 K06 bỏ qua (D44)
Người dùng quyết định bỏ K06 (không thực tế dựng lượt chạy chồng). Ghi D44: giữ `concurrency: 1` làm lớp phụ, đúng đắn dựa vào ghi idempotent; tiêu chí "xong khi" T3.3 sửa theo.
Còn lại của T0.2: K07 (Teams), K11c (ID động).
## 2026-10-15 — Đính chính K11e
Mục "T0.2 K11e (tiếp)" và commit `6918d5c` ghi "Copy không tạo thư mục đích khi 0 file" là **chưa kiểm chứng** (thư mục đích đã có từ trước nên không phân biệt được). Đã sửa 04 §3.1 và DECISIONS: nb_land xử lý cả hai trường hợp. Nguyên nhân: lệnh ghi bị ngắt bởi người dùng vẫn hoàn tất; từ nay sẽ kiểm `git log` sau khi bị ngắt.
## 2026-10-15 — T2.1 nb_config — REVIEW
Người/agent: Claude (Sonnet 5.5)   Nhánh: dev
Đã làm: viết lại `nb_config` theo 05 §2: `SETTINGS`, từ điển (từ EDA), `SILVER` (5 bảng), `GOLD` (5 bảng), `CHECKS` rỗng, `validate_config()` V01–V10; chạy độc lập được (`%run nb_contracts` ở cell đầu). Mọi `is_active = False` cho tới các task Silver/Gold.
File: fabric/platform/nb_config.Notebook/notebook-content.py (giữ nguyên logicalId và khối META của bản cũ).
Tài liệu đã sửa: 06-tasks (trạng thái).
Kiểm tra local: nạp `nb_contracts` + `nb_config` bằng Python thuần ⇒ "Validated 9 contracts, 5 silver, 5 gold, 0 checks"; thêm chu trình vào GOLD ⇒ V06 báo "gld.dim_date -> gld.fct_order_line -> gld.dim_date". `check_repo.py` OK.
Kiểm chứng Fabric: chưa kiểm chứng (chưa chạy notebook trên Fabric).
## 2026-10-15 — T0.2 phạm vi còn lại
Theo người dùng: chỉ K11c (ID động cho Connection) còn cần trước T3.3; K07 hoãn tới T6.1; K06 đã bỏ (D44).
## 2026-10-15 — T2.2 nb_std — REVIEW
Người/agent: Claude (Sonnet 5.5)   Nhánh: dev
Đã làm: `nb_std` mới (mọi hàm của 05 §3.2 và `conform_sql`). Literal chuỗi escape bằng `\'` (không dùng `''` vì Spark nối hai literal liền nhau thành chuỗi sai); `lookup:*` dùng `lookup_expr` khớp đúng chữ vì khoá `STATUS_RANK` là `Pending…`, không phải dạng `dict_key`.
Tài liệu đã sửa: 05 §3.1, §3.2 (thêm `lookup_expr`, tham số `available_columns`).
Kiểm tra local: `check_repo.py` (không có Spark/Java ở máy local nên không chạy SQL).
Kiểm chứng Fabric: chưa kiểm chứng; `nb_test_unit` (T2.5) kiểm trên Spark.
## 2026-10-15 — T2.3–T7.1: viết toàn bộ code v2 — REVIEW
Người/agent: Claude (Sonnet 5.5)   Nhánh: dev
Đã làm (theo yêu cầu "triển khai end to end"; K11c bỏ qua, người dùng tự dựng):
- `nb_common` (05 §4 đầy đủ + `run_node`, `conform_master`, `preflight`, `plan_ingest`…), `nb_setup_ddl` (sinh từ khối SQL của 04, C11 PASS), `nb_test_unit`.
- Ingest: `nb_plan`, `nb_land`, `nb_close`; `pl_master_daily` (13 activity + `fail_run`), `nb_dq`, `nb_recon`; `CHECKS` đủ 28 check trong `nb_config`.
- Silver: `nb_slv_orders`, `nb_slv_master`. Gold: `nb_gld_dim_date`, `dim_customer`, `dim_product`, `dim_salesman`, `fct_order_line`. Mọi object `is_active = True`.
- Vận hành: `nb_sla_watch` + `pl_sla_watch`, `nb_maintenance` + `pl_maintenance`, `nb_ops_delivery`, `nb_backfill_dates` + `pl_backfill`.
- Simulator: `nb_00_sim_inject` (workspace Source); `nb_test_e2e`.
Quyết định kỹ thuật cần biết:
- `snapshot_changes`: sửa SQL của 05 §4.9; `_seq` chỉ tính trên snapshot mới (dòng `previous` có `_seq = 0`), thêm điều kiện khoảng trống `_seq > _prev_seq + 1` ⇒ `REINSERT`. Bản cũ không phát hiện REINSERT khi dựng full.
- `resolve_quarantine`: chỉ đóng dòng REJECT thuộc delivery khác delivery mới nhất của khoá (dòng lỗi và dòng đúng cùng delivery không tự đóng nhau).
- `quarantine`: khoá chống trùng so sánh null-safe (`business_key` có thể NULL).
- `_merge`: MERGE không đổi gì có thể không tạo commit ⇒ so version trước/sau, trả số 0.
- `pl_master_daily` thêm activity `Fail` sau `close_failed` để Monitor hiện lượt Failed; `nb_close` lấy lỗi từ `run_log` thay vì biểu thức `activity().error`.
- `plan` trả `staging_folder`, `ids.platform_*`, `modify_time_utc` của file; `landed_file` so theo giây.
Kiểm tra local (không có Spark): `check_repo.py` OK (C01–C12); `preflight` và `plan_ingest` chạy bằng Python thuần với pyspark giả.
Kiểm chứng Fabric: **chưa kiểm chứng** — toàn bộ notebook SQL/Spark và pipeline chưa chạy trên Fabric. Thứ tự: docs/07 §1.4.
Chưa làm: cảnh báo Teams (K07, T6.1), Connection của Copy (người dùng gắn trong UI, ID Connection là placeholder `0000…`), T2.6 reset Dev (người chạy `nb_setup_ddl` với `p_reset`).