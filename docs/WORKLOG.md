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