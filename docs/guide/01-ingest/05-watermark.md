# 1.5 — Watermark + nối `nb_brz_load` vào pipeline (thiết kế)

## Mục tiêu
Mỗi lần chạy chỉ lấy phần mới; chạy lại 1 ngày → lấy **đúng cửa sổ cũ** → kết quả không đổi.

## Hiểu trước khi làm — 4 quy tắc

**1. ERP: watermark = `max(updated_at)` của dữ liệu đã lấy — KHÔNG phải giờ chạy.**
`updated_at` trong ERP là **giờ giả lập** (vd `2026-01-01`), còn giờ chạy pipeline là **giờ thật** (`2026-10-06`). Nếu lưu giờ chạy làm watermark, ngày mai simulator thả dòng `updated_at = 2026-01-02` < `2026-10-06` → **không bao giờ được lấy**.
Dự án thật cũng làm vậy: watermark lấy từ chính dữ liệu, tránh lệch đồng hồ giữa máy nguồn và máy pipeline.

**2. File: watermark = `run_start` (giờ thật).**
LastModified của file là giờ thật lúc team nguồn ghi file → so với giờ thật là đúng. Bộ lọc Copy là `start ≤ LastModified < end` → lần sau bắt đầu đúng chỗ lần trước kết thúc, không hở, không trùng.

**3. Mỗi ngày logic ghi 1 dòng watermark (append-only, có `load_date`).**
Chạy ngày D đọc watermark của **ngày gần nhất trước D**. → Chạy lại D dùng lại đúng mốc bắt đầu cũ → cùng cửa sổ.

**4. Watermark chỉ ghi khi mọi thứ thành công.**
Copy lỗi → ForEach lỗi → `nb_brz_load` không chạy → watermark không tiến → lần sau lấy lại đúng cửa sổ đó. Không mất dữ liệu.

| | ERP (`db`) | File |
|---|---|---|
| Lấy | `updated_at > wm − lookback AND updated_at <= run_start` | `wm ≤ LastModified < run_start` |
| Watermark mới | `max(updated_at)` trong batch (0 dòng → giữ mốc cũ) | `run_start` |
| Trùng do lookback | có (dòng ngày cuối lấy lại) → Silver dedup theo khoá + `updated_at` | không |

**Trường hợp biên — dòng ERP có `updated_at` tương lai (năm 2027, lỗi nhập liệu cố ý):**
cận trên `updated_at <= run_start` (giờ thật, 2026) loại chúng → **không vào Bronze qua incremental**, và nhờ vậy **không đẩy watermark lên 2027** (nếu đẩy lên, mọi dòng sau đó bị bỏ). Đây là hành vi đúng ở hệ thật; số dòng chênh này phải lộ ra ở **recon Source ↔ Bronze** (Bước 6) và ghi vào `dq_findings.md` (Bước 2). Bên nguồn file (retail) các dòng 2027 vẫn vào Bronze vì file được chép nguyên.

## Thay đổi cần làm
1. **`nb_setup_ddl`** — thêm cell: `ALTER TABLE meta.state_watermark ADD COLUMNS (load_date DATE)`.
2. **Lookup `lkp_source_entity`** — query mới (dynamic content, vì có `p_load_date`):
   ```
   @concat('SELECT e.source_system, e.entity, e.source_type, e.source_object, e.watermark_column, e.lookback_days,
     COALESCE((SELECT TOP 1 w.watermark_value FROM meta.state_watermark w
               WHERE w.step = ''ingest'' AND w.source_system = e.source_system AND w.entity = e.entity
                 AND w.load_date < ''', pipeline().parameters.p_load_date, '''
               ORDER BY w.load_date DESC, w.updated_at DESC), ''1900-01-01 00:00:00'') AS watermark_value
   FROM meta.cfg_source_entity e WHERE e.is_active = 1 ORDER BY e.load_order')
   ```
3. **`nb_brz_load` Cell 4** — append 1 dòng / entity vào `state_watermark` (`load_date = p_load_date`); xoá dòng cũ của cùng `load_date` trước (chạy lại không nhân đôi).
4. **Pipeline** — thêm **Notebook** activity `nb_brz_load`, nối ✓ sau `fe_source_entity`:
   - Notebook: `nb_brz_load`
   - Base parameters: `p_load_date` = `@pipeline().parameters.p_load_date` · `p_run_id` = `@pipeline().RunId` · `p_run_start` = `@variables('v_run_start')`

## Kết quả mong đợi
`meta.state_watermark` có 9 dòng `load_date = 2026-01-01`: 4 dòng ERP mang `max(updated_at)` (ngày 2026-01-01 giả lập), 5 dòng file mang `run_start`.
