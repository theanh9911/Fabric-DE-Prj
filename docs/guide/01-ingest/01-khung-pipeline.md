# 1.1 — Khung pipeline ✅

## Mục tiêu
Pipeline `pl_ingest` đọc danh sách 9 nguồn và lặp qua từng nguồn, rẽ nhánh theo loại (`db` / `file`).

## Hiểu trước khi làm

| Khối | Loại | Làm gì | Vì sao |
|---|---|---|---|
| `p_load_date` | Parameter | Ngày logic của lần chạy (virtual clock), vd `2026-01-01` | Đặt tên thư mục landing; không dùng ngày thật |
| `v_run_start` | Variable | Mốc trên của lần chạy | Mọi nguồn lấy chung 1 mốc `(watermark, run_start]` → không lệch giữa các nguồn |
| `set_run_start` | Set variable | `v_run_start = utcNow()` | |
| `lkp_source_entity` | Lookup | Đọc `cfg_source_entity` **JOIN** `state_watermark` → 9 dòng, mỗi dòng có mốc đã lấy | Pipeline không viết cứng nguồn nào (metadata-driven) |
| `fe_source_entity` | ForEach | Lặp 9 dòng, song song 4 | `item()` = dòng đang xử lý |
| `sw_source_type` | Switch | Rẽ theo `item().source_type` | `db` và `file` lấy dữ liệu khác nhau |
| `fail_unknown_source_type` | Fail | Báo lỗi nếu config có loại lạ | Không im lặng bỏ qua (P8) |

Lookup dùng **T-SQL Query** → chạy qua *SQL analytics endpoint* của `lh_platform` (cần cho JOIN). Endpoint đồng bộ trễ vài giây–phút so với bảng Delta; chạy mỗi ngày 1 lần thì không ảnh hưởng.

## Làm

**A. Tạo pipeline** — ws Dev → New item → **Data pipeline** → tên `pl_ingest`.

**B. Tham số & biến** — bấm vào nền trống của canvas → panel dưới:
- Tab **Parameters** → + New → `p_load_date`, String, để trống default.
- Tab **Variables** → + New → `v_run_start`, String.
- **Ctrl+S**.

**C. `set_run_start`** — Ribbon **Activities** → **Set variable**:
- General → Name `set_run_start`
- Settings → Variable type *Pipeline variable* · Name `v_run_start` · Value → *Add dynamic content*: `@utcNow('yyyy-MM-dd HH:mm:ss')`
- **Ctrl+S**.

**D. `lkp_source_entity`** — Activities → **Lookup**:
- **Nối:** kéo ô **✓ xanh lá** bên phải `set_run_start` thả vào Lookup. *(Không kéo từ ô ↷ trên cùng — đó là "Skipped".)*
- General → Name `lkp_source_entity` · Retry `2`
- Settings → Connection **Lakehouse anhnguyen** · Lakehouse `lh_platform` · Root folder **Tables** · Use query **T-SQL Query**:
  ```sql
  SELECT e.source_system, e.entity, e.source_type, e.source_object,
         e.watermark_column, e.lookback_days,
         COALESCE(w.watermark_value, '1900-01-01 00:00:00') AS watermark_value
  FROM meta.cfg_source_entity e
  LEFT JOIN meta.state_watermark w
         ON w.step = 'ingest' AND w.source_system = e.source_system AND w.entity = e.entity
  WHERE e.is_active = 1
  ORDER BY e.load_order
  ```
- **First row only: bỏ tick.**
- **Preview data** → 9 dòng. **Ctrl+S**.

**E. `fe_source_entity`** — Activities → **ForEach**:
- Nối từ ô **✓** của Lookup.
- General → Name `fe_source_entity`
- Settings → Sequential *không tick* · Batch count `4` · Items: `@activity('lkp_source_entity').output.value`
- **Ctrl+S**.

**F. `sw_source_type`** — trên khối ForEach bấm ✏️ (vào trong) → Activities → **Switch**:
- General → Name `sw_source_type`
- Activities → Expression `@item().source_type` · đổi `Case1` thành `db` · + Add case `file`.
- Case **Default** → ✏️ → Activities → **Fail**: Name `fail_unknown_source_type` · Fail message (dynamic):
  `@concat('Unknown source_type: ', item().source_type, ' (', item().source_system, '.', item().entity, ')')` · Error code `UNKNOWN_SOURCE_TYPE`.
- Case trống không lưu được → tạm thêm **Wait** 1 giây vào case `db` và `file` (`wait_todo_db`, `wait_todo_file`), thay bằng Copy ở việc 2 và 3.
- **Ctrl+S** → **Source control → Commit**.

## Kết quả mong đợi
- Bấm vào từng mũi tên → đều là **Succeeded**.
- Preview Lookup → 9 dòng, `watermark_value = 1900-01-01 00:00:00` (chưa lấy lần nào).

## Lỗi đã gặp
Xem [99-loi-da-gap.md](99-loi-da-gap.md): mũi tên "Skipped" · `\n` thừa trong biểu thức · case trống · mất pipeline chưa lưu · `Invalid object name 'meta…'`.
