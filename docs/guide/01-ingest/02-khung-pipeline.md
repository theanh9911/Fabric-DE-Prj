# 1.5a — Khung pipeline: tham số, Lookup, ForEach, Switch

## Mục tiêu
`pl_ingest` tính **cửa sổ nguồn** cho từng entity rồi lặp qua chúng; rerun thì không Copy.

## Hiểu trước khi làm
- Lookup trả về **kế hoạch ingest**: mỗi entity 1 dòng, có sẵn `window_start` / `window_end`. Copy chỉ việc dùng → mọi logic cửa sổ nằm 1 chỗ (câu SQL của Lookup).
- **Mỗi cửa sổ một đồng hồ** ([design/03 §2](../../design/03-ingest.md)):

  | load_strategy | window_start | window_end | Đồng hồ |
  |---|---|---|---|
  | db_incremental | watermark − lookback (chưa có → 1900-01-01) | **cuối ngày `p_load_date`** (`p_load_date + 1 ngày`, loại trừ) | nguồn (virtual clock) |
  | db_full_snapshot | *(không dùng)* | `run_start` (chỉ để ghi log) | — |
  | file_new_or_changed | watermark − lookback | `run_start − settle` | giờ thật UTC |

- Lookup chạy T-SQL qua SQL endpoint; thời gian định dạng `yyyy-MM-dd HH:mm:ss` (`CONVERT(…, 120)`).
- Pipeline đã có từ 1.1; dưới đây là **cấu hình đích** và **cần sửa gì**.

## Làm (B)

**A. Tham số & biến** — bấm nền canvas:

| Tab | Tên | Kiểu | Mặc định | So với bản cũ |
|---|---|---|---|---|
| Parameters | `p_load_date` | String | *(trống)* | giữ |
| Parameters | `p_mode` | String | `normal` | **thêm** |
| Variables | `v_run_start` | String | | giữ |

**B. `set_run_start`** — giữ: `@utcNow('yyyy-MM-dd HH:mm:ss')`.

**C. `lkp_source_entity`** — Settings → Use query **T-SQL Query** → *Add dynamic content* → **thay** bằng:
```
@concat('SELECT e.source_system, e.entity, e.load_strategy, e.source_object, e.watermark_column, ',
'w.watermark_value AS watermark_before, ',
'CONVERT(varchar(19), DATEADD(minute, -COALESCE(e.lookback_minutes, 0), ',
'  CAST(COALESCE(w.watermark_value, ''1900-01-01 00:00:00'') AS datetime2)), 120) AS window_start, ',
'CONVERT(varchar(19), CASE e.load_strategy ',
'  WHEN ''db_incremental'' THEN DATEADD(day, 1, CAST(''', pipeline().parameters.p_load_date, ''' AS datetime2)) ',
'  WHEN ''file_new_or_changed'' THEN DATEADD(minute, -e.settle_minutes, CAST(''', variables('v_run_start'), ''' AS datetime2)) ',
'  ELSE CAST(''', variables('v_run_start'), ''' AS datetime2) END, 120) AS window_end ',
'FROM meta.cfg_source_entity e ',
'LEFT JOIN meta.watermark_state w ON w.source_system = e.source_system AND w.entity = e.entity ',
'WHERE e.is_active = 1 ORDER BY e.load_order')
```
First row only: **bỏ tick** · Retry `2` · **Ctrl+S**.

**D. `fe_source_entity`** — Settings → Items → **thay** bằng:
```
@if(equals(pipeline().parameters.p_mode, 'rerun'), json('[]'), activity('lkp_source_entity').output.value)
```
Sequential: không tick · Batch count `4` · **Ctrl+S**.

**E. `sw_source_type`** — Activities → Expression → **thay** bằng:
```
@if(startsWith(item().load_strategy, 'db_'), 'db', 'file')
```
Giữ case `db`, `file`, Default `fail_unknown_source_type` · **Ctrl+S** → **Commit**.

## Kết quả mong đợi
- Không Preview được (biểu thức có `variables()`) → kiểm ở lần Run đầu (file 06): Output → `lkp_source_entity` → 9 dòng. Với `p_load_date = 2026-01-01`, lần đầu:
  - `wholesale.orders`: `window_start = 1899-12-31 00:00:00`, `window_end = 2026-01-02 00:00:00`.
  - file: `window_start = 1899-12-31 00:00:00`, `window_end` = giờ chạy − 5 phút.
- Mọi mũi tên là **Succeeded** (kiểm bằng `{ }`).
