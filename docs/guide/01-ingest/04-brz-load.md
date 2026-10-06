# 1.4 — `nb_brz_load`: landing → Bronze (thiết kế)

## Mục tiêu
1 notebook **generic** nạp batch landing của **mọi** entity vào `brz.brz_<source>_<entity>`. Thêm nguồn mới = thêm 1 dòng config, không sửa notebook.

## Hiểu trước khi làm
- **Bronze = landing dạng bảng.** Không làm sạch, không lọc, không dedup. Dòng bẩn vẫn giữ — Silver xử lý.
- **Mọi cột là chuỗi:** file CSV vốn là chữ; ERP (parquet có kiểu) cũng ép về chuỗi → mọi Bronze cùng 1 kiểu, Silver parse bằng `nb_common` như nhau.
- **Cột kỹ thuật** thêm vào mỗi dòng:

  | Cột | Giá trị |
  |---|---|
  | `_source_system` | `wholesale` / `retail` / `reference` |
  | `_source_file` | đường dẫn file landing chứa dòng đó |
  | `_load_date` | `p_load_date` |
  | `_batch_id` | `<source>_<entity>_<yyyymmdd>` — **theo ngày, không theo lần chạy** (xem 1.5) |
  | `_run_id` | RunId của pipeline |
  | `_ingested_at` | thời điểm ghi (UTC) |

- **Idempotent:** `DELETE … WHERE _batch_id = X` rồi `INSERT` → chạy lại ngày D thay đúng phần của ngày D.
- **Schema evolution (Q16):** nguồn thêm cột → Bronze tự thêm cột (`mergeSchema`) + ghi `meta.schema_registry` (DETECTED).
- File: ghi `meta.state_file_manifest` mỗi file đã nạp (path, size, modified_at, batch_id, `LOADED`).

## Thiết kế notebook

```
Cell 0  %%configure -f   (lh_platform)
Cell 1  parameters: p_load_date, p_run_id, p_run_start
Cell 2  đọc meta.cfg_source_entity (is_active) theo load_order
Cell 3  với mỗi entity:
          path = Files/landing/<source>/<entity>/load_date=<d>/batch=<p_run_id>/
          không có thư mục → 0 dòng, bỏ qua
          db  → spark.read.parquet ; file → spark.read.csv(header, mọi cột string)
          ép mọi cột → string, thêm cột kỹ thuật
          bảng chưa có → tạo ; có rồi → DELETE theo _batch_id, append (mergeSchema)
          file → ghi state_file_manifest
          ghi nhận max(watermark_column) (db) cho 1.5
Cell 4  ghi watermark (1.5) — CHỈ chạy khi mọi entity ở Cell 3 thành công
```

Python ở đây là "keo dán" generic (đúng ADR 011); logic nghiệp vụ vẫn để Silver/Gold bằng SQL.

## Làm
Notebook do Claude viết vào repo → bạn **Update all** → chạy thử tay với `p_run_id` = RunId của lần chạy pipeline gần nhất (lấy ở Output) → kiểm bảng `brz.*`. Hướng dẫn chi tiết bổ sung khi notebook có trong repo.

## Kết quả mong đợi
9 bảng `brz.brz_<source>_<entity>`; `brz_wholesale_orders` ≈ 41.940 dòng, `brz_retail_orders` ≈ 42.258 dòng (42.194 + 64).
