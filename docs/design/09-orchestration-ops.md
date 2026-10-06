# 09 — Điều phối, vận hành, hiệu năng

## 1. Pipeline

```
pl_master_daily(p_load_date, p_mode)
  pl_ingest → nb_run_layer('slv') → nb_run_layer('gld') → nb_ops_recon → refresh sm_sales
  → nb_ops_run_end(SUCCEEDED) → email/Teams tóm tắt
  (bất kỳ lỗi) → nb_ops_run_end(FAILED) → email/Teams cảnh báo → pipeline FAILED

pl_backfill(p_from, p_to)   lặp ngày (tuần tự) → pl_master_daily
pl_sim_drive(p_days)        [Source] nb_00_sim_daily → pl_master_daily(exit value)
pl_maintenance              OPTIMIZE theo lịch; VACUUM có kiểm retention (§4)
```

- **Pipeline** chịu trách nhiệm điều phối và trạng thái tổng; **notebook** ghi chi tiết (task, batch, DQ, recon).
- `nb_run_layer(layer)`: DAG khai báo **trong notebook** (step → notebook, depends_on) → `notebookutils.notebook.runMultiple` (1 phiên Spark) → ghi `task_run` mỗi bước → gọi `nb_dq(layer)`.
- Retry: activity Notebook retry 1 lần; mỗi lần thử 1 dòng `task_run`.

## 2. Gọi notebook

| Cách | Dùng khi |
|---|---|
| `%run nb_common` | Dùng chung function trong cùng session |
| Pipeline Notebook activity + parameter cell | Bước lớn (ingest, layer) |
| `runMultiple` | Nhiều notebook trong 1 layer, chung session |

Cần kiểm (K6): `%%configure -f` khi notebook chạy từ pipeline và trong `runMultiple`.

## 3. Khôi phục

| Sự cố | Cách |
|---|---|
| Lượt chạy lỗi giữa chừng | Chạy lại cùng `p_load_date` (`p_mode = rerun`) |
| Bảng Gold/Silver hỏng | `RESTORE TABLE … VERSION AS OF <task_run.delta_version>` — **chỉ khi version còn trong retention** |
| Hỏng sâu / sửa logic | `p_full_reload`: dựng lại Silver/Gold từ Bronze |
| Bronze hỏng | Dựng lại từ landing |
| Debug thiếu dữ liệu (Q15) | report → refresh → Gold history → Silver → quarantine → Bronze → `ingestion_batch` (cửa sổ) → landing → Source |

## 4. Retention
- `VACUUM` xoá file cũ → **mất time travel/RESTORE** về các version đó.
- Quy tắc: retention ≥ 7 ngày (mặc định Delta); `VACUUM` chỉ trong `pl_maintenance`, sau khi xác nhận không cần restore; ghi rõ trong runbook.
- Landing: giữ toàn bộ trong dự án này (nhỏ); dự án thật đặt lifecycle (vd 90 ngày) sau khi Bronze đã kiểm.

## 5. Hiệu năng — đo rồi mới tối ưu (Q12)
1. **Đo nền:** thời gian mỗi bước (`task_run`), số file & kích thước bảng (`DESCRIBE DETAIL`), dữ liệu quét của truy vấn report.
2. **Giả thuyết → thay đổi 1 thứ → đo lại.** Ứng viên: Liquid Clustering trên khoá lọc thường dùng; partition theo tháng **chỉ khi** bảng đủ lớn; OPTIMIZE gom file nhỏ; V-Order (đọc nhanh hơn, ghi chậm hơn — bật cho bảng Gold phục vụ Direct Lake); bảng aggregate.
3. **Không chốt partition/Z-order trước khi có số đo.** Với dữ liệu giả lập nhỏ, mục tiêu là chứng minh **biết cách đo** (scan, file count, duration), không phải benchmark capacity.

## Quyết định
- `pipeline_run` ghi khi kết thúc; DAG và rule nằm trong notebook; tối ưu sau đo.

## Còn mở
- K6 (`%%configure` qua pipeline/`runMultiple`).
- Có cần dòng RUNNING trong `pipeline_run` không (hiện dùng Monitoring hub).
