# 09 — Điều phối, vận hành, hiệu năng

## 1. Pipeline

```
pl_master_daily(p_load_date, p_mode)
  pl_ingest → nb_run_layer('slv') → nb_run_layer('gld') → nb_ops_recon → refresh sm_sales
  → email/Teams tóm tắt (DQ WARN, quarantine, recon)
  (bất kỳ lỗi) → email/Teams cảnh báo → pipeline FAILED (trạng thái do Fabric giữ)

pl_backfill(p_from, p_to)   lặp ngày (tuần tự) → pl_master_daily
pl_sim_drive(p_days)        [Source] nb_00_sim_daily → pl_master_daily(exit value)
pl_maintenance              OPTIMIZE theo lịch; VACUUM có kiểm retention (§4)
```

- **Fabric** giữ trạng thái pipeline/activity (Monitoring hub); **notebook** ghi audit dữ liệu (`ingestion_batch`, `dq_result`, `reconciliation_result`) — design/04 §1.
- `nb_run_layer(layer)`: DAG khai báo **trong notebook** (step → notebook, depends_on) → `notebookutils.notebook.runMultiple` (1 phiên Spark; kết quả từng notebook con hiện trong Monitoring) → gọi `nb_dq(layer)`.
- Mọi lệnh ghi bảng đặt `spark.databricks.delta.commitInfo.userMetadata = <run_id>` → `DESCRIBE HISTORY` cho biết version nào do run nào ghi.
- Retry: activity Notebook retry 1 lần (Fabric ghi lại từng lần thử).

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
| Bảng Gold/Silver hỏng | `DESCRIBE HISTORY <bảng>` → tìm version theo `userMetadata = run_id` của lượt tốt gần nhất → `RESTORE TABLE … VERSION AS OF <v>` — **chỉ khi version còn trong retention** |
| Hỏng sâu / sửa logic | `p_full_reload`: dựng lại Silver/Gold từ Bronze |
| Bronze hỏng | Dựng lại từ landing |
| Debug thiếu dữ liệu (Q15) | report → refresh → Gold history → Silver → quarantine → Bronze → `ingestion_batch` (cửa sổ) → landing → Source |

## 4. Retention
- `VACUUM` xoá file cũ → **mất time travel/RESTORE** về các version đó.
- Quy tắc: retention ≥ 7 ngày (mặc định Delta); `VACUUM` chỉ trong `pl_maintenance`, sau khi xác nhận không cần restore; ghi rõ trong runbook.
- Landing: giữ toàn bộ trong dự án này (nhỏ); dự án thật đặt lifecycle (vd 90 ngày) sau khi Bronze đã kiểm.

## 5. Hiệu năng — đo rồi mới tối ưu (Q12)
1. **Đo nền:** thời gian mỗi bước (Monitoring hub / Spark UI), số file & kích thước bảng (`DESCRIBE DETAIL`), dữ liệu quét của truy vấn report.
2. **Giả thuyết → thay đổi 1 thứ → đo lại.** Ứng viên: Liquid Clustering trên khoá lọc thường dùng; partition theo tháng **chỉ khi** bảng đủ lớn; OPTIMIZE gom file nhỏ; V-Order (đọc nhanh hơn, ghi chậm hơn — bật cho bảng Gold phục vụ Direct Lake); bảng aggregate.
3. **Không chốt partition/Z-order trước khi có số đo.** Với dữ liệu giả lập nhỏ, mục tiêu là chứng minh **biết cách đo** (scan, file count, duration), không phải benchmark capacity.

## Quyết định
- Log vận hành ở Fabric; RESTORE theo Delta history + `userMetadata`; DAG và rule nằm trong notebook; tối ưu sau đo.

## Còn mở
- K6 (`%%configure` qua pipeline/`runMultiple`).
- `meta.run_summary` cho `rpt_pipeline_health` — chỉ khi report cần (Bước 8).
