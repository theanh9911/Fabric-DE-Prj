# 07 — Vận hành và kiểm thử

## 1. Môi trường

| Workspace | Nhánh Git | Thư mục | Ghi chú |
|---|---|---|---|
| `CompanyA-Source` | `dev` | `fabric/source` | simulator; platform chỉ đọc |
| `CompanyA-DataPlatform-Dev` | `dev` | `fabric/platform` | phát triển, kiểm chứng, diễn tập |
| `CompanyA-DataPlatform-Prod` | `main` | `fabric/platform` | chạy theo lịch |

Identity chạy pipeline cần: quyền dùng hai Connection tới `sqldb_erp_wholesale` và `lh_retail_drop` ở workspace nguồn (Copy activity), quyền đọc thư mục drop zone (để `nb_plan` liệt kê file), quyền đọc `lh_sim` (chỉ Dev, cho `nb_test_e2e`), quyền ghi `lh_platform`.

### 1.1 Dựng workspace platform mới

1. Tạo workspace trên capacity, cấp quyền Contributor cho người vận hành.
2. **Workspace settings → Git integration**: nối repo, nhánh đúng môi trường, thư mục `fabric/platform`; **Update all**. Không chép workspace ID hay item ID từ workspace khác.
3. Tạo Lakehouse `lh_platform` nếu Git chưa tạo.
4. Với từng notebook trừ notebook thư viện ([04 §2](04-kien-truc.md#2-workspace-item-path)): **Data items → Add** `lh_platform`, đặt làm default; bật **Lakehouse Auto-Binding in Git**; lưu, commit binding, sync lại.
5. Mở từng pipeline, **Validate**; sửa nếu notebook reference chưa được gắn.
6. Chạy `nb_setup_ddl` (`p_reset = False`).
7. Chạy `nb_test_unit` ⇒ mọi test PASS.
8. Tạo Connection cho: (a) SQL Database nguồn, (b) Lakehouse drop zone (hai cái này cho Copy activity), (c) kênh cảnh báo; gắn vào `pl_master_daily`, `pl_sla_watch`.
9. Chạy thử `pl_master_daily` một lượt; kiểm Q01, Q17.
10. Đặt lịch ([04 §15](04-kien-truc.md#15-pipeline-lịch-cảnh-báo-bảo-trì)) — chỉ Prod.

### 1.4 Chạy kiểm chứng lần đầu (Dev)

Làm theo thứ tự; dừng ở bước đầu tiên không đạt và dán lỗi vào WORKLOG ([§6.3](#63-báo-kết-quả-kiểm-chứng)).

| # | Việc | Kỳ vọng |
|---|---|---|
| 1 | Workspace Dev: **Update all** từ `dev`; gắn `lh_platform` làm default cho từng notebook (nếu Git chưa giữ binding) | không còn lỗi sync |
| 2 | Chạy `nb_config` | in `Validated 9 contracts, 5 silver, 5 gold, 28 checks` |
| 3 | Chạy `nb_setup_ddl` (`p_reset = False`), hai lần | lần hai không lỗi; in `nb_setup_ddl OK` |
| 4 | Chạy `nb_test_unit` | mọi test PASS |
| 5 | Tạo Connection (SQL Database nguồn, Lakehouse drop zone, Lakehouse platform) và chọn trong từng Copy activity của `pl_master_daily` (trong `if_db`) | Validate không lỗi |
| 6 | Chạy `nb_plan` trực tiếp (`p_load_date` = ngày simulator đầu) | `sources` đủ 9 source |
| 7 | Chạy `pl_master_daily` với `p_load_date` = ngày simulator | Q01, Q03, Q17: mọi activity Succeeded |
| 8 | Chạy lại cùng tham số | source file `NO_DATA`, DB snapshot `UNCHANGED`, node `SKIPPED up_to_date` |
| 9 | Chạy `nb_test_e2e` | không FAIL (kịch bản chưa tiêm là SKIP) |
### 1.2 Reset Dev

Chỉ ở Dev. Đưa platform về trống để chạy lại từ đầu. Bước 2 và 5 (dựng lại nguồn) chỉ làm khi cần dữ liệu nguồn mới; khi nguồn đã sẵn sàng thì bỏ qua.

1. Đảm bảo không có pipeline nào đang chạy.
2. (Chỉ khi dựng lại nguồn) Workspace Source: chạy `nb_00_sim_reset` với `p_confirm = "RESET"`.
3. Workspace Dev: chạy `nb_setup_ddl` với `p_reset = True`, `p_confirm = "RESET"`.
4. Kiểm: Q18 chỉ còn object rỗng; nếu đã reset nguồn thì `lh_retail_drop/Files/inbound` trống.
5. (Chỉ khi dựng lại nguồn) Workspace Source: chạy `nb_00_sim_daily` (lần đầu = initial load tới 2025-12-31). Exit value là `p_load_date` cho lượt platform đầu tiên.

### 1.3 Promote Prod

1. `uv run tools/build_contracts.py --strict` PASS (không còn contract `draft`).
2. `uv run tools/check_repo.py` PASS.
3. Pull request `dev` → `main`, review, merge.
4. Workspace Prod: **Update all** từ `main`; làm bước 4–5 của §1.1 nếu là lần đầu.
5. `nb_setup_ddl` (`p_reset = False`).
6. Lượt đầu: `pl_master_daily` với `p_load_date` = ngày hiện hành của nguồn. Kiểm Q01, Q17, Q20.
7. Bật lịch.

## 2. Danh mục item

Đích cuối cùng; mọi item khác trong `fabric/platform/` là thừa.

| Item | Loại | Chạy bởi |
|---|---|---|
| `lh_platform` | Lakehouse | |
| `nb_contracts` | Notebook (sinh tự động) | `%run` |
| `nb_config`, `nb_std`, `nb_common` | Notebook thư viện | `%run` |
| `nb_setup_ddl` | Notebook | người |
| `nb_plan`, `nb_land`, `nb_close` | Notebook ingest và đóng lượt | `pl_master_daily` |
| `nb_slv_orders`, `nb_slv_master` | Notebook Silver | `pl_master_daily` |
| `nb_gld_dim_date`, `nb_gld_dim_customer`, `nb_gld_dim_product`, `nb_gld_dim_salesman`, `nb_gld_fct_order_line` | Notebook Gold | `pl_master_daily` |
| `nb_dq`, `nb_recon` | Notebook check | `pl_master_daily` |
| `nb_sla_watch` | Notebook | `pl_sla_watch` |
| `nb_maintenance` | Notebook | `pl_maintenance` |
| `nb_test_unit`, `nb_test_e2e` | Notebook test | người |
| `nb_ops_delivery` | Notebook thao tác (park/release delivery) | người |
| 
b_backfill_dates | Notebook (sinh danh sách ngày cho pl_backfill) | pl_backfill |
| `pl_master_daily`, `pl_backfill`, `pl_sla_watch`, `pl_maintenance` | Data pipeline | lịch / người |

Workspace Source: `lh_sim`, `lh_retail_drop`, `sqldb_erp_wholesale`, `nb_00_sim_common`, `nb_00_sim_setup`, `nb_00_sim_daily`, `nb_00_sim_reset`, `nb_00_sim_inject`.

## 3. Kiểm thử

### 3.1 Các tầng

| Tầng | Công cụ | Chạy ở | Khi nào |
|---|---|---|---|
| Tĩnh | `uv run tools/check_repo.py` (gồm `build_contracts.py --check`) | máy local | mọi commit |
| Unit | `nb_test_unit` | Dev | mọi thay đổi `nb_std`, `nb_common`, `nb_config`, `nb_contracts` |
| Worker | chạy `pl_master_daily` với `p_scope` = bảng đang làm | Dev | mỗi task Silver/Gold |
| E2E | simulator + `nb_test_e2e` | Dev | T8.2 và sau thay đổi lớn |
| Mỗi lượt | `nb_dq`, `nb_recon` | mọi môi trường | luôn |

### 3.2 `nb_test_unit`

- Chỉ dùng schema `tst` (tạo ở đầu, `DROP SCHEMA tst CASCADE` ở cuối); không đọc/ghi `meta`, `slv`, `gld`.
- Fixture nội tuyến bằng `spark.createDataFrame([...], "<schema>")`.
- Mỗi test là hàm `test_<đối_tượng>__<tình_huống>()` dùng `assert` có thông điệp.
- Cuối notebook:

```python
results = []
for name, fn in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
    try:
        fn()
        results.append((name, "PASS", ""))
    except Exception as exc:
        results.append((name, "FAIL", str(exc)[:500]))
display(spark.createDataFrame(results, "test string, status string, error string"))
failed = [r[0] for r in results if r[1] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} test(s) failed: {failed}")
```

| Nhóm | Test tối thiểu |
|---|---|
| `nb_std` | mọi dòng "Test bắt buộc" ở [05 §3.2](05-thu-vien.md#32-hàm); `conform_sql` với fixture [05 §3.3](05-thu-vien.md#33-conform_sql) |
| `validate_config` | cấu hình thật PASS; mỗi luật V01–V10 có một cấu hình hỏng bị bắt |
| `decide_run` | mỗi dòng bảng [04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node) một test (state giả, version giả) |
| `plan_ingest` | cửa sổ đầu tiên; có watermark + lookback; `virtual` vs `real` |
| `align_to_target` | cột thừa ⇒ lỗi; thiếu nullable ⇒ NULL; thiếu NOT NULL ⇒ lỗi; cast kiểu |
| `merge_events` | chèn mới; chạy lại ⇒ 0 thay đổi; delivery mới hơn + hash khác ⇒ update; delivery cũ hơn ⇒ bỏ qua |
| `merge_upsert`, `sync_table` | hash giống ⇒ không đổi; dòng biến mất ⇒ xoá (sync); chạy lại ⇒ CDF rỗng |
| `snapshot_changes` | test ở [05 §4.9](05-thu-vien.md#49-snapshot_changessnapshots-previous-key_cols) |
| `read_changes` | dải rỗng ⇒ rỗng; dải hợp lệ ⇒ đúng số thay đổi |
| quarantine | reject + flag ghi; chạy lại không trùng; resolve đặt `resolved_at_utc` |

### 3.3 `nb_test_e2e`

Đọc bảng thật (chỉ đọc) sau khi diễn tập; mỗi kịch bản §5 một hàm `test_scenario_<tên>()` assert cột "Kết quả mong đợi"; cộng `test_current_matches_view` (`latest_event` so với `slv.v_orders_current` trên dữ liệu thật: cùng số dòng, cùng tổng băm `_record_hash`) và RC-06 (đọc `lh_sim.sim_release_log` qua abfss, chỉ đọc). Runner như §3.2.

## 4. Thư viện SQL kiểm chứng

Chạy trong notebook gắn `lh_platform` (cell `%%sql` hoặc Python). Thay `<…>`.

**Q01 — lượt gần nhất**
```sql
SELECT event_at_utc, step, layer, event, reason, exec_mode, rows_in, rows_inserted, rows_updated,
       rows_deleted, rows_rejected, rows_flagged, error_message
FROM meta.v_run_latest ORDER BY event_at_utc;
```
**Q02 — quyết định của planner**
```sql
SELECT step, event, reason, exec_mode, input_versions, output_version
FROM meta.run_log WHERE run_id = '<run_id>' AND layer IN ('slv', 'gld') ORDER BY event_at_utc;
```
**Q03 — delivery của lượt**
```sql
SELECT delivery_id, status, batch_id, source_system, entity, rows_landed, source_count,
       rows_read, rows_accepted, rows_rejected, rows_flagged, schema_added, schema_missing, error_message
FROM meta.delivery_log WHERE run_id = '<run_id>' ORDER BY source_system, entity, event_at_utc;
```
**Q04 — file đã xử lý**
```sql
SELECT source_system, entity, file_name, file_size, modify_time_utc, outcome, delivery_id, batch_id
FROM meta.landed_file ORDER BY landed_at_utc DESC LIMIT 50;
```
**Q05 — manifest** (Python)
```python
print(notebookutils.fs.head("<landing_path>/_manifest.json", 100000))
```
**Q06 — `rows_landed` khớp dữ liệu**
```python
p = "<landing_path>"; m = json.loads(notebookutils.fs.head(p + "/_manifest.json", 100000))
n = spark.read.parquet(p).count() if m["format"] == "parquet" else read_csv_strings([p]).count()
print(m["rows_landed"], n, m["source_count"])
```
**Q07 — quarantine đang mở**
```sql
SELECT entity, source_system, reason, action, count(*) n
FROM slv.v_quarantine_open GROUP BY ALL ORDER BY n DESC;
```
**Q08 — orders_event theo delivery**
```sql
SELECT _delivery_id, source_system, count(*) n, min(updated_at_utc), max(updated_at_utc)
FROM slv.orders_event GROUP BY ALL ORDER BY 1;
```
**Q09 — dòng hiện hành duy nhất; lịch sử một đơn**
```sql
SELECT count(*) - count(DISTINCT source_system, order_no, product_code) AS dup FROM slv.v_orders_current;
SELECT * FROM slv.orders_event WHERE order_no = '<order_no>' ORDER BY product_code, updated_at_utc;
```
**Q10 — loại thay đổi master**
```sql
SELECT source_system, _snapshot_date, _change_type, count(*) FROM slv.<entity>_history
GROUP BY ALL ORDER BY 2, 1, 3;
```
**Q11 — lịch sử một mã master**
```sql
SELECT * FROM slv.<entity>_history WHERE <key> = '<code>' ORDER BY source_system, _snapshot_date;
```
**Q12 — dấu vân tay bảng** (so hai lần dựng)
```sql
SELECT count(*) n, sum(cast(xxhash64(_record_hash) AS decimal(38,0))) h FROM slv.orders_event;
```
**Q13 — CDF của commit gần nhất** (Python)
```python
v = spark.sql("DESCRIBE HISTORY <table> LIMIT 1").first()["version"]
display(spark.sql(f"SELECT _change_type, count(*) FROM table_changes('<table>', {v}, {v}) GROUP BY 1"))
```
**Q14 — phiên bản SCD2**
```sql
SELECT source_system, salesman_code, valid_from, valid_to, is_current, manager_code, manager_name, is_inferred
FROM gld.dim_salesman WHERE salesman_code = '<code>' ORDER BY source_system, valid_from;
```
**Q15 — tổng hợp fact**
```sql
SELECT source_system, current_status, count(*) n, sum(quantity * price) amount,
       count_if(customer_sk = -1) unk_customer, count_if(salesman_sk = -1) unk_salesman
FROM gld.fct_order_line GROUP BY ALL ORDER BY 1, 2;
```
**Q16 — dòng fact ghi trong lượt**
```sql
SELECT floor(order_date_key / 100) AS month_key, count(*) FROM gld.fct_order_line
WHERE _run_id = '<run_id>' GROUP BY 1 ORDER BY 1;
```
**Q17 — check của lượt**
```sql
SELECT check_id, source, object_name, scope_value, status, expected, actual, message, sample
FROM meta.check_result WHERE run_id = '<run_id>' ORDER BY status DESC, check_id;
```
**Q18 — object và CDF**
```sql
SHOW TABLES IN meta; SHOW TABLES IN slv; SHOW TABLES IN gld;
SHOW TBLPROPERTIES gld.fct_order_line ('delta.enableChangeDataFeed');
```
**Q19 — số tính trực tiếp từ nguồn ERP** (chạy ở SQL endpoint của `sqldb_erp_wholesale`, chỉ đọc; dùng ở T8.1)
```sql
SELECT COUNT(*) AS n_events,
       COUNT(DISTINCT CONCAT(order_no, '|', product_code)) AS n_lines,
       SUM(quantity * price) AS gross_events
FROM dbo.orders;
```
**Q20 — v2 theo tháng** (dòng Delivered, theo tháng `order_date`, `quantity * price`; đối chiếu với nguồn ở T8.1)
```sql
SELECT floor(order_date_key / 100) AS month_key, count(*) n, sum(quantity * price) gross,
       count_if(salesman_sk = -1) unk_salesman
FROM gld.fct_order_line WHERE current_status = 'Delivered' GROUP BY 1 ORDER BY 1;
```
**Q21 — checkpoint**
```sql
SELECT target_table, logic_version, input_versions, output_version, last_exec_mode, last_success_at_utc
FROM meta.table_state ORDER BY target_table;
```

## 5. Kịch bản sự cố nguồn

Kịch bản "seed" có sẵn trong dữ liệu; kịch bản khác tạo bằng `nb_00_sim_inject` ([02 §3](02-nguon.md#3-tiêm-sự-cố-nguồn--nb_00_sim_inject-đặc-tả-cho-t71)) trước lượt platform của ngày đó.

| Kịch bản | Cách tạo | Kết quả mong đợi | Kiểm |
|---|---|---|---|
| late arrival | seed (2026-01-15 có `order_date = 2026-01-02`) | dòng vào `fct_order_line` với `order_date_key` tháng 1; chỉ dòng hàng bị chạm có `_run_id` của lượt | Q16 |
| order update | seed | `orders_event` đủ mọi sự kiện; `v_orders_current` là trạng thái mới nhất; `first_*_at_utc` đúng | Q09, Q15 |
| dữ liệu bẩn | seed + `bad_rows` | REJECT/FLAG đúng mã; lượt không dừng; DQ-SLV-04 theo ngưỡng contract | Q07, Q17 |
| `file_resend` | inject | một delivery mới; dòng sửa được cập nhật (R-ORD-03); không nhân đôi | Q04, Q08, Q12 |
| `duplicate_file` | inject | một delivery mới; Silver `rows_inserted = 0`; DQ-ING-01 WARN | Q03, Q17 |
| `missing_file` | inject | SLA watch cảnh báo; khi file tới lượt sau ⇒ nạp đủ | Q03 |
| `partial_file` | inject (lần 1 nửa file, ngay trước lượt) | file bị settle bỏ qua; lần 2 ghi đủ ⇒ lượt sau nạp đủ | Q04 |
| `schema_change` | inject | Landing có cột mới; Silver bỏ qua; `schema_added`; DQ-SLV-06 WARN | Q03, Q17 |
| `org_change` | inject | history `UPDATE`; `dim_salesman` 2 phiên bản; dòng hàng có `order_date` trước ngày đổi giữ salesman_sk phiên bản cũ | Q11, Q14 |
| `master_delete` | inject | history `DELETE`; dim `is_deleted = true`; fact không đổi | Q10, Q11 |
| lỗi giữa lượt | sửa tạm một notebook Gold cho raise | activity lỗi, activity sau không chạy, `close_failed` ghi lượt FAILED và cảnh báo; bỏ lỗi, **Rerun from failed activity** (hoặc chạy lại `normal`) ⇒ chỉ phần lỗi và phần sau chạy; notebook xong trước đó `SKIPPED up_to_date` | Q01, Q02 |
| chạy chồng | hai lượt cùng lúc | theo K06: lượt sau bị hoãn; nếu cả hai chạy thì số liệu không sai (hai delivery, Silver dedupe, DQ-ING-01 WARN) | Q01, Q03 |
| delivery hỏng schema | sửa tay một bản sao file đã tới (xoá một cột bắt buộc) | delivery `FAILED`; delivery khác `LOADED`; activity Silver Failed, Gold không chạy; sửa file và gửi lại ⇒ delivery mới `LOADED`, delivery hỏng cũ tự hết chặn, lượt xanh; không gửi lại mà `park` ⇒ lượt kế xanh, DQ-OPS-03 WARN | Q01, Q03 |

## 6. Vận hành

### 6.1 Công thức

| Tình huống | Làm |
|---|---|
| Hằng ngày | lịch chạy `pl_master_daily` mặc định (Dev: chạy simulator, chờ hơn `settleMinutes`, rồi `pl_master_daily` với `p_load_date` = ngày giả lập) |
| Lượt FAILED | Monitor → activity Failed đầu tiên; Q01 cho `reason`, `error_message`. Sửa nguyên nhân rồi **Rerun from failed activity**, hoặc chạy lại `normal` cùng `p_load_date`: phần đã xong được bỏ qua, phần lỗi chạy tiếp |
| Lượt FAILED vì delivery hỏng (activity Silver Failed, `delivery_log` có `FAILED`) | Q03 xem lỗi. Báo nguồn gửi lại bản đúng rồi chạy `normal`: delivery hỏng cũ tự hết chặn. Nguồn không gửi lại được và chấp nhận bỏ ⇒ `nb_ops_delivery` `park` kèm lý do và tên người chạy (không chạy khi lượt đang chạy). `release` để xử lý lại sau |
| Nạp lại batch D mà không đọc nguồn | `p_mode = rerun`, `p_load_date = D` |
| Đổi logic một bảng | sửa notebook, tăng `logic_version`, deploy; lượt `normal` kế tiếp tự chạy full bảng đó và các bảng con |
| Dựng lại một bảng | `p_mode = rerun`, `p_full_reload = true`, `p_scope = <bảng>` |
| Dựng lại toàn bộ | `p_mode = rerun`, `p_full_reload = true`, `p_scope = ALL` |
| Thêm định dạng ngày / biến thể trạng thái | contract (MINOR) hoặc từ điển `nb_config`; tăng `logic_version` Silver ⇒ dòng đã bị REJECT được nạp lại khi full |
| Bù nhiều ngày | `pl_backfill` với `p_start_date`, `p_end_date` |
| Quarantine tăng đột biến | Q07; lỗi định dạng ⇒ cập nhật contract; lỗi nguồn ⇒ báo owner trong contract |

### 6.2 Chẩn đoán

| Triệu chứng | Xem | Thường do |
|---|---|---|
| Gold không chạy | Monitor: activity Silver (hoặc trước đó) Failed | một delivery `FAILED` chưa được thay thế hoặc park ([04 §10.6](04-kien-truc.md#106-lỗi-và-chạy-lại)) |
| Gold không chạy dù có dữ liệu mới | Q02 `reason`; Q21 | Silver không có commit thay đổi dữ liệu (dữ liệu trùng) |
| Notebook chạy `full` không mong muốn | Q02 `reason` | `logic_version`, `checkpoint_too_old`, `cdf_unavailable`, `full_reload` |
| Source lỗi ở ingest | Monitor: Copy hoặc `nb_land` Failed; Q01 `error_message` | Connection, quyền đọc nguồn, thư mục, thiếu `modifyTime`, `plan_mismatch` |
| Lỗi Fabric (430, kernel, binding) | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | |

### 6.3 Báo kết quả kiểm chứng

Người chạy kiểm chứng dán vào WORKLOG dưới mục của task:

```text
Kiểm chứng T<x.y> — <yyyy-MM-dd>
Workspace: <…>   Commit đã sync: <sha>
Tham số: p_mode=… p_load_date=… p_scope=… p_full_reload=…
run_id: <…>
Kết quả: PASS | FAIL
Bằng chứng: <Qxx rút gọn>
Lỗi (nếu có): <exception đầu tiên, notebook, cell>
```
