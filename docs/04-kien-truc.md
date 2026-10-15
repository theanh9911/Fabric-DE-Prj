# 04 — Kiến trúc và thiết kế đích

Đặc tả để code theo. Tên bảng, cột, path, tham số ở đây là chuẩn; code phải khớp. Muốn đổi thì sửa file này trước, trong cùng commit với code.

## Mục lục
1. [Sơ đồ](#1-sơ-đồ)
2. [Workspace, item, path](#2-workspace-item-path)
3. [Landing](#3-landing)
4. [Schema `meta`](#4-schema-meta)
5. [Silver](#5-silver)
6. [Gold](#6-gold)
7. [Quarantine](#7-quarantine)
8. [Rule chuẩn hoá và mô hình](#8-rule-chuẩn-hoá-và-mô-hình)
9. [Thuật toán từng worker](#9-thuật-toán-từng-worker)
10. [Điều phối](#10-điều-phối)
11. [Tham số và chế độ chạy](#11-tham-số-và-chế-độ-chạy)
12. [Schema drift](#12-schema-drift)
13. [Data quality checks](#13-data-quality-checks)
14. [Đối soát](#14-đối-soát)
15. [Pipeline, lịch, cảnh báo, bảo trì](#15-pipeline-lịch-cảnh-báo-bảo-trì)
16. [Ngoài phạm vi hiện tại](#16-ngoài-phạm-vi-hiện-tại)

---

## 1. Sơ đồ

```text
CompanyA-Source (chỉ đọc)
  sqldb_erp_wholesale.dbo.*            lh_retail_drop/Files/inbound/**
            │ Connection                          │ Connection
            └────────── Copy activity ────────────┘   (pipeline; một lần mỗi nguồn mỗi lượt)
                              │
lh_platform/Files/_staging/<delivery_id>/
                              │ nb_land: đếm, checksum, so snapshot, mv, ghi manifest
                              ▼
Files/landing/<batch_id>/<source_system>/<entity>/run=<delivery_id>/ + _manifest.json
                              │
              nb_slv_orders, nb_slv_master   (conform theo CONTRACTS + nb_config.SILVER)
                              │
  slv.orders_event ─▶ view slv.v_orders_current
  slv.{customers,products,sales_hierarchy,categories}_history ─▶ view slv.v_<entity>_current
  slv.quarantine
                              │  CDF + checkpoint (meta.table_state)
                              ▼
  gld.dim_date   gld.dim_customer   gld.dim_product   gld.dim_salesman (SCD2)
  gld.fct_order_line  (accumulating snapshot của dòng hàng)
                              │
                     nb_dq (CHECKS + contract quality) → nb_recon (RC)
                              │
  meta.{delivery_log, landed_file, run_log, check_result, table_state}
```

Một lượt (`pl_master_daily`): `nb_plan` → ForEach nguồn (Copy → `nb_land`) → Silver → Gold → DQ → đối soát → `nb_close`. Thứ tự do pipeline quyết định; mỗi notebook tự bỏ qua khi không có gì mới ([§10](#10-điều-phối)).
## 2. Workspace, item, path

| Thứ | Giá trị |
|---|---|
| Workspace nguồn | tham số `p_source_workspace` (mặc định `CompanyA-Source`) |
| ERP | tham số `p_erp_item` (mặc định `sqldb_erp_wholesale`); Copy đọc qua Connection; `nb_plan` tra ID item bằng Fabric REST lúc chạy |
| Drop zone | tham số `p_drop_lakehouse` (mặc định `lh_retail_drop`); Copy đọc qua Connection; `nb_plan` chỉ liệt kê tên file qua `abfss://<ws_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Files/<physicalName>` (ID tra lúc chạy) |
| Workspace platform | `CompanyA-DataPlatform-Dev` (nhánh `dev`), `CompanyA-DataPlatform-Prod` (nhánh `main`); thư mục Git `fabric/platform/` |
| Lakehouse platform | `lh_platform`, schema `meta`, `slv`, `gld` |
| Staging | `Files/_staging/<delivery_id>/` |
| Landing | `Files/landing/<batch_id>/<source_system>/<entity>/run=<delivery_id>/` |
| Manifest | `<thư mục delivery>/_manifest.json` |

Mọi notebook chạy từ pipeline hoặc chạy tay phải gắn `lh_platform` làm default Lakehouse (Data items → Add, bật Auto-Binding in Git). Notebook thư viện (`nb_contracts`, `nb_config`, `nb_std`, `nb_common`) chỉ được `%run`.

---

## 3. Landing

### 3.1 Quy tắc

- Một **delivery** = một source ID trong một lượt = một thư mục `run=<delivery_id>` (`delivery_id` = UUID4 do `nb_plan` sinh).
- **Ingest chỉ bằng Copy activity của pipeline, qua Connection.** Notebook không đọc dữ liệu nguồn (không JDBC, không token, không copy file); `nb_plan` chỉ liệt kê tên file ở drop zone.
- Copy ghi vào `Files/_staging/<delivery_id>/`. `nb_land` đếm, tính checksum, rồi `notebookutils.fs.mv` sang Landing và ghi `_manifest.json` sau cùng. Thư mục Landing có manifest là delivery hợp lệ; thư mục không có manifest (lượt chết giữa `mv` và ghi manifest) bị bỏ qua và nguồn đó được lấy lại ở lượt sau.
- Copy không retry (staging có thể còn file dở). Lỗi ⇒ lượt sau lấy lại; staging cũ bị dọn bởi `nb_maintenance`.
- Không ghi đè, không xoá Landing (L2).
- `batch_id` = `yyyyMMdd` của `p_load_date`.

### 3.2 Nguồn DB

`loadStrategy` `db_incremental` hoặc `db_full_snapshot`.

1. `nb_plan`: dựng `query` ([§3.4](#34-câu-select)) và `staging_path`.
2. Copy: nguồn = SQL Database item (`erp_item_id`) qua Connection, truy vấn `query`; đích = `lh_platform` Files, định dạng Parquet, thư mục `staging_path`.
3. `nb_land` (nhận `rowsRead` của Copy làm `p_copy_rows_read`): đọc Parquet ở staging, tính `rows_landed` và `source_checksum` ([§3.5](#35-checksum)) trong một lần `agg`; `source_count = rowsRead`.
4. `rows_landed = 0` ⇒ xoá staging, `NO_DATA`.
5. `kind: snapshot` và `source_checksum` = checksum của delivery `LOADED` gần nhất (`meta.v_last_snapshot_checksum`) ⇒ xoá staging, `UNCHANGED`.
6. Còn lại ⇒ `mv` sang Landing, ghi manifest, `record_delivery(LANDED)`, `DELIVERED`.

`snapshot_date` của delivery DB snapshot = `p_load_date`.

### 3.3 Nguồn file

`loadStrategy` `file_new_or_changed`.

`nb_plan` (chỉ liệt kê, không đọc nội dung):

1. `list_drop_zone`: tên, `size`, `modifyTime` của file khớp `fileGlob` trong `physicalName`; thiếu `size` hoặc `modifyTime` ⇒ source `FAILED` (lỗi môi trường, không đoán).
2. **Settle:** bỏ file có `modifyTime > run_start_utc − settleMinutes`.
3. **Đã xử lý:** bỏ file có cùng `(source_system, entity, file_name, file_size, modify_time_utc)` trong `meta.landed_file`. File gửi lại cùng tên có `modifyTime` mới ⇒ được xử lý.
4. `kind: snapshot`: chỉ giữ file có ngày `yyyyMMdd` lớn nhất trong tên (regex `(\d{8})`); file không có ngày trong tên ⇒ source `FAILED`. `snapshot_date` = ngày đó. Các file snapshot cũ hơn không vào kế hoạch; `nb_plan` ghi chúng vào `landed_file` với `outcome = 'SUPERSEDED'`.
5. Không còn file ⇒ source không vào kế hoạch; `run_log` `ingest.<source_id>` `NO_DATA`, và nếu có file bị bước 2 loại thì `reason = settle_pending:<số file>` để phân biệt với "chưa có file mới".
6. Còn file ⇒ kế hoạch có `expected_files` (tên, size), `source_folder`, `modified_start = min(modifyTime)`, `modified_end = max(modifyTime) + 1 giây`.

Copy: nguồn = drop zone Lakehouse (`drop_lakehouse_id`) qua Connection, thư mục `source_folder`, Binary, wildcard `fileGlob`, lọc last modified trong [`modified_start`, `modified_end`); đích = `staging_path`, không giữ cấu trúc thư mục con.

`nb_land`:

1. Giữ file trong staging có `(name, size)` khớp `expected_files`; file khác (đã nạp trước đó nhưng rơi vào cửa sổ thời gian) bị xoá khỏi staging. File `expected` thiếu hoặc khác size (nguồn đổi giữa lúc lập kế hoạch và lúc Copy) ⇒ không nạp, `run_log` `reason = plan_mismatch:<tên>`, lượt sau nhặt lại.
2. Đọc CSV ở staging ([§3.6](#36-đọc-csv)): số dòng và checksum **từng file** trong một `groupBy(_source_file)`; tổng ⇒ `rows_landed`, `source_checksum`.
3. `kind: snapshot` và checksum = checksum `LOADED` gần nhất ⇒ xoá staging, `UNCHANGED` (file vẫn ghi `landed_file` với `outcome = 'UNCHANGED'`).
4. Còn lại ⇒ `mv` sang Landing, manifest, `record_delivery(LANDED)`, `record_landed_files` (`outcome = 'LANDED'`), `DELIVERED`.

`source_count` của file = `null`.
### 3.4 Câu SELECT

| `loadStrategy` | Câu lệnh |
|---|---|
| `db_incremental` | `SELECT * FROM <physicalName> WHERE [<wm>] >= '<window_start>' AND [<wm>] < '<window_end>'` |
| `db_full_snapshot` | `SELECT * FROM <physicalName>` |

- `window_start` = watermark (`meta.v_watermark`) − `lookbackMinutes`; chưa có watermark ⇒ `1900-01-01 00:00:00`.
- `lookbackMinutes > 0` làm mỗi lượt đọc lại và ghi lại vào Landing các dòng cuối của cửa sổ trước (Silver dedupe nên số liệu không sai, nhưng Landing phình). Chỉ đặt > 0 khi nguồn thật có commit trễ, và chọn giá trị theo đo đạc (L12). Nguồn `sourceClock: virtual` có cửa sổ chính xác (cận dưới bao gồm, cận trên loại trừ) nên đặt `0`.
- `window_end`: `sourceClock = virtual` ⇒ `p_load_date + 1 ngày` lúc `00:00:00`; `real` ⇒ `run_start_utc` (đổi sang `sourceTimezone`) − `commitLagMinutes`.
- Định dạng chuỗi `yyyy-MM-dd HH:mm:ss`.

### 3.5 Checksum

```text
row_hash = xxhash64(cột theo thứ tự tên, mỗi cột cast string, NULL → '<NULL>')
checksum = sha256(f"{count}|{sum(cast(row_hash AS decimal(38,0)))}")
```
Không phụ thuộc thứ tự dòng; `decimal(38,0)` để tổng không tràn khi bật ANSI.

### 3.6 Đọc CSV

```python
spark.read.option("header", True).option("multiLine", True).option("quote", '"')
     .option("escape", '"').option("mode", "PERMISSIVE").option("inferSchema", False)
     .option("encoding", "UTF-8").csv(paths)
```
Mọi cột là `string`.

### 3.7 Manifest

```json
{
  "manifest_version": 1,
  "delivery_id": "6a0c…", "run_id": "b1f2…", "batch_id": "20260115",
  "source_id": "retail.orders", "source_system": "retail", "entity": "orders",
  "contract_version": "1.0.0", "load_strategy": "file_new_or_changed", "kind": "event",
  "format": "csv",
  "window_start": null, "window_end": null, "snapshot_date": null,
  "source_files": [
    {"name": "orders_20260115.csv", "size": 10423, "modify_time_utc": "2026-01-15T23:10:04Z",
     "rows": 62, "checksum": "9f…"}
  ],
  "rows_landed": 62, "source_count": null, "source_checksum": "4be1…",
  "columns": ["order_no", "customer_code", "…"],
  "delivered_at_utc": "2026-01-16T00:05:11Z"
}
```

| Trường | Ghi chú |
|---|---|
| `format` | `parquet` (DB) / `csv` (file) |
| `window_*` | chỉ `db_incremental`; `null` ở chỗ khác |
| `snapshot_date` | `yyyy-MM-dd`, chỉ `kind: snapshot` |
| `source_files` | chỉ file; `[]` với DB |
| `source_count` | chỉ DB; bằng `rowsRead` của Copy activity |
| `columns` | tên cột đúng như nguồn giao (để phát hiện schema drift) |

---

## 4. Schema `meta`

Tạo bởi `nb_setup_ddl`. Mọi bảng `USING DELTA`. Mỗi bảng chỉ do các notebook nêu ở [§4.6](#46-ai-ghi-bảng-nào) ghi (N9).

### 4.1 `meta.delivery_log`

Một dòng = một sự kiện trạng thái của một delivery:

| `status` | Nghĩa |
|---|---|
| `LANDED` | có manifest; chờ Silver |
| `LOADED` | Silver đã commit |
| `FAILED` | delivery có khuyết tật xác định (thiếu cột bắt buộc); được kiểm lại mỗi lượt và chặn Gold cho tới khi bị thay thế bởi delivery `LOADED` sau đó ([§4.7](#47-view)) hoặc người `park` nó |
| `PARKED` | người vận hành chủ động loại khỏi xử lý, lý do ở `error_message`; chỉ đổi lại bằng `nb_ops_delivery` ([§10.7](#107-thao-tác-của-người-vận-hành)) |

```sql
CREATE TABLE IF NOT EXISTS meta.delivery_log (
  delivery_id STRING NOT NULL, status STRING NOT NULL,
  batch_id STRING NOT NULL, source_system STRING NOT NULL, entity STRING NOT NULL,
  contract_version STRING, load_strategy STRING, kind STRING, landing_path STRING,
  source_files ARRAY<STRING>,
  delivered_at_utc TIMESTAMP, snapshot_date DATE, window_start STRING, window_end STRING,
  rows_landed BIGINT, source_count BIGINT, source_checksum STRING,
  schema_added ARRAY<STRING>, schema_missing ARRAY<STRING>,
  rows_read BIGINT, rows_accepted BIGINT, rows_rejected BIGINT, rows_flagged BIGINT,
  logic_version INT, run_id STRING NOT NULL, event_at_utc TIMESTAMP NOT NULL, error_message STRING
) USING DELTA;
```
Cột Silver (`schema_*`, `rows_read` … `logic_version`) chỉ có ở dòng `LOADED`/`FAILED`.

### 4.2 `meta.landed_file`

```sql
CREATE TABLE IF NOT EXISTS meta.landed_file (
  source_system STRING NOT NULL, entity STRING NOT NULL, file_name STRING NOT NULL,
  file_size BIGINT NOT NULL, modify_time_utc TIMESTAMP NOT NULL,
  row_count BIGINT, file_checksum STRING,
  outcome STRING NOT NULL,          -- LANDED | UNCHANGED | SUPERSEDED
  delivery_id STRING, batch_id STRING NOT NULL, run_id STRING NOT NULL, landed_at_utc TIMESTAMP NOT NULL
) USING DELTA;
```

### 4.3 `meta.run_log`

Một dòng = một sự kiện của một bước trong một lượt.

```sql
CREATE TABLE IF NOT EXISTS meta.run_log (
  run_id STRING NOT NULL, step STRING NOT NULL, layer STRING NOT NULL,
  event STRING NOT NULL, reason STRING, exec_mode STRING,
  run_mode STRING, batch_from STRING, batch_to STRING,
  logic_version INT, input_versions MAP<STRING, BIGINT>, output_version BIGINT,
  rows_in BIGINT, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT,
  rows_rejected BIGINT, rows_flagged BIGINT,
  params STRING, event_at_utc TIMESTAMP NOT NULL, error_message STRING
) USING DELTA;
```

| Cột | Giá trị |
|---|---|
| `step` | `pipeline`, `ingest.<source id>`, tên bảng (`slv.orders_event`, `gld.dim_customer`, …), `dq`, `recon` |
| `layer` | `pipeline`, `ingest`, `slv`, `gld`, `dq`, `recon`, `maint`, `ops` |
| `event` | `STARTED`, `SUCCEEDED`, `FAILED`, `SKIPPED`, `NO_DATA`, `UNCHANGED` |
| `reason` | lý do quyết định của planner ([§10.2](#102-quyết-định-chạy-từng-node)) hoặc lý do ingest |
| `params` | JSON tham số lượt; chỉ ở dòng `step='pipeline', event='STARTED'` |

### 4.4 `meta.check_result`

```sql
CREATE TABLE IF NOT EXISTS meta.check_result (
  run_id STRING NOT NULL, batch_id STRING, check_id STRING NOT NULL,
  source STRING NOT NULL,           -- platform | contract | recon
  check_type STRING, object_name STRING, scope_value STRING,
  expected DECIMAL(38,4), actual DECIMAL(38,4), threshold DECIMAL(38,4),
  severity STRING NOT NULL,         -- critical | warning
  status STRING NOT NULL,           -- PASS | WARN | FAIL
  message STRING, sample STRING, checked_at_utc TIMESTAMP NOT NULL
) USING DELTA;
```

### 4.5 `meta.table_state`

Checkpoint của mỗi bảng Silver/Gold.

```sql
CREATE TABLE IF NOT EXISTS meta.table_state (
  target_table STRING NOT NULL, logic_version INT NOT NULL,
  input_versions MAP<STRING, BIGINT>,   -- version đã xử lý tới (bao gồm) của từng input bảng
  output_version BIGINT NOT NULL, last_run_id STRING NOT NULL,
  last_exec_mode STRING NOT NULL, last_success_at_utc TIMESTAMP NOT NULL
) USING DELTA;
```

### 4.6 Ai ghi bảng nào

| Bảng | Notebook ghi |
|---|---|
| `delivery_log` | `nb_land` (`LANDED`); `nb_slv_orders`, `nb_slv_master` (`LOADED`, `FAILED`); `nb_ops_delivery` (`PARKED`, và `LANDED` khi `release`) |
| `landed_file` | `nb_plan` (`SUPERSEDED`); `nb_land` (`LANDED`, `UNCHANGED`) |
| `run_log` | mỗi notebook ghi các bước của chính nó; `nb_plan` ghi dòng `pipeline STARTED`; `nb_close` ghi dòng kết thúc; `nb_ops_delivery` ghi `ops.*` |
| `check_result` | `nb_dq`, `nb_recon`, `nb_sla_watch` |
| `table_state` | mỗi notebook Silver/Gold ghi đúng một dòng: bảng đích của nó |

Không notebook nào ghi bảng `meta` ngoài danh sách này.
### 4.7 View

```sql
CREATE OR REPLACE VIEW meta.v_delivery_state AS
SELECT * FROM (
  SELECT d.*, row_number() OVER (PARTITION BY delivery_id ORDER BY event_at_utc DESC) AS _rn
  FROM meta.delivery_log d) x
WHERE _rn = 1;

CREATE OR REPLACE VIEW meta.v_pending_delivery AS
SELECT d.* FROM meta.v_delivery_state d
WHERE d.status = 'LANDED'
   OR (d.status = 'FAILED' AND NOT EXISTS (
        SELECT 1 FROM meta.v_delivery_state n
        WHERE n.status = 'LOADED' AND n.source_system = d.source_system AND n.entity = d.entity
          AND n.delivered_at_utc > d.delivered_at_utc
          AND (d.kind = 'snapshot' OR size(d.source_files) = 0
               OR arrays_overlap(n.source_files, d.source_files))));

CREATE OR REPLACE VIEW meta.v_watermark AS
SELECT source_system, entity, max(window_end) AS watermark
FROM meta.v_delivery_state
WHERE status = 'LOADED' AND load_strategy = 'db_incremental'
GROUP BY source_system, entity;

CREATE OR REPLACE VIEW meta.v_last_snapshot_checksum AS
SELECT source_system, entity, max_by(source_checksum, delivered_at_utc) AS source_checksum
FROM meta.v_delivery_state
WHERE status = 'LOADED' AND snapshot_date IS NOT NULL
GROUP BY source_system, entity;

CREATE OR REPLACE VIEW meta.v_run_latest AS
SELECT r.* FROM meta.run_log r
JOIN (SELECT run_id FROM meta.run_log WHERE step = 'pipeline' AND event = 'STARTED'
      ORDER BY event_at_utc DESC LIMIT 1) l ON r.run_id = l.run_id;
```

Delivery `FAILED` được thay thế (không còn trong `v_pending_delivery`) khi có delivery `LOADED` của cùng source, mới hơn, và: nguồn là `snapshot` (snapshot sau thay snapshot trước); hoặc nguồn DB (cửa sổ sau luôn phủ cửa sổ trước vì watermark chỉ tiến theo `LOADED`); hoặc file event có cùng tên file (file gửi lại). File của ngày khác không thay thế được nhau.

---

## 5. Silver

### 5.1 Quy tắc chung

- Bảng tạo bằng DDL với `TBLPROPERTIES (delta.enableChangeDataFeed = true)`.
- Một bảng mỗi entity cho mọi nguồn; cột `source_system` phân biệt nguồn.
- Mã (`*_code`, `order_no`) chuẩn hoá bằng `std_code`.
- Thời gian lưu UTC (`*_utc`).
- Cột kỹ thuật chung:

| Cột | Kiểu | Nghĩa |
|---|---|---|
| `_bk` | STRING | `source_system|<khoá>` đã chuẩn hoá |
| `_record_hash` | STRING | sha256 cột canonical trừ `hash_exclude` ([05 §2.3](05-thu-vien.md#23-silver)) |
| `_batch_id`, `_delivery_id` | STRING | delivery nguồn |
| `_delivered_at_utc` | TIMESTAMP | tie-breaker giữa delivery |
| `_source_file` | STRING | file trong Landing |
| `_row_no` | BIGINT | thứ tự dòng trong delivery |
| `_dq_flags` | ARRAY<STRING> | mã FLAG ([§7](#7-quarantine)) |
| `_run_id` | STRING | |
| `_logic_version` | INT | `logic_version` Silver lúc ghi |
| `_ingested_at_utc` | TIMESTAMP | |

### 5.2 `slv.orders_event`

Grain: một sự kiện trạng thái của một dòng hàng. **Event key:** `(source_system, order_no, product_code, order_status, updated_at_utc)`.

```sql
CREATE TABLE IF NOT EXISTS slv.orders_event (
  source_system STRING NOT NULL, order_no STRING NOT NULL, product_code STRING NOT NULL,
  customer_code STRING, salesman_code STRING, order_date DATE,
  order_status STRING NOT NULL, order_status_raw STRING, status_rank INT NOT NULL,
  quantity DECIMAL(18,2), price DECIMAL(18,2),
  tax_rate DECIMAL(9,6), tax_rate_raw STRING,
  created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP NOT NULL,
  _bk STRING NOT NULL, _event_key STRING NOT NULL, _record_hash STRING NOT NULL,
  _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
  _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

| Cột canonical | Cột nguồn (contract) | Chuẩn hoá | Bắt buộc |
|---|---|---|---|
| `order_no` | `order_no` | `code` | ✔ |
| `product_code` | `product_code` | `code` | ✔ |
| `customer_code` | `customer_code` | `code` | |
| `salesman_code` | `salesman_code` | `code` | |
| `order_date` | `order_date` | `date` | ✔ |
| `order_status` | `order_status` | `dict:ORDER_STATUS` (giữ `order_status_raw`) | ✔ |
| `status_rank` | — | `STATUS_RANK[order_status]` | |
| `quantity` | `quantity` | `decimal` | ✔ |
| `price` | `price` | `decimal` | ✔ |
| `tax_rate` | `tax_rate` | `tax_rate` (giữ `tax_rate_raw`; lỗi ⇒ FLAG) | |
| `created_at_utc` | `created_at` | `ts` | |
| `updated_at_utc` | `updated_at` | `ts` | ✔ |

`order_line_id` (chỉ ERP) không được dùng.

### 5.3 `slv.v_orders_current`

```sql
CREATE OR REPLACE VIEW slv.v_orders_current AS
SELECT * FROM (
  SELECT e.*, row_number() OVER (
           PARTITION BY source_system, order_no, product_code
           ORDER BY updated_at_utc DESC, status_rank DESC, _delivered_at_utc DESC, _row_no DESC) AS _rn
  FROM slv.orders_event e) x
WHERE _rn = 1;
```

Worker Gold **không** đọc view này: nó lấy các sự kiện của khoá bị chạm rồi chọn dòng mới nhất bằng `latest_event` ([05 §4.13](05-thu-vien.md#413-trạng-thái-hiện-hành)), để không tính window trên cả bảng. View dành cho người đọc và đối soát; hai cách phải cho cùng kết quả (test `test_current_matches_view`).

### 5.4 Lịch sử master `slv.<entity>_history`

Grain: một **thay đổi** của một thực thể ở một nguồn. Khoá `(source_system, <key>, _snapshot_date)`. Có dòng khi: thực thể xuất hiện lần đầu (`INSERT`), `_record_hash` khác phiên bản trước (`UPDATE`), thực thể vắng mặt ở snapshot sau (`DELETE`, `is_deleted = true`), xuất hiện lại sau khi bị xoá (`REINSERT`).

Cột thêm cho mọi bảng history: `is_deleted BOOLEAN NOT NULL`, `_snapshot_date DATE NOT NULL`, `_change_type STRING NOT NULL`. Cột kỹ thuật như §5.1 trừ `_event_key`.

```sql
CREATE TABLE IF NOT EXISTS slv.customers_history (
  source_system STRING NOT NULL, customer_code STRING NOT NULL,
  customer_name STRING, country STRING, city STRING, gender STRING, gender_raw STRING, address STRING,
  created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP,
  is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_type STRING NOT NULL,
  _bk STRING NOT NULL, _record_hash STRING NOT NULL,
  _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
  _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS slv.products_history (
  source_system STRING NOT NULL, product_code STRING NOT NULL,
  product_name STRING, brand STRING, brand_raw STRING, category_code STRING,
  created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP,
  is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_type STRING NOT NULL,
  _bk STRING NOT NULL, _record_hash STRING NOT NULL,
  _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
  _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS slv.sales_hierarchy_history (
  source_system STRING NOT NULL, salesman_code STRING NOT NULL,
  salesman_name STRING, division STRING, position STRING, position_raw STRING,
  salesmanager_code STRING, salesmanager_name STRING,
  created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP,
  is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_type STRING NOT NULL,
  _bk STRING NOT NULL, _record_hash STRING NOT NULL,
  _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
  _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS slv.categories_history (
  source_system STRING NOT NULL, category_code STRING NOT NULL,
  category_lvl1 STRING, category_lvl2 STRING, category_lvl3 STRING, category_lvl4 STRING,
  is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_type STRING NOT NULL,
  _bk STRING NOT NULL, _record_hash STRING NOT NULL,
  _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
  _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

| Bảng | Cột canonical ← nguồn (chuẩn hoá) |
|---|---|
| customers | `customer_code` ← `customer_code` (code, bắt buộc); `customer_name` (text); `country`, `city` (title); `gender` (dict:GENDER, giữ raw); `address` (text); `created_at_utc` ← `created_at` (ts); `updated_at_utc` ← `updated_at` (ts) |
| products | `product_code` (code, bắt buộc); `product_name` (text); `brand` (dict:BRAND, giữ raw); `category_code` (code); `created_at_utc`, `updated_at_utc` (ts) |
| sales_hierarchy | `salesman_code` (code, bắt buộc); `salesman_name` (text); `division` (title); `position` (dict:POSITION, giữ raw); `salesmanager_code` (code); `salesmanager_name` (text); `created_at_utc` ← `inserted_at` (ts); `updated_at_utc` ← `updated_at` (ts) |
| categories | `category_code` (code, bắt buộc); `category_lvl1` … `category_lvl4` (text) |

`_record_hash` không tính `created_at_utc`, `updated_at_utc`, cột `*_raw`.

### 5.5 View hiện hành master

Một view mỗi entity (`slv.v_customers_current`, `slv.v_products_current`, `slv.v_sales_hierarchy_current`, `slv.v_categories_current`):

```sql
CREATE OR REPLACE VIEW slv.v_<entity>_current AS
SELECT * FROM (
  SELECT h.*, row_number() OVER (PARTITION BY source_system, <key>
                                 ORDER BY _snapshot_date DESC, _delivered_at_utc DESC) AS _rn
  FROM slv.<entity>_history h) x
WHERE _rn = 1;
```
View trả cả dòng `is_deleted = true`.

---

## 6. Gold

Mọi bảng: DDL tường minh, CDF bật, cột `_run_id STRING NOT NULL`, `_logic_version INT NOT NULL`, `_updated_at_utc TIMESTAMP NOT NULL`. Surrogate key = `xxhash64` tất định của khoá tự nhiên, nên không cần tra cứu khi dựng fact.

### 6.1 `gld.dim_date`

```sql
CREATE TABLE IF NOT EXISTS gld.dim_date (
  date_key INT NOT NULL, date_value DATE NOT NULL,
  year INT, quarter INT, month_key INT, month INT, month_name STRING,
  day INT, day_of_week INT, day_name STRING, iso_week INT, is_weekend BOOLEAN,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
Một dòng mỗi ngày từ `SETTINGS.date_dim_start` tới `SETTINGS.date_dim_end`, cộng dòng `date_key = -1`, `date_value = 1900-01-01` cho ngày không xác định. `day_of_week`: 1 = Thứ Hai … 7 = Chủ nhật.

### 6.2 `gld.dim_customer`

```sql
CREATE TABLE IF NOT EXISTS gld.dim_customer (
  customer_sk BIGINT NOT NULL, customer_code STRING NOT NULL,
  customer_name STRING, country STRING, city STRING, gender STRING, address STRING,
  source_systems STRING NOT NULL,
  is_inferred BOOLEAN NOT NULL, is_deleted BOOLEAN NOT NULL, _dim_hash STRING NOT NULL,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
`customer_sk = xxhash64(customer_code)`. `source_systems`: các nguồn hiện có mã này, sắp xếp, nối bằng `,`. Dòng không xác định: `customer_sk = -1`, `customer_code = '#UNKNOWN'`.

### 6.3 `gld.dim_product`

```sql
CREATE TABLE IF NOT EXISTS gld.dim_product (
  product_sk BIGINT NOT NULL, product_code STRING NOT NULL,
  product_name STRING, brand STRING, category_code STRING, category_path STRING,
  category_l1 STRING, category_l2 STRING, category_l3 STRING, category_l4 STRING,
  source_systems STRING NOT NULL,
  is_inferred BOOLEAN NOT NULL, is_deleted BOOLEAN NOT NULL, _dim_hash STRING NOT NULL,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
`product_sk = xxhash64(product_code)`. Dòng không xác định `-1`, `'#UNKNOWN'`.

### 6.4 `gld.dim_salesman` (SCD2)

```sql
CREATE TABLE IF NOT EXISTS gld.dim_salesman (
  salesman_sk BIGINT NOT NULL, source_system STRING NOT NULL, salesman_code STRING NOT NULL,
  salesman_name STRING, division STRING, position STRING,
  manager_code STRING, manager_name STRING,
  valid_from DATE NOT NULL, valid_to DATE NOT NULL, is_current BOOLEAN NOT NULL,
  is_deleted BOOLEAN NOT NULL, is_inferred BOOLEAN NOT NULL, _scd_hash STRING NOT NULL,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
`salesman_sk = xxhash64(source_system, salesman_code, cast(valid_from as string))`. `manager_name` lấy từ phiên bản của manager có hiệu lực tại `valid_from` (không dùng `salesmanager_name` của nguồn). Dòng không xác định: `salesman_sk = -1`, `source_system = '#'`, `salesman_code = '#UNKNOWN'`, `1900-01-01 → 9999-12-31`.

### 6.5 `gld.fct_order_line` (accumulating snapshot)

Grain: một dòng hàng `(source_system, order_no, product_code)`, mọi trạng thái. Cột mốc thời gian = lần **đầu** dòng hàng đạt trạng thái đó.

```sql
CREATE TABLE IF NOT EXISTS gld.fct_order_line (
  source_system STRING NOT NULL, order_no STRING NOT NULL, product_code STRING NOT NULL,
  customer_code STRING, salesman_code STRING,
  customer_sk BIGINT NOT NULL, product_sk BIGINT NOT NULL, salesman_sk BIGINT NOT NULL,
  order_date_key INT NOT NULL,
  current_status STRING NOT NULL,
  quantity DECIMAL(18,2) NOT NULL, price DECIMAL(18,2) NOT NULL, tax_rate DECIMAL(9,6),
  first_pending_at_utc TIMESTAMP, first_confirmed_at_utc TIMESTAMP, first_shipped_at_utc TIMESTAMP,
  first_delivered_at_utc TIMESTAMP, first_returned_at_utc TIMESTAMP, first_cancelled_at_utc TIMESTAMP,
  last_event_at_utc TIMESTAMP NOT NULL, status_event_count INT NOT NULL,
  _row_hash STRING NOT NULL,
  _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
`quantity`, `price`, `tax_rate`, `customer_code`, `salesman_code`, `order_date_key` lấy từ dòng hiện hành (`v_orders_current`). Cột `first_<status>_at_utc` có cho mọi giá trị trong `ORDER_STATUS`; thêm trạng thái mới vào từ điển ⇒ thêm cột (DDL + `logic_version`).

---

## 7. Quarantine

### 7.1 `slv.quarantine`

```sql
CREATE TABLE IF NOT EXISTS slv.quarantine (
  entity STRING NOT NULL, source_system STRING NOT NULL,
  batch_id STRING NOT NULL, delivery_id STRING NOT NULL, business_key STRING,
  reason STRING NOT NULL, action STRING NOT NULL,     -- REJECT | FLAG
  row_json STRING NOT NULL, run_id STRING NOT NULL, quarantined_at_utc TIMESTAMP NOT NULL,
  resolved_at_utc TIMESTAMP, resolved_run_id STRING
) USING DELTA;

CREATE OR REPLACE VIEW slv.v_quarantine_open AS
SELECT * FROM slv.quarantine WHERE resolved_at_utc IS NULL;
```
Khoá chống ghi trùng: `(entity, delivery_id, business_key, reason, row_json)`.

### 7.2 Mã lý do

| `reason` | `action` | Khi | Ai sửa |
|---|---|---|---|
| `missing_business_key` | REJECT | cột khoá rỗng sau chuẩn hoá | nguồn |
| `invalid_updated_at` | REJECT | `updated_at` không khớp format nào | nguồn hoặc thêm format vào contract |
| `invalid_order_date` | REJECT | `order_date` không khớp format nào | như trên |
| `unknown_order_status` | REJECT | trạng thái không có trong `ORDER_STATUS` | thêm vào từ điển hoặc nguồn |
| `invalid_quantity` | REJECT | không parse được số | nguồn |
| `invalid_price` | REJECT | không parse được số | nguồn |
| `future_order_date` | REJECT | R-ORD-01 | nguồn |
| `invalid_tax_rate` | FLAG | R-STD-06; dòng vào với `tax_rate = NULL` | nguồn hoặc `TAX_WORDS` |
| `ambiguous_date` | FLAG | R-STD-04 | xác nhận với nguồn |
| `negative_quantity`, `negative_price` | FLAG | R-ORD-05; giá trị âm vẫn vào bảng nguyên vẹn | xác nhận với nguồn |
| `unmapped_value` | FLAG | giá trị không có trong từ điển gender/position/brand | từ điển |
| `duplicate_key_in_snapshot` | FLAG | R-MST-01; dòng trùng không vào history | nguồn |
| `self_manager` | FLAG | quản lý trỏ chính mình; `salesmanager_code` đặt NULL | nguồn |
| `missing_required_value`, `invalid_timestamp`, `invalid_date`, `invalid_number`, `unknown_value` | REJECT | mã mặc định khi một cột `required` không có `reject_reason` riêng ([05 §3.1](05-thu-vien.md#31-registry-std)) | nguồn |

- Một dòng có nhiều lỗi REJECT: ghi mã đầu tiên theo thứ tự bảng trên.
- FLAG: dòng vẫn vào bảng đích, mã nằm trong `_dq_flags` và có một dòng quarantine.
- Resolve: khi một delivery sau nạp thành công dòng hợp lệ có cùng `(entity, business_key)`, mọi dòng REJECT đang mở của khoá đó được đặt `resolved_at_utc`, `resolved_run_id`.

---

## 8. Rule chuẩn hoá và mô hình

Notebook của object liệt kê ID rule nó áp dụng ở cell Markdown đầu. Đổi rule ⇒ tăng `logic_version` của object.

| ID | Tầng | Rule |
|---|---|---|
| R-STD-01 | Silver | Sau `trim`, chuỗi rỗng, `NULL`, `null`, `N/A`, `n/a` ⇒ NULL |
| R-STD-02 | Silver | Mã: trim, bỏ mọi khoảng trắng, chữ hoa |
| R-STD-03 | Silver | Thời gian: thử `timestampFormats` của contract theo thứ tự; hiểu theo `sourceTimezone`; lưu UTC |
| R-STD-04 | Silver | Ngày dạng `a/b/yyyy` với `a ≤ 12`, `b ≤ 12`, `a ≠ b` là mơ hồ: lấy format đầu tiên khớp trong contract, gắn FLAG `ambiguous_date`. Nếu EDA (E04) thấy một file trộn hai thứ tự trong cùng file thì không có cách đúng: bắt buộc yêu cầu nguồn chuẩn hoá ISO 8601 trước khi contract `active`. DQ-SLV-07 đo độ lệch giữa ngày sự kiện và ngày trong tên file |
| R-STD-05 | Silver | Trạng thái: khoá = chữ hoa, bỏ ký tự không phải chữ; tra `ORDER_STATUS`; không có ⇒ REJECT |
| R-STD-06 | Silver | Thuế: bỏ `%`; số > 1 thì chia 100; không phải số thì tra `TAX_WORDS`; ngoài [0, 1] hoặc không parse được ⇒ NULL + FLAG |
| R-STD-07 | Silver | Số: chuỗi khớp `^-?\d{1,3}(,\d{3})+(\.\d+)?$` thì bỏ dấu `,`; `try_cast decimal(18,2)` |
| R-STD-08 | Silver | Gender, position, brand: tra từ điển; không có ⇒ giữ `std_title` + FLAG `unmapped_value` |
| R-ORD-01 | Silver | `order_date > to_date(_batch_id) + SETTINGS.future_order_days` ⇒ REJECT `future_order_date` |
| R-ORD-02 | Silver | Trùng event key trong cùng lượt: giữ `_delivered_at_utc` mới nhất, rồi `_row_no` lớn nhất |
| R-ORD-03 | Silver | Trùng event key với dòng đã có: cập nhật chỉ khi delivery mới hơn và `_record_hash` khác |
| R-ORD-05 | Silver | `quantity < 0` hoặc `price < 0` ⇒ giữ nguyên giá trị, FLAG `negative_quantity` / `negative_price`; không REJECT và không sửa dấu (có thể là điều chỉnh hay trả hàng hợp lệ) |
| R-ORD-04 | Silver | Dòng hiện hành của dòng hàng: `updated_at_utc` mới nhất; hoà thì `status_rank` cao hơn, rồi `_delivered_at_utc`, `_row_no` |
| R-MST-01 | Silver | Trùng khoá (contract `primaryKey`) trong một snapshot: giữ `_row_no` nhỏ nhất; dòng còn lại FLAG `duplicate_key_in_snapshot` |
| R-MST-02 | Silver | Khoá có ở snapshot trước, vắng ở snapshot sau ⇒ dòng `DELETE` tại `_snapshot_date` của snapshot sau |
| R-CUS-01 | Gold | Survivorship khách hàng: với từng thuộc tính, lấy giá trị khác NULL theo thứ tự `SETTINGS.source_priority`; chỉ xét dòng hiện hành chưa xoá |
| R-CUS-02 | Gold | `is_deleted = true` khi mọi nguồn có mã đó đều đã xoá |
| R-PRD-01 | Gold | Survivorship sản phẩm như R-CUS-01 |
| R-PRD-02 | Gold | Danh mục: nối `category_code` với `slv.v_categories_current` chưa xoá; `category_l1`–`category_l4` lấy từ `category_lvl1`–`category_lvl4`; `category_path` = các cấp khác NULL nối bằng `/`; không tìm thấy ⇒ `category_path = 'Unknown (pending)'` |
| R-INF-01 | Gold | Mã có trong orders mà không có master ⇒ inferred member: tên `Unknown (pending)`, `is_inferred = true`, cùng công thức SK; khi master tới, dòng được cập nhật tại chỗ |
| R-SLM-01 | Gold | SCD2 theo `salesman_name`, `division`, `position`, `salesmanager_code`, `is_deleted` |
| R-SLM-02 | Gold | Phiên bản đầu tiên của mỗi `(source_system, salesman_code)` có `valid_from = 1900-01-01` |
| R-SLM-03 | Gold | `valid_to` = `valid_from` của phiên bản kế − 1 ngày; phiên bản cuối `9999-12-31` và `is_current = true` |
| R-SLM-04 | Gold | Salesman suy diễn: một phiên bản `1900-01-01 → 9999-12-31`; SK trùng SK phiên bản đầu khi master thật tới |
| R-SLM-05 | Gold | Point-in-time: fact gắn `salesman_sk` theo `(source_system, salesman_code)` và `order_date BETWEEN valid_from AND valid_to`; `salesman_code` NULL ⇒ `-1` |

---

## 9. Thuật toán từng worker

Ký hiệu: `CDF(t)` = `read_changes(t, from, to)` với `from` = version đã lưu trong `meta.table_state.input_versions` và `to` = version hiện tại của `t`; `touched` = tập khoá bị chạm. Mỗi notebook Silver/Gold tự ghi `meta.table_state`, `meta.run_log` (và `meta.delivery_log` với Silver) sau khi ghi dữ liệu thành công.

### 9.1 `nb_plan` và `nb_land`

`nb_plan`:

```text
1. preflight (§11.2); sai ⇒ raise
2. run_id = p_run_id; load_date = p_load_date hoặc hôm qua giờ VN (chỉ khi normal); batch_id
3. log_run(step='pipeline', event=STARTED, params)
4. ids = resolve_source_ids(p_source_workspace, p_erp_item, p_drop_lakehouse)
5. p_mode = rerun ⇒ sources = []
   ngược lại, mỗi contract không retired trong phạm vi:
     DB   ⇒ query (§3.4), staging_path, delivery_id
     file ⇒ danh sách file (§3.3 bước 1–6); lỗi ⇒ log_run FAILED cho source đó và raise sau khi lập xong các source khác
   source không có gì để lấy ⇒ không đưa vào sources, log_run NO_DATA
6. exit {run_id, batch_id, load_date, run_start_utc, check_scope, ids, sources, master_entities}
```

```json
{
  "run_id": "…", "batch_id": "20260115", "load_date": "2026-01-15",
  "run_start_utc": "2026-01-16T00:05:00Z", "check_scope": "daily",
  "ids": {"source_workspace_id": "…", "erp_item_id": "…", "drop_lakehouse_id": "…"},
  "sources": [
    {"source_id": "retail.orders", "source_system": "retail", "entity": "orders",
     "load_strategy": "file_new_or_changed", "kind": "event",
     "delivery_id": "…", "staging_path": "Files/_staging/…",
     "source_folder": "inbound/retail/orders", "file_glob": "*.csv",
     "modified_start": "…", "modified_end": "…",
     "expected_files": [{"name": "orders_20260115.csv", "size": 10423}]},
    {"source_id": "wholesale.orders", "load_strategy": "db_incremental", "kind": "event",
     "delivery_id": "…", "staging_path": "Files/_staging/…", "query": "SELECT * FROM dbo.orders WHERE …"}
  ],
  "master_entities": ["customers", "products", "sales_hierarchy", "categories"]
}
```

`master_entities` = `entity` của các bảng `SILVER` có `kind: snapshot`, `is_active` và có ít nhất một contract không `retired`. `check_scope`: `full` nếu `p_load_date` rơi vào `SETTINGS.full_checks_weekday` hoặc `p_full_reload`, ngược lại `daily`.

`nb_land` (nhận `p_source_json` = một phần tử của `sources`, `p_copy_rows_read`):

```text
1. theo §3.2 (DB) hoặc §3.3 `nb_land` (file) → outcome DELIVERED | NO_DATA | UNCHANGED
2. log_run(step='ingest.<source_id>', layer='ingest', event theo outcome, reason, rows)
3. exit {source_id, outcome, delivery_id, rows_landed}
```
Lỗi ⇒ raise (activity Failed, lượt dừng).
### 9.2 `nb_slv_orders`

```text
0. decide_run (§10.2); SKIP ⇒ ghi run_log và thoát
1. deliveries = delivery cần xử lý theo §10.4
   kiểm schema từng delivery (manifest.columns so với contract): thiếu cột `required` ⇒
   delivery FAILED (schema_missing_required:<cột>), loại khỏi lượt này
2. raw = union(read_landing(d) for d in deliveries còn lại)
3. conformed = spark.sql(conform_sql(contract, SILVER spec))  theo từng source, union   → persist
4. stats theo _delivery_id (một groupBy): rows_read, rows_rejected, rows_flagged
5. rejected = conformed có _reject_reason → quarantine REJECT; flagged → quarantine FLAG
6. accepted = conformed không reject; dedupe theo _event_key (R-ORD-02)
7. full ⇒ overwrite_table ; incremental ⇒ merge_events (R-ORD-03)
8. resolve_quarantine theo _bk của accepted
9. record_delivery LOADED cho delivery đã xử lý, FAILED cho delivery bị loại ở bước 1
10. state_put, log_run
11. đọc lại v_pending_delivery: còn delivery FAILED ⇒ raise (activity Failed, Gold không chạy);
    ngược lại exit_worker
```
### 9.3 `nb_slv_master` (`p_entity`)

```text
0. decide_run (§10.2); SKIP ⇒ ghi run_log và thoát
1. deliveries của các source của entity theo §10.4, sắp theo snapshot_date, delivered_at_utc;
   kiểm schema như §9.2 bước 1
2. snapshots = conform (SQL) rồi read_landing(...); dedupe trong snapshot (R-MST-01)
3. incremental mà có snapshot_date mới ≤ max(_snapshot_date) đã có của cùng source
   ⇒ chuyển full (run_log reason = out_of_order_snapshot)
4. previous = v_<entity>_current (incremental) hoặc rỗng (full)
5. changes = snapshot_changes(snapshots, previous)   (R-MST-02)
6. full ⇒ overwrite_table ; incremental ⇒ merge_insert_only theo (source_system, key, _snapshot_date)
7. record_delivery LOADED / FAILED; state_put, log_run
8. còn delivery FAILED trong v_pending_delivery ⇒ raise; ngược lại exit_worker
```
### 9.4 `nb_gld_dim_date`

Sinh toàn bộ khoảng ngày + dòng `-1`, `overwrite_table`. Quyết định chạy theo `decide_run` ([§10.2](#102-quyết-định-chạy-từng-node)); thực tế chỉ chạy khi `first_run`, `logic_version` hoặc full, ngoài ra SKIP.

### 9.5 `nb_gld_dim_customer`, `nb_gld_dim_product`

```text
0. decide_run (§10.2); SKIP ⇒ ghi run_log và thoát
1. cur = v_<entity>_current where not is_deleted
2. survivor theo R-CUS-01 / R-PRD-01 (first_value ignore nulls theo thứ tự nguồn); product thêm R-PRD-02
3. codes = full ⇒ distinct code từ v_orders_current ; incremental ⇒ distinct code từ CDF(slv.orders_event)
4. inferred = codes − survivor − inferred đã có trong dim
5. src = survivor ∪ inferred mới ∪ inferred đã có mà vẫn chưa có master ∪ dòng -1
6. sync_table(keys=[<x>_sk], hash=_dim_hash, delete_missing=True)
7. state_put, log_run, exit_worker
```

### 9.6 `nb_gld_dim_salesman`

```text
0. decide_run (§10.2); SKIP ⇒ ghi run_log và thoát
1. h = slv.sales_hierarchy_history (toàn bộ)
2. mỗi dòng là một mốc; bỏ mốc có _scd_hash trùng mốc trước (R-SLM-01)
3. valid_from = _snapshot_date; mốc đầu ⇒ 1900-01-01 (R-SLM-02); valid_to, is_current (R-SLM-03)
4. manager_name: nối chính bảng phiên bản theo (source_system, manager_code) và valid_from trong hiệu lực
5. inferred cho (source_system, salesman_code) có trong orders mà không có history (R-SLM-04)
6. sync_table(keys=[salesman_sk], hash=_scd_hash, delete_missing=True)   ⇒ CDF chỉ chứa phiên bản đổi
7. state_put, log_run, exit_worker
```

### 9.7 `nb_gld_fct_order_line`

```text
0. decide_run (§10.2); SKIP ⇒ ghi run_log và thoát
1. touched (source_system, order_no, product_code):
     full ⇒ mọi (source_system, order_no, product_code) trong slv.orders_event
     incremental ⇒ khoá trong CDF(slv.orders_event)
                  ∪ khoá của slv.orders_event có (source_system, salesman_code) trong CDF(gld.dim_salesman)
2. events = slv.orders_event ⋈ touched        (semi-join, không đi qua view)
3. cur = latest_event(events)                 (R-ORD-04)
   milestones = events: min(updated_at_utc) theo order_status, max(updated_at_utc), count(*)
4. SK: customer_sk, product_sk = xxhash64(code) hoặc -1; salesman_sk theo R-SLM-05
5. _row_hash trên mọi cột nghiệp vụ
6. full ⇒ overwrite_table ; incremental ⇒ merge_upsert(keys=grain, hash=_row_hash)
7. state_put, log_run, exit_worker
```

---

## 10. Điều phối

### 10.1 Pipeline `pl_master_daily`

Pipeline là bộ điều phối: thứ tự do `dependsOn`; activity lỗi thì mọi activity phụ thuộc không chạy.

| # | Activity | Loại | Chạy sau | Nội dung |
|---|---|---|---|---|
| 1 | `plan` | Notebook `nb_plan` | — | preflight, lập kế hoạch ingest ([§9.1](#91-nb_plan-và-nb_land)) |
| 2 | `for_each_source` | ForEach tuần tự trên `sources` của `plan` | `plan` | mỗi phần tử: If `load_strategy` bắt đầu bằng `db_` thì `copy_db` → `land_db`, ngược lại `copy_file` → `land_file` |
| 3 | `silver_orders` | Notebook `nb_slv_orders` | `for_each_source` | |
| 4 | `for_each_master` | ForEach tuần tự trên `master_entities` của `plan` | `silver_orders` | Notebook `nb_slv_master` với `p_entity = @item()` |
| 5 | `gld_dim_date` | Notebook `nb_gld_dim_date` | `for_each_master` | |
| 6 | `gld_dim_customer` | Notebook `nb_gld_dim_customer` | `gld_dim_date` | |
| 7 | `gld_dim_product` | Notebook `nb_gld_dim_product` | `gld_dim_customer` | |
| 8 | `gld_dim_salesman` | Notebook `nb_gld_dim_salesman` | `gld_dim_product` | |
| 9 | `gld_fct_order_line` | Notebook `nb_gld_fct_order_line` | `gld_dim_salesman` | |
| 10 | `dq` | Notebook `nb_dq` | `gld_fct_order_line` | |
| 11 | `recon` | Notebook `nb_recon` | `dq` | |
| 12 | `close_ok` | Notebook `nb_close` (`p_status = SUCCEEDED`) | `recon` thành công | |
| 13 | `close_failed`, `alert` | Notebook `nb_close` (`p_status = FAILED`, `p_error`) rồi activity cảnh báo | bất kỳ activity ở trên lỗi | |

- Tham số của mọi Notebook activity: [§10.3](#103-hợp-đồng-worker); `p_run_id = @pipeline().RunId`.
- Mọi activity: `retry: 0`, `timeout: 0.02:00:00`.
- Copy activity ([§3.2](#32-nguồn-db), [§3.3](#33-nguồn-file)): ID workspace/item lấy từ `ids` trong output của `plan`.
- Notebook mới cho một bảng Gold = thêm một activity vào chuỗi 5–9 (thứ tự theo phụ thuộc). Nguồn mới không cần sửa pipeline (L9).
- Cú pháp nhánh lỗi 13 xác minh ở spike K12.

### 10.2 Quyết định chạy từng node

Mỗi notebook Silver/Gold gọi `decide_run()` ở đầu. Hàm thuần; điều kiện xét từ trên xuống, điều kiện đúng đầu tiên quyết định:

| # | Điều kiện | Kết quả | `exec_mode` | `reason` |
|---|---|---|---|---|
| 1 | object `is_active = False` | SKIP | | `inactive` |
| 2 | `p_full_reload` và (`p_scope = ALL` hoặc chứa bảng này) | RUN | full | `full_reload` |
| 3 | chưa có dòng `meta.table_state` | RUN | full | `first_run` |
| 4 | `logic_version` cấu hình ≠ `table_state.logic_version` | RUN | full | `logic_version` |
| 5 | có input `cdf` và `last_success_at_utc` cũ hơn `SETTINGS.cdf_max_age_days` | RUN | full | `checkpoint_too_old` |
| 6 | Silver có delivery cần xử lý ([§10.4](#104-chọn-delivery-cho-silver)) | RUN | incremental | `pending_deliveries` |
| 7 | Gold có input `full`/`cdf` với commit thay đổi dữ liệu sau version đã lưu | RUN | incremental | `input_changed` |
| 8 | còn lại | SKIP | | `up_to_date` |

- "Commit thay đổi dữ liệu" (`data_changed_since`): `operation` thuộc `WRITE`, `MERGE`, `DELETE`, `UPDATE`, `CREATE OR REPLACE TABLE AS SELECT`, `REPLACE TABLE AS SELECT` và `operationMetrics` có số dòng thay đổi > 0. `OPTIMIZE`, `VACUUM`, `SET TBLPROPERTIES` không tính.
- Đọc CDF lỗi `CdfUnavailable` (lịch sử đã bị `VACUUM`) ⇒ chạy `full`, `reason = cdf_unavailable`.
- Bảng cha vừa dựng lại (overwrite) thì CDF của nó ghi xoá và chèn mọi dòng; bảng con thấy qua CDF và tự tính lại đầy đủ, không cần cờ riêng.
- SKIP ⇒ ghi `run_log` (`event = SKIPPED`, `reason`) và thoát `status = SKIPPED`.
- Input kiểu `full`: đọc toàn bảng nhưng chỉ chạy khi bảng đó có thay đổi dữ liệu; `cdf`: đọc thay đổi từ checkpoint; `dep`: chỉ ràng buộc thứ tự (do pipeline), không làm node chạy.

Input theo bảng ([05 §2.4](05-thu-vien.md#24-gold)):

| Bảng | Input |
|---|---|
| `gld.dim_date` | — |
| `gld.dim_customer` | `slv.customers_history` (full), `slv.orders_event` (cdf) |
| `gld.dim_product` | `slv.products_history` (full), `slv.categories_history` (full), `slv.orders_event` (cdf) |
| `gld.dim_salesman` | `slv.sales_hierarchy_history` (full), `slv.orders_event` (cdf) |
| `gld.fct_order_line` | `slv.orders_event` (cdf), `gld.dim_salesman` (cdf), `gld.dim_customer`, `gld.dim_product`, `gld.dim_date` (dep) |

### 10.3 Hợp đồng worker

Tham số (notebook bỏ qua tham số nó không dùng; mọi tham số có mặc định):

| Tham số | Notebook | Nghĩa |
|---|---|---|
| `p_run_id` | mọi notebook | `@pipeline().RunId` |
| `p_load_date`, `p_load_date_to` | `nb_plan`, `nb_slv_orders`, `nb_slv_master` | khoảng batch; Silver nhận `load_date` đã chuẩn hoá từ output của `plan` |
| `p_mode` | `nb_plan`, `nb_slv_orders`, `nb_slv_master` | `normal` / `rerun` |
| `p_scope` | `nb_plan`, mọi notebook Silver/Gold | [§11.1](#111-tham-số-pl_master_daily) |
| `p_full_reload` | `nb_plan`, mọi notebook Silver/Gold | |
| `p_entity` | `nb_slv_master` | `customers` / `products` / `sales_hierarchy` / `categories` |
| `p_source_json` | `nb_land` | một phần tử của `sources` (`@string(item())`) |
| `p_copy_rows_read` | `nb_land` | `rowsRead` của Copy (chỉ DB) |
| `p_batch_id`, `p_check_scope` | `nb_dq`, `nb_recon` | `batch_id`, `check_scope` từ `plan` |
| `p_status`, `p_error` | `nb_close` | `SUCCEEDED` / `FAILED`; thông điệp lỗi |
| `p_source_workspace`, `p_erp_item`, `p_drop_lakehouse` | `nb_plan` | |

Kết quả Silver/Gold (`exit_worker(worker_result(...))`):

```json
{
  "contract": 1, "status": "SUCCEEDED", "target": "gld.fct_order_line", "exec_mode": "incremental",
  "reason": "input_changed", "rows_in": 120, "rows_inserted": 10, "rows_updated": 3,
  "rows_deleted": 0, "rows_rejected": 2, "rows_flagged": 5, "output_version": 42
}
```

`status`: `SUCCEEDED`, `NO_DATA` hoặc `SKIPPED`. Lỗi ⇒ notebook raise (activity Failed). Trước khi thoát, notebook đã ghi `meta.table_state` (Silver/Gold), `meta.delivery_log` (Silver) và `meta.run_log` của chính nó. Kết quả `nb_plan`, `nb_land` theo §9.1.

### 10.4 Chọn delivery cho Silver

Notebook tự đọc `meta`:

| Chế độ | Delivery được xử lý |
|---|---|
| `normal` | mọi delivery trong `meta.v_pending_delivery` của các source thuộc bảng, mọi batch |
| `rerun` | mọi delivery có `batch_id` trong `[p_load_date, p_load_date_to]` của các source thuộc bảng, trừ `PARKED` |
| `full` | mọi delivery có trong `meta.v_delivery_state` của các source thuộc bảng, trừ `PARKED` |

### 10.5 Chạy chồng

1. `pl_master_daily`, `pl_backfill` đặt `"concurrency": 1`.
2. Mọi ghi idempotent nên hai lượt vô tình chồng nhau không làm sai số liệu; hậu quả tệ nhất là một nguồn bị lấy hai lần (hai delivery, Silver dedupe, DQ-ING-01 cảnh báo).
3. Không có bảng khoá. Nếu spike K06 cho thấy `concurrency` không có hiệu lực, ghi vào [DECISIONS](DECISIONS.md) và xem lại.

### 10.6 Lỗi và chạy lại

- Activity lỗi ⇒ mọi activity phụ thuộc không chạy, lượt FAILED, `close_failed` ghi `run_log` và gửi cảnh báo. Dừng cả chuỗi là có chủ đích: Gold không tính trên dữ liệu thiếu.
- Chạy lại: **Rerun from failed activity** trên Monitor, hoặc chạy lại pipeline với cùng `p_load_date`. Ingest không làm lại việc đã xong (watermark chỉ tiến theo `LOADED`; file đã có trong `landed_file`); Silver chỉ lấy delivery chờ; Gold tự bỏ qua nếu checkpoint đã mới.
- Notebook commit xong nhưng chết trước khi ghi `table_state`/log ⇒ lượt sau xử lý lại cùng phạm vi; an toàn vì mọi cách ghi idempotent.
- Delivery có khuyết tật xác định ⇒ dòng `delivery_log` `FAILED`; các delivery hợp lệ cùng lượt vẫn `LOADED` và `table_state` của bảng Silver vẫn được cập nhật. Sau khi ghi, notebook đọc lại `meta.v_pending_delivery`: còn delivery `FAILED` ⇒ raise (activity Failed ⇒ Gold không chạy: **chặn Gold thay vì tính trên dữ liệu thiếu**); không còn ⇒ thành công. Nguồn gửi lại bản đúng thì delivery hỏng cũ tự được thay thế ([§4.7](#47-view)), kể cả ngay trong lượt đó. Nguồn không gửi lại được mà chấp nhận bỏ ⇒ người `park` ([§10.7](#107-thao-tác-của-người-vận-hành)).
- Copy lỗi: không retry; lượt sau lấy lại.
- Watermark chỉ tiến theo dòng `LOADED` (L4).

### 10.7 Thao tác của người vận hành

`nb_ops_delivery` là đường duy nhất để người đổi trạng thái delivery. Chạy tay, không có pipeline. Chỉ cần khi nguồn không gửi lại được bản đúng; khi nguồn gửi lại thì delivery hỏng cũ tự được thay thế ([§4.7](#47-view)).

| Tham số | Giá trị |
|---|---|
| `p_action` | `park` / `release` |
| `p_delivery_id` | một hoặc nhiều ID, cách nhau dấu phẩy |
| `p_reason` | bắt buộc với `park`; ghi vào `delivery_log.error_message` |
| `p_operator` | bắt buộc; tên người chạy, ghi vào `run_log.params` |
| `p_confirm` | phải bằng `CONFIRM` |

- `park`: chỉ nhận delivery đang `FAILED` hoặc `LANDED`; ghi dòng `PARKED`. Delivery `LOADED` không park được (dữ liệu đã vào Silver; muốn loại phải dựng lại bằng `full`).
- `release`: `PARKED` ⇒ `LANDED`; lượt `normal` kế tiếp xử lý lại.
- Mỗi thao tác ghi một dòng `run_log` (`step = ops.<action>`, `layer = ops`, `params` có ID, lý do, người chạy). Landing không bị đụng (L2).
- Không có khoá: đây là thao tác hiếm, do người chạy. Quy tắc vận hành: không chạy khi lượt đang chạy, và báo cho nhóm khi chạy.
- Delivery `PARKED` là dữ liệu bị loại có chủ đích: DQ-OPS-03 cảnh báo mỗi lượt cho tới khi `release`, nên không bị quên.

### 10.8 Trình tự một lượt

```text
plan              preflight → run_log pipeline STARTED → lập kế hoạch ingest
for_each_source   Copy → nb_land      (run_log ingest.<source>, delivery_log LANDED, landed_file)
silver_orders     decide_run → [SKIP | RUN] → delivery_log LOADED/FAILED → table_state → run_log
for_each_master   như trên, mỗi entity
gld_*             decide_run → [SKIP | RUN: CDF → ghi] → table_state → run_log
dq → recon        check_result; critical FAIL ⇒ raise
close_ok          run_log pipeline SUCCEEDED
(lỗi bất kỳ)      close_failed → run_log pipeline FAILED → cảnh báo
```

Thông tin lượt lỗi ở đâu: Monitor (activity nào Failed), `meta.v_run_latest` (từng bước, `reason`), `meta.check_result`.

### 10.9 Notebook kiểm tra và vận hành

| Notebook | Tham số | Kết quả |
|---|---|---|
| `nb_dq` | `p_run_id`, `p_batch_id`, `p_check_scope` | đọc delivery và bảng của lượt từ `meta`; ghi `meta.check_result`; có critical FAIL ⇒ raise sau khi ghi hết; ngược lại `{"contract": 1, "status": "SUCCEEDED", "checks": n, "warnings": n}` |
| `nb_recon` | như `nb_dq` | như `nb_dq`; RC-01 đến RC-04 theo delivery của lượt; RC-05 trên toàn bảng |
| `nb_close` | `p_run_id`, `p_status`, `p_error` | ghi `run_log` (`step = pipeline`, `event = p_status`, `error_message = p_error`) |
| `nb_sla_watch` | `p_load_date` (trống ⇒ hôm qua giờ VN) | ghi DQ-OPS-01; `{"late_sources": [{"source_id", "owner", "support_channel"}]}` |
| `nb_maintenance` | không | ghi `run_log` (`layer = maint`); `{"optimized": [...], "vacuumed": [...]}` |
| `nb_setup_ddl` | `p_reset` (mặc định `False`), `p_confirm` | tạo object (idempotent); `p_reset = True` và `p_confirm = "RESET"` ⇒ xoá mọi object trong `meta`, `slv`, `gld` và `Files/landing`, `Files/_staging`; chỉ dùng ở Dev |

---
## 11. Tham số và chế độ chạy

### 11.1 Tham số `pl_master_daily`

| Tham số | Kiểu | Mặc định | Giá trị |
|---|---|---|---|
| `p_load_date` | string | `""` ⇒ hôm qua theo `SETTINGS.business_timezone` (chỉ khi `normal`) | `yyyy-MM-dd` |
| `p_load_date_to` | string | `""` ⇒ bằng `p_load_date` | `yyyy-MM-dd` |
| `p_mode` | string | `normal` | `normal`, `rerun` |
| `p_scope` | string | `ALL` | `ALL` hoặc danh sách cách nhau dấu phẩy gồm source ID và/hoặc tên bảng Silver/Gold |
| `p_full_reload` | bool | `false` | |
| `p_source_workspace` | string | `CompanyA-Source` | |
| `p_erp_item` | string | `sqldb_erp_wholesale` | |
| `p_drop_lakehouse` | string | `lh_retail_drop` | |

`p_scope`: source ID ⇒ chỉ ingest source đó (Silver, Gold vẫn chạy theo checkpoint). Tên bảng ⇒ chỉ có nghĩa với `p_full_reload` (bảng nào được dựng lại). `dq` và `recon` luôn chạy.
### 11.2 Kiểm tra trước khi chạy

Chạy trong `nb_plan` trước mọi việc khác. Sai ⇒ `plan` Failed, các activity sau không chạy, `close_failed` ghi lượt FAILED.

| Mã | Điều kiện sai |
|---|---|
| PF01 | `p_mode` ∉ {`normal`, `rerun`} |
| PF02 | ngày sai định dạng; `p_load_date_to < p_load_date`; có `p_load_date_to` mà không có `p_load_date` |
| PF03 | `p_mode = rerun`, `p_full_reload = false` mà không có `p_load_date` |
| PF04 | `p_full_reload = true` mà `p_mode ≠ rerun` |
| PF05 | `p_full_reload = true` mà có `p_load_date` (full luôn là toàn bộ lịch sử) |
| PF06 | `p_load_date_to` có giá trị khi `p_mode = normal` |
| PF07 | phần tử `p_scope` không phải source ID của contract hay bảng trong `SILVER`/`GOLD` |
| PF08 | `validate_config()` lỗi |

### 11.3 Chế độ

| `p_mode` | `p_full_reload` | Ingest | Silver | Gold | Dùng khi |
|---|---|---|---|---|---|
| `normal` | false | source trong phạm vi, batch `p_load_date` | delivery đang chờ, mọi batch | node có input đổi | hằng ngày; sau lỗi |
| `rerun` | false | không | delivery có batch trong khoảng ngày (idempotent) | node có input đổi | nạp lại batch cụ thể mà không đọc nguồn |
| `rerun` | true | không | bảng Silver trong phạm vi: dựng lại từ toàn bộ Landing | bảng Gold trong phạm vi: full; bảng con của bảng vừa dựng lại tự tính lại đầy đủ qua CDF | sửa dữ liệu hỏng; dựng lại sau khi xoá bảng |

`logic_version` đổi ⇒ object đó chạy `full` ở lượt `normal` kế tiếp; bảng con tự tính lại đầy đủ qua CDF.

---

## 12. Schema drift

So `manifest.columns` với `properties[].name` của contract (Silver, khi conform):

| Hiện tượng | Xử lý |
|---|---|
| Có cột không có trong contract | Landing giữ nguyên; Silver bỏ qua cột; `delivery_log.schema_added`; DQ-SLV-06 WARN |
| Thiếu cột `required: false` | cột canonical tương ứng NULL; `schema_missing`; DQ-SLV-06 WARN |
| Thiếu cột `required: true` | delivery `FAILED` (`schema_missing_required:<cột>`) và không được xử lý; delivery khác vẫn nạp; node Silver FAILED chặn Gold cho tới khi nguồn gửi lại bản đúng hoặc người `park` ([§10.6](#106-lỗi-và-chạy-lại)) |
| Đổi kiểu | Silver luôn đọc chuỗi rồi parse ⇒ giá trị không parse được bị REJECT/FLAG theo §7 |

Đưa cột mới vào dùng: bump MINOR contract ⇒ thêm vào `SILVER.columns` + DDL `ALTER TABLE ADD COLUMNS` trong `nb_setup_ddl` + tăng `logic_version` Silver ([03 §5](03-contract.md#5-phiên-bản-và-quy-trình-đổi)).

---

## 13. Data quality checks

Ba nguồn check, cùng ghi `meta.check_result`:
- **Platform**: khai báo ở `nb_config.CHECKS`.
- **Contract**: `quality[]` của contract, chạy trên mỗi delivery của lượt ([03 §3.5](03-contract.md#35-quality)).
- **Đối soát**: [§14](#14-đối-soát).

`scope`: `touched` (chỉ dòng có `_run_id` của lượt), `full` (cả bảng; chạy vào ngày `SETTINGS.full_checks_weekday` theo `p_load_date` và mọi lượt có `p_full_reload`; `nb_plan` quyết định và truyền `check_scope`), `run` (theo dữ liệu `meta` của lượt).

| ID | Đối tượng | Loại | Điều kiện | Severity | Scope |
|---|---|---|---|---|---|
| DQ-SLV-01 | `slv.orders_event` | `unique` | `_event_key` | critical | touched |
| DQ-SLV-02 | `slv.orders_event` | `not_null` | `order_no, product_code, order_status, order_date, updated_at_utc` | critical | touched |
| DQ-SLV-03 | `slv.*_history` (4) | `unique` | `source_system, <key>, _snapshot_date` | critical | touched |
| DQ-SLV-04 | delivery của lượt | `quarantine_rate` | `rows_rejected / rows_read` so `quarantineWarnPct` / `quarantineFailPct` của contract | warning / critical | run |
| DQ-SLV-05 | `slv.orders_event` | `flag_count` | dòng có `ambiguous_date`, `negative_quantity`, `negative_price` | warning (> 0) | touched |
| DQ-SLV-06 | delivery của lượt | `schema_drift` | `schema_added` hoặc `schema_missing` khác rỗng | warning | run |
| DQ-SLV-07 | `slv.orders_event` | `event_date_vs_file` | dòng từ file `orders_<yyyyMMdd>.csv` có ngày của `updated_at_utc` (múi giờ nguồn) lệch tên file hơn 1 ngày | warning (> 0) | touched |
| DQ-GLD-01 | `gld.dim_*` (4) | `unique` | SK | critical | full |
| DQ-GLD-02 | `gld.dim_customer`, `gld.dim_product` | `unique` | khoá tự nhiên | critical | full |
| DQ-GLD-03 | `gld.dim_salesman` | `scd2_one_current` | mỗi `(source_system, salesman_code)` đúng một `is_current` | critical | full |
| DQ-GLD-04 | `gld.dim_salesman` | `scd2_contiguous` | không chồng lấn, không hở giữa các phiên bản | critical | full |
| DQ-GLD-05 | `gld.fct_order_line` | `unique` | grain | critical | touched |
| DQ-GLD-06 | `gld.fct_order_line` | `not_null` | SK, `order_date_key`, `current_status`, `quantity`, `price` | critical | touched |
| DQ-GLD-07 | `gld.fct_order_line` | `unknown_member_rate` | % dòng có SK = −1 hoặc trỏ dòng `is_inferred` | warning (> 1%) | touched |
| DQ-GLD-08 | `gld.dim_*` (4) | `sk_collision` | một SK ứng với hai khoá tự nhiên | critical | full |
| DQ-ING-01 | `meta.landed_file` | `duplicate_file` | file của lượt có `file_checksum` trùng file khác tên đã nạp | warning | run |
| DQ-OPS-01 | `meta.delivery_log` | `freshness` | contract `daily` chưa có delivery `LOADED` cho batch sau `availableByLocalTime` | warning | `nb_sla_watch` |
| DQ-OPS-03 | `meta.delivery_log` | `parked_delivery` | số delivery đang `PARKED` | warning (> 0) | run |

Critical FAIL ⇒ `nb_dq` ghi hết mọi check rồi raise với danh sách check ID ⇒ lượt FAILED.

## 14. Đối soát

Hai số của mỗi phép so được tính từ hai nơi độc lập (N8). Chạy bởi `nb_recon` cho phần của lượt.

| ID | So sánh | Số A | Số B | Severity |
|---|---|---|---|---|
| RC-01 | nguồn ↔ Landing (DB) | `source_count`: `rowsRead` do Copy activity báo | `rows_landed`: đếm lại Parquet ở Landing | `db_incremental` critical; `db_full_snapshot` warning |
| RC-02 | Landing ↔ Silver, orders | dòng đọc lại từ Landing của delivery, conform lại, không REJECT ⇒ tập `_event_key` | anti-join với `slv.orders_event` ⇒ số khoá thiếu, phải = 0 | critical |
| RC-03 | Landing ↔ quarantine | số dòng REJECT khi conform lại delivery | số dòng `action = REJECT` của delivery trong `slv.quarantine` | critical |
| RC-04 | Landing ↔ Silver, master | snapshot mới nhất của lượt (conform lại, dedupe) | `v_<entity>_current` của source đó: số khoá lệch `_record_hash` + số khoá có ở current mà không ở snapshot nhưng chưa `is_deleted`, phải = 0 | critical |
| RC-05 | Silver ↔ Gold | `count(*)`, `sum(quantity * price)` của `slv.v_orders_current` | cùng phép tính trên `gld.fct_order_line` | critical |
| RC-06 | (Dev) simulator ↔ Landing | `sim_release_log.rows_released` theo cửa sổ | `rows_landed` theo batch | warning; chỉ `nb_test_e2e` |

---

## 15. Pipeline, lịch, cảnh báo, bảo trì

| Pipeline | Activity | Tham số | Lịch (Prod, giờ VN) |
|---|---|---|---|
| `pl_master_daily` | 13 activity theo [§10.1](#101-pipeline-pl_master_daily); nhánh lỗi → `nb_close` + cảnh báo | §11.1 | 06:30 hằng ngày |
| `pl_backfill` | ForEach tuần tự từng ngày `p_start_date..p_end_date` → Invoke pipeline `pl_master_daily` (`p_mode = normal`, `p_load_date = ngày`) | `p_start_date`, `p_end_date`, 3 tham số nguồn | chạy tay |
| `pl_sla_watch` | `check_sla` → `nb_sla_watch`; If Condition: `@greater(length(json(activity('check_sla').output.result.exitValue).late_sources), 0)` → activity cảnh báo | `p_load_date` | 07:30 hằng ngày |
| `pl_maintenance` | `run_maintenance` → `nb_maintenance` | — | Chủ nhật 03:00 |

- `concurrency: 1` cho `pl_master_daily`, `pl_backfill`.
- Notebook activity trỏ `notebookId` = `logicalId` trong `.platform` của notebook; `workspaceId = 00000000-0000-0000-0000-000000000000` (Git sync tự gắn).
- Activity cảnh báo: activity Teams có sẵn của Data pipeline, connection do người cấu hình trên workspace; nội dung gồm tên pipeline, `@pipeline().RunId`, lỗi, owner và support channel của source (với SLA). Không có secret trong Git.
- Dev: không đặt lịch; người chạy simulator, **chờ hơn `settleMinutes`** (file vừa ghi bị settle bỏ qua; lượt đó ghi `NO_DATA` với `reason = settle_pending:<n>`), rồi chạy `pl_master_daily` với `p_load_date` = ngày giả lập. Không có tham số bỏ settle: kịch bản `partial_file` cần settle hoạt động giống Prod.

`nb_maintenance`:

| Việc | Đối tượng |
|---|---|
| `OPTIMIZE` | mọi bảng `slv`, `gld`, `meta` |
| `VACUUM … RETAIN 720 HOURS` | mọi bảng `slv`, `gld` (30 ngày > `cdf_max_age_days` = 25) |
| Xoá thư mục staging cũ hơn 1 ngày | `Files/_staging/*` |
| Không xoá | Landing (L2), `meta` dưới 30 ngày |

## 16. Ngoài phạm vi hiện tại

Chưa thiết kế, không code cho tới khi có task riêng:

- Lớp KPI: ghi nhận doanh thu, tiền thuế, lead time, bảng tổng hợp theo tháng, khoá kỳ kế toán.
- Semantic model và report (khi có: tắt tự đồng bộ Direct Lake, refresh từ `pl_master_daily` chỉ sau khi `recon` thành công — N7).
- Cây quản lý nhiều cấp (bridge hierarchy).
- Chạy song song nhiều notebook, partition/clustering bảng.
