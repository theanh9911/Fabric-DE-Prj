# 05 — Thư viện dùng chung, cấu hình, mẫu file

Worker phải dùng các hàm ở đây cho mọi thao tác đọc Landing, ghi Delta, CDF, quarantine (W3 trong [CLAUDE.md](../CLAUDE.md)). Rule nghiệp vụ không nằm ở đây mà nằm trong notebook của object.

## Mục lục
1. [Thứ tự import](#1-thứ-tự-import)
2. [`nb_config`](#2-nb_config)
3. [`nb_std`](#3-nb_std)
4. [`nb_common`](#4-nb_common)
5. [Mẫu file](#5-mẫu-file)
6. [Công cụ local](#6-công-cụ-local)

---

## 1. Thứ tự import

Mỗi notebook (trừ chính các notebook thư viện) có các cell theo đúng thứ tự, mỗi `%run` một cell riêng:

```text
PARAMETERS CELL
%run nb_contracts      ← CONTRACTS (sinh từ YAML)
%run nb_config         ← SETTINGS, từ điển, SILVER, GOLD, CHECKS, validate_config()
%run nb_std            ← hàm chuẩn hoá cột
%run nb_common         ← hạ tầng; đặt cấu hình phiên Spark
CELL: NB_NAME = "<tên notebook>"
CELL: code
```

`nb_common` là nơi duy nhất đặt cấu hình phiên:

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
```

---

## 2. `nb_config`

Chỉ chứa định nghĩa và `validate_config()`; không đọc/ghi dữ liệu. Mỗi khoá dưới đây đều có nơi đọc; thêm khoá mới phải kèm nơi dùng.

### 2.1 `SETTINGS`

| Khoá | Giá trị | Ai đọc |
|---|---|---|
| `business_timezone` | `"Asia/Ho_Chi_Minh"` | `default_load_date`, `nb_sla_watch` |
| `date_dim_start`, `date_dim_end` | `"2020-01-01"`, `"2035-12-31"` | `nb_gld_dim_date` |
| `future_order_days` | `1` | `conform` (R-ORD-01) |
| `source_priority` | `["wholesale", "retail"]` | survivorship (R-CUS-01, R-PRD-01) |
| `full_checks_weekday` | `6` (Chủ nhật, Thứ Hai = 0) | `nb_dq` |
| `cdf_max_age_days` | `25` | `decide_run` |

### 2.2 Từ điển

Khoá của mọi từ điển = `dict_key(giá trị thô)`: chữ hoa, bỏ ký tự không phải chữ. Danh sách dưới đây lấy từ EDA (E06, E13); giá trị mới xuất hiện ở nguồn thì thêm vào đây và tăng `logic_version` của bảng Silver.

```python
ORDER_STATUS = {"PENDING": "Pending", "PENDNG": "Pending", "CONFIRMED": "Confirmed", "CONFIRMD": "Confirmed",
                "SHIPPED": "Shipped", "SHIPPD": "Shipped", "DELIVERED": "Delivered", "DELIVERD": "Delivered",
                "RETURNED": "Returned", "RETURN": "Returned", "CANCELLED": "Cancelled", "CANCEL": "Cancelled"}
STATUS_RANK = {"Pending": 1, "Confirmed": 2, "Shipped": 3, "Delivered": 4, "Returned": 5, "Cancelled": 6}
TAX_WORDS = {"FIVEPERCENT": "0.05"}
GENDER = {"F": "Female", "FEMALE": "Female", "M": "Male", "MALE": "Male", "MAEL": "Male", "UNKNOWN": "Unknown"}
POSITION = {"SALESMAN": "Salesman", "SALE": "Salesman", "SALSEMAN": "Salesman", "TEAMLEAD": "Team Lead",
            "MANAGER": "Manager", "DIRECTOR": "Director"}
BRAND = {"IKEA": "IKEA", "APPLE": "Apple", "ZARA": "Zara", "UNIQLO": "Uniqlo", "HP": "HP", "LENOVO": "Lenovo",
         "SAMSUNG": "Samsung", "HM": "H&M", "DELL": "Dell", "ACER": "Acer", "TRUNGNGUYEN": "Trung Nguyen",
         "RAZER": "Razer", "OPPO": "OPPO", "XIAOMI": "Xiaomi", "ASUS": "Asus", "MAXWELLHOUSE": "Maxwell House",
         "HIGHLANDSCOFFEE": "Highlands Coffee", "LEVIS": "Levi's", "STARBUCKS": "Starbucks",
         "MICROSOFT": "Microsoft", "NESTLE": "Nestle", "NESCAFE": "Nescafe", "VIVO": "Vivo", "MSI": "MSI"}
DICTIONARIES = dict(ORDER_STATUS=ORDER_STATUS, STATUS_RANK=STATUS_RANK, TAX_WORDS=TAX_WORDS,
                    GENDER=GENDER, POSITION=POSITION, BRAND=BRAND)
```

`STATUS_RANK` theo thứ tự vòng đời; trạng thái kết thúc xếp sau (DECISIONS D13). Mỗi giá trị của `ORDER_STATUS` phải có trong `STATUS_RANK` và có cột `first_<status>_at_utc` trong `gld.fct_order_line`.

### 2.3 `SILVER`

Một phần tử mỗi bảng Silver.

| Trường | Bắt buộc | Nghĩa |
|---|---|---|
| `table` | ✔ | `slv.orders_event`, `slv.<entity>_history` |
| `entity` | ✔ | `orders`, `customers`, … |
| `kind` | ✔ | `event` / `snapshot`; khớp `kind` của mọi contract trỏ vào bảng |
| `notebook` | ✔ | `nb_slv_orders` / `nb_slv_master` |
| `logic_version` | ✔ | int ≥ 1 |
| `is_active` | ✔ | `False` ⇒ node bị bỏ qua (dùng khi đang dựng dần; [04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node)) |
| `business_keys` | ✔ | cột canonical tạo `_bk` (sau `source_system`) |
| `event_keys` | event | cột canonical tạo `_event_key` (sau `source_system`) |
| `hash_exclude` | ✔ | cột canonical không tính vào `_record_hash` |
| `columns` | ✔ | danh sách cột, bảng dưới |

| Trường của cột | Bắt buộc | Nghĩa |
|---|---|---|
| `name` | ✔ | cột canonical, khớp DDL |
| `type` | ✔ | kiểu Spark SQL, khớp DDL |
| `std` | ✔ | giá trị trong registry [§3.1](#31-registry-std) |
| `source` | | tên property trong contract; mặc định = `name`; không dùng với `std = lookup:*` |
| `source_by_system` | | `{"retail": "<tên khác>"}` khi một nguồn đặt tên khác; giá trị `None` = nguồn đó không có cột này |
| `from_column` | `lookup:*` | cột canonical làm khoá tra |
| `required` | | `True` ⇒ NULL sau chuẩn hoá là REJECT |
| `keep_raw` | | tên cột lưu giá trị gốc đã trim |
| `reject_reason` | | mã REJECT thay mã mặc định của `std` |
| `flag_reason` | | không parse được ⇒ NULL + FLAG mã này (không REJECT) |
| `flag_negative` | | giá trị < 0 ⇒ giữ nguyên + FLAG mã này (không REJECT) |

Ví dụ `slv.orders_event` (đầy đủ):

```python
dict(table="slv.orders_event", entity="orders", kind="event", notebook="nb_slv_orders",
     logic_version=1, is_active=True,
     business_keys=["order_no", "product_code"],
     event_keys=["order_no", "product_code", "order_status", "updated_at_utc"],
     hash_exclude=["order_status_raw", "tax_rate_raw", "created_at_utc"],
     columns=[
       dict(name="order_no", type="string", std="code", required=True),
       dict(name="product_code", type="string", std="code", required=True),
       dict(name="customer_code", type="string", std="code"),
       dict(name="salesman_code", type="string", std="code"),
       dict(name="order_date", type="date", std="date", required=True, reject_reason="invalid_order_date"),
       dict(name="order_status", type="string", std="dict:ORDER_STATUS", required=True,
            keep_raw="order_status_raw", reject_reason="unknown_order_status"),
       dict(name="status_rank", type="int", std="lookup:STATUS_RANK", from_column="order_status"),
       dict(name="quantity", type="decimal(18,2)", std="decimal", required=True,
            reject_reason="invalid_quantity", flag_negative="negative_quantity"),
       dict(name="price", type="decimal(18,2)", std="decimal", required=True,
            reject_reason="invalid_price", flag_negative="negative_price"),
       dict(name="tax_rate", type="decimal(9,6)", std="tax_rate", keep_raw="tax_rate_raw",
            flag_reason="invalid_tax_rate"),
       dict(name="created_at_utc", type="timestamp", std="ts", source="created_at"),
       dict(name="updated_at_utc", type="timestamp", std="ts", source="updated_at", required=True,
            reject_reason="invalid_updated_at"),
     ])
```

Bốn bảng master khai báo theo bảng ánh xạ ở [04 §5.4](04-kien-truc.md#54-lịch-sử-master-slventity_history).

### 2.4 `GOLD`

| Trường | Bắt buộc | Nghĩa |
|---|---|---|
| `table` | ✔ | bảng Gold |
| `notebook` | ✔ | `nb_gld_<tên bảng>` |
| `logic_version` | ✔ | int ≥ 1 |
| `is_active` | ✔ | như SILVER |
| `inputs` | ✔ | `[{table, read}]`, `read` ∈ `full`, `cdf`, `dep` ([04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node)) |
| `grain` | ✔ | cột khoá của bảng |
| `rules` | ✔ | ID rule áp dụng ([04 §8](04-kien-truc.md#8-rule-chuẩn-hoá-và-mô-hình)) |

Nội dung: đúng 5 phần tử theo bảng node Gold ở [04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node).

### 2.5 `CHECKS`

| Trường | Bắt buộc | Nghĩa |
|---|---|---|
| `check_id` | ✔ | ID ở [04 §13](04-kien-truc.md#13-data-quality-checks) |
| `object` | ✔ | bảng hoặc `meta.*` |
| `type` | ✔ | registry [§4.12](#412-check) |
| `params` | | tham số theo type |
| `severity` | ✔ | `critical` / `warning` |
| `scope` | ✔ | `touched` / `full` / `run` |
| `threshold` | | ngưỡng cho type dạng tỷ lệ |

Một check áp cho nhiều bảng (ví dụ DQ-SLV-03) khai báo một phần tử mỗi bảng, cùng `check_id` với hậu tố `.<bảng>`: `DQ-SLV-03.slv.customers_history`.

### 2.6 `validate_config()`

Chạy cuối `nb_config`; lỗi ⇒ raise (PF08).

| Mã | Luật |
|---|---|
| V01 | `table` trong SILVER, GOLD không trùng; mỗi bảng chỉ một object ghi |
| V02 | `silverTable` của mọi contract có trong SILVER và `kind` khớp; mỗi phần tử SILVER có ít nhất một contract |
| V03 | `std` thuộc registry; `dict:`/`lookup:` trỏ khoá có trong `DICTIONARIES` |
| V04 | với mỗi cột có `source`: property tồn tại trong mọi contract đổ vào bảng, trừ nguồn có `source_by_system = None` |
| V05 | `business_keys`, `event_keys`, `hash_exclude`, `from_column` là cột có trong `columns` |
| V06 | `inputs[].table` có trong SILVER/GOLD; đồ thị không chu trình |
| V07 | `logic_version` là int ≥ 1 |
| V08 | `check_id` không trùng; `object` tồn tại; `type`, `severity`, `scope` hợp lệ |
| V09 | mọi giá trị `ORDER_STATUS` có trong `STATUS_RANK` |
| V10 | `SETTINGS` có đủ khoá §2.1 |

---

## 3. `nb_std`

Sinh biểu thức Spark SQL (chuỗi) cho từng kiểu chuẩn hoá và câu `SELECT` của bước `conform`. Python chỉ ghép chuỗi; việc biến đổi chạy bằng `spark.sql` (SQL-first). Không đọc/ghi bảng; không đọc `nb_config` (cấu hình truyền qua tham số). Cần Spark ≥ 3.5 (`try_to_timestamp`) — xác minh ở spike K05.

### 3.1 Registry `std`

| `std` | Hàm | Mã REJECT mặc định khi `required` và NULL |
|---|---|---|
| `code` | `std_code` | `missing_business_key` |
| `text` | `std_text` | `missing_required_value` |
| `title` | `std_title` | `missing_required_value` |
| `ts` | `parse_ts(c, timestamp_formats, source_timezone)` | `invalid_timestamp` |
| `date` | `parse_date(c, date_formats)` | `invalid_date` |
| `decimal` | `parse_decimal(c)` | `invalid_number` |
| `tax_rate` | `parse_tax_rate(c, TAX_WORDS)` | — |
| `dict:<TÊN>` | `std_dict(c, DICTIONARIES[TÊN])` | `unknown_value` |
| `lookup:<TÊN>` | `lookup_expr` trên cột canonical `from_column` | — |

Mã REJECT mặc định chỉ dùng khi cột không khai báo `reject_reason`. Mọi mã phải có trong [04 §7.2](04-kien-truc.md#72-mã-lý-do).

### 3.2 Hàm

Mỗi hàm nhận biểu thức SQL (chuỗi) và trả biểu thức SQL (chuỗi). Test: `spark.sql("SELECT " + <hàm>(...) + " AS v")`.

| Hàm | Chữ ký | Biểu thức sinh ra | Test bắt buộc |
|---|---|---|---|
| `null_if_blank` | `(c)` | `CASE WHEN trim(c) IN ('', 'NULL', 'null', 'N/A', 'n/a') THEN NULL ELSE trim(c) END` | `'  NULL '` ⇒ NULL |
| `std_code` | `(c)` | `upper(regexp_replace(null_if_blank(c), '\\s+', ''))` | `' cus 001'` ⇒ `'CUS001'` |
| `std_text` | `(c)` | `regexp_replace(null_if_blank(c), '\\s+', ' ')` | `'a   b'` ⇒ `'a b'` |
| `std_title` | `(c)` | `initcap(lower(std_text(c)))` | `'viet NAM'` ⇒ `'Viet Nam'` |
| `parse_ts` | `(c, formats, tz)` | `to_utc_timestamp(coalesce(try_to_timestamp(x, 'fmt1'), try_to_timestamp(x, 'fmt2'), …), 'tz')` với `x = null_if_blank(c)` | `'3/15/2025 9:30'`, VN ⇒ `2025-03-15 02:30:00`; `'abc'` ⇒ NULL |
| `parse_date` | `(c, formats)` | `to_date(coalesce(try_to_timestamp(x, 'fmt1'), …))`, không đổi múi giờ | `'25/3/2025'` ⇒ `2025-03-25`; `'3/4/2025'` ⇒ `2025-03-04` |
| `is_ambiguous_dm` | `(c)` | `c RLIKE '^\\d{1,2}/\\d{1,2}/\\d{4}'` và hai số đầu ≤ 12 và khác nhau | `'3/4/2025'` ⇒ true; `'4/4/2025'`, `'13/4/2025'` ⇒ false |
| `parse_decimal` | `(c)` | R-STD-07: bỏ `,` nghìn khi khớp mẫu, rồi `try_cast(... AS DECIMAL(18,2))` | `'1,234.5'` ⇒ 1234.50; `'abc'` ⇒ NULL |
| `dict_key` | `(c)` | `upper(regexp_replace(c, '[^A-Za-z]', ''))` | `' Delivred '` ⇒ `'DELIVRED'` |
| `std_dict` | `(c, mapping)` | `CASE dict_key(c) WHEN 'KHOÁ' THEN 'GIÁ TRỊ' … END` (khoá, giá trị đã escape) | `'delivered '` ⇒ `'Delivered'`; `'xyz'` ⇒ NULL |
| `parse_tax_rate` | `(c, words)` | R-STD-06, `DECIMAL(9,6)` | `'5%'`, `'5'`, `'0.05'`, `'five percent'` ⇒ 0.05; `'150%'` ⇒ NULL |
| `record_hash` | `(cols)` | `sha2(concat_ws('\u001f', coalesce(cast(c AS string), '<NULL>'), …), 256)`, cột sắp theo tên | đổi thứ tự cột không đổi hash; NULL ≠ `''` |
| `sk` | `(*cols)` | `xxhash64(cast(c AS string), …)` | tất định |
| `business_date` | `(ts_utc, tz)` | `to_date(from_utc_timestamp(ts_utc, 'tz'))` | `2025-03-14 18:00` UTC, VN ⇒ `2025-03-15` |
| `date_key` | `(d)` | `coalesce(cast(date_format(d, 'yyyyMMdd') AS int), -1)` | |
| `schema_drift` | `(delivery_columns, contract)` | Python: `(added, missing, missing_required)`; tên cột so không phân biệt hoa thường, đã trim | |
| `conform_sql` | `(contract, silver_spec, dictionaries, future_order_days, source_view, available_columns=None) -> str` | §3.3; `available_columns` (tên cột có trong view, đã trim, chữ thường): cột nguồn thiếu ⇒ NULL | §3.3 |
| `lookup_expr` | `(c, mapping, cast_type)` | `CAST(CASE c WHEN 'Pending' THEN '1' … END AS type)`; khoá khớp đúng chữ (khác `std_dict`, vốn so theo `dict_key`) | `'Delivered'` ⇒ 4 với `STATUS_RANK` |

### 3.3 `conform_sql`

Trả một câu `SELECT` để chạy bằng `spark.sql` trên temp view `source_view`. View chứa dữ liệu đọc từ Landing (cột nguồn dạng chuỗi + cột kỹ thuật `_*`) của **một** source. Kết quả: cột canonical theo `silver_spec.columns`, các cột `keep_raw`, `_bk`, `_record_hash`, `_event_key` (event), `_reject_reason`, `_dq_flags` và cột kỹ thuật giữ nguyên. Không lọc dòng.

```sql
WITH s AS (SELECT <tên cột nguồn, đã trim và chữ thường>, <cột kỹ thuật> FROM <source_view>),
c AS (SELECT <std(cột nguồn)> AS <cột canonical>, …,
             <std_text(cột nguồn)> AS <keep_raw>, …, <cột kỹ thuật>
      FROM s),
r AS (SELECT c.*,
             <CASE … END> AS _reject_reason,
             array_compact(array(<CASE … END>, …)) AS _dq_flags,
             concat_ws('|', source_system, <business_keys>) AS _bk,
             <record_hash(cột canonical − hash_exclude − cột *_raw)> AS _record_hash,
             <sha2(concat_ws('|', source_system, <event_keys>), 256)> AS _event_key
      FROM c)
SELECT * FROM r
```

Quy tắc dựng:

```text
1. Tên cột nguồn: trim, chữ thường.
2. Mỗi cột spec: cột nguồn theo source_by_system[source_system] → source → name (thiếu ⇒ NULL);
   áp std với cấu hình từ contract (formats, timezone); cast type; keep_raw ⇒ std_text(nguồn).
   lookup:* áp sau cùng trên cột canonical from_column.
3. _reject_reason = giá trị đầu tiên khác NULL, theo thứ tự cột trong spec, của:
     when(required và giá trị NULL, reject_reason hoặc mã mặc định §3.1)
   rồi, nếu có cột order_date: when(order_date > to_date(_batch_id, 'yyyyMMdd') + future_order_days, 'future_order_date')
4. _dq_flags:
     when(flag_reason và giá trị NULL và nguồn không NULL, flag_reason)
     when(std dict:* không required và giá trị NULL và nguồn không NULL, 'unmapped_value')
     when(std date/ts và is_ambiguous_dm(nguồn), 'ambiguous_date')
     when(flag_negative và giá trị < 0, flag_negative)
   Với dict:* không required, giá trị NULL được thay bằng std_title(nguồn) (R-STD-08).
5. event: _event_key theo event_keys (cast string).
```

Fixture test: 10 dòng gồm khoá rỗng, `updated_at` 3 định dạng, trạng thái sai chính tả và không có trong từ điển, thuế dạng chữ và vượt khoảng, ngày mơ hồ, ngày tương lai, cột thừa, cột thiếu. Test ghi rõ `_reject_reason` và `_dq_flags` mong đợi của từng dòng.

---
## 4. `nb_common`

Cột "Ai gọi": `P` = `nb_plan`, `W` = notebook Silver/Gold và `nb_land`, `*` = mọi nơi. Riêng §4.10 ghi rõ notebook gọi từng hàm.

### 4.1 Runtime

| Hàm | Ai | Chữ ký | Hành vi |
|---|---|---|---|
| `utc_now` | * | `() -> datetime` | UTC, có `tzinfo` (ghi vào Spark không lệch múi giờ của tiến trình Python) |
| `iso_utc` | * | `(epoch_ms \| datetime) -> str` | `yyyy-MM-ddTHH:mm:ssZ`, cắt phần giây lẻ |
| `new_id` | * | `() -> str` | UUID4 |
| `default_load_date` | P | `() -> str` | hôm qua theo `business_timezone` |
| `to_batch_id` | * | `(load_date) -> str` | `'2026-01-15'` ⇒ `'20260115'`; sai ⇒ `ValueError` |
| `parse_bool` | * | `(x) -> bool` | nhận bool hoặc `'true'/'false'/'1'/'0'` |
| `json_param` | * | `(s, default)` | `''` ⇒ default; JSON sai ⇒ `ValueError` có tên tham số |

### 4.2 Truy cập cấu hình

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `get_contract` | `(source_id) -> dict` | từ `CONTRACTS`; không có ⇒ `KeyError` |
| `active_contracts` | `() -> list[dict]` | contract có `status ≠ retired` |
| `contracts_for_silver` | `(table) -> list[dict]` | contract không `retired` có `silver_table = table` |
| `get_silver`, `get_gold` | `(table) -> dict` | |
| `master_entities` | `() -> list[str]` | `entity` của bảng `SILVER` `kind: snapshot`, `is_active` và có contract không `retired` |
| `hash_columns` | `(silver_spec) -> list[str]` | cột canonical tính vào `_record_hash` (trừ `hash_exclude` và cột `keep_raw`) |

### 4.3 Tên và path

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `safe_table_name` | `(name) -> str` | regex `^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$`; sai ⇒ `ValueError` |
| `staging_dir` | `(delivery_id) -> str` | `Files/_staging/{delivery_id}` |
| `landing_dir` | `(batch_id, source_system, entity, delivery_id) -> str` | `Files/landing/{batch_id}/{source_system}/{entity}/run={delivery_id}` |
| `temp_view` | `(name) -> str` | `_{NB_NAME}__{name}`; `NB_NAME` chưa đặt ⇒ `RuntimeError` |

### 4.4 Catalog và version

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `table_exists` | `(name) -> bool` | `spark.catalog.tableExists` |
| `require_table` | `(name)` | không có ⇒ `RuntimeError("<name> missing — run nb_setup_ddl")` |
| `table_version` | `(name) -> int` | version mới nhất từ `DESCRIBE HISTORY` |
| `data_changed_since` | `(name, version) -> bool` | có commit > `version` thay đổi dữ liệu ([04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node)) |

### 4.5 Landing

| Hàm | Ai | Chữ ký | Hành vi |
|---|---|---|---|
| `read_csv_strings` | * | `(paths, base_path=None) -> DataFrame` | options [04 §3.6](04-kien-truc.md#36-đọc-csv); tên cột trim + chữ thường; thêm `_source_file` (tên file), `_row_no`. `base_path` chặn Spark suy ra cột phân vùng `run=` |
| `read_landing` | W | `(delivery) -> DataFrame` | Parquet ⇒ đọc rồi cast mọi cột về string; CSV ⇒ `read_csv_strings`. Thêm `source_system`, `_batch_id`, `_delivery_id`, `_delivered_at_utc`, `_snapshot_date` từ `delivery` (ưu tiên `delivered_at_str`, `snapshot_date_str` do `select_deliveries` tính bằng SQL) |
| `list_files` | * | `(path) -> list` | file (đệ quy) dưới `path`; thư mục không tồn tại ⇒ `[]` |
| `promote_staging` | W | `(delivery_id, landing_path, files=None)` | `files=None` ⇒ `notebookutils.fs.mv(staging, landing, create_path=True)`; có `files` ⇒ chuyển từng file rồi xoá phần còn lại của staging |
| `write_manifest` | W | `(landing_path, manifest)` | kiểm tra trường bắt buộc ([04 §3.7](04-kien-truc.md#37-manifest)); `put(..., overwrite=False)`; đã có ⇒ lỗi |
| `read_manifest` | * | `(landing_path) -> dict` | |

`_row_no = monotonically_increasing_id()`: ổn định cho cùng tập file và cùng cách đọc; chỉ dùng làm tie-breaker cuối.

### 4.6 Ghi Delta

Quy ước chung:
- Bảng đích phải tồn tại (`require_table`).
- Input qua `align_to_target`: chọn đúng cột của bảng theo thứ tự, cast về kiểu bảng; cột nullable thiếu ⇒ NULL; cột NOT NULL thiếu ⇒ lỗi; cột thừa ⇒ lỗi.
- MERGE dựng bằng danh sách cột tường minh.
- Trước MERGE: input trùng khoá ⇒ `ValueError` kèm tối đa 5 khoá mẫu.
- Trả `last_commit_metrics(target)`; MERGE không tạo commit mới (version không đổi) ⇒ `operation = "NOOP"`, mọi số bằng 0.

| Hàm | Chữ ký | Câu lệnh |
|---|---|---|
| `align_to_target` | `(target, df) -> DataFrame` | |
| `overwrite_table` | `(target, df) -> dict` | `mode("overwrite").option("overwriteSchema", "false").saveAsTable` |
| `merge_insert_only` | `(target, df, keys) -> dict` | `WHEN NOT MATCHED THEN INSERT` |
| `merge_events` | `(target, df) -> dict` | khoá `_event_key`; `WHEN MATCHED AND s._delivered_at_utc > t._delivered_at_utc AND s._record_hash <> t._record_hash THEN UPDATE`; `WHEN NOT MATCHED THEN INSERT` |
| `merge_upsert` | `(target, df, keys, hash_col) -> dict` | `WHEN MATCHED AND t.<hash> <> s.<hash> THEN UPDATE`; `WHEN NOT MATCHED THEN INSERT` |
| `sync_table` | `(target, df, keys, hash_col) -> dict` | `merge_upsert` + `WHEN NOT MATCHED BY SOURCE THEN DELETE` |
| `last_commit_metrics` | `(target) -> dict` | dòng đầu của `DESCRIBE HISTORY`; trả `version`, `rows_inserted`, `rows_updated`, `rows_deleted` theo `operation` (K09): `MERGE` ⇒ `numTargetRowsInserted/Updated/Deleted`; `WRITE`, `CREATE OR REPLACE TABLE AS SELECT` ⇒ `numOutputRows` làm `rows_inserted`, 0 cho hai số còn lại; `DELETE` ⇒ `numDeletedRows`; operation khác ⇒ 0 |

### 4.7 CDF

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `read_changes` | `(table, from_excl, to_incl) -> DataFrame` | `from_excl >= to_incl` ⇒ rỗng đúng schema; ngược lại `table_changes('<table>', from_excl + 1, to_incl)` |
| `changed_keys` | `(table, rng, key_cols) -> DataFrame` | distinct `key_cols` của mọi loại thay đổi |

### 4.8 Quarantine

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `quarantine_frame` | `(df, entity, run_id) -> DataFrame` | dòng có `_reject_reason` ⇒ `action = REJECT`; dòng được nhận, `explode(_dq_flags)` ⇒ `action = FLAG`; `row_json` = `to_json` mọi cột trừ `_reject_reason`, `_dq_flags`, `_row_no`; khử trùng theo khoá chống trùng. `nb_recon` dùng cho RC-03 |
| `quarantine_rows` | `(df, entity, run_id, table="slv.quarantine") -> (int, int)` | `merge_insert_only` `quarantine_frame` theo khoá chống trùng ([04 §7.1](04-kien-truc.md#71-slvquarantine), so sánh null-safe vì `business_key` có thể NULL); trả (số reject, số flag) |
| `resolve_quarantine` | `(entity, keys_df, run_id, table="slv.quarantine")` | `keys_df` có `_bk`, `_delivery_id`, `_delivered_at_utc` của dòng hợp lệ. Đặt `resolved_at_utc`, `resolved_run_id` cho dòng REJECT đang mở cùng `business_key` nhưng thuộc **delivery khác** delivery mới nhất của khoá đó (dòng lỗi và dòng đúng cùng delivery không tự đóng nhau) |

### 4.9 `snapshot_changes(snapshots, previous, key_cols)`

`snapshots`: các snapshot mới đã conform và dedupe, có `source_system`, `_snapshot_date`, `_record_hash`, `is_deleted` (= false) và mọi cột của bảng history trừ `_change_type`. `previous`: `v_<entity>_current` (incremental) hoặc rỗng (full). Trả các dòng thay đổi kèm `is_deleted`, `_change_type`, không gồm dòng của `previous`.

Thứ tự (`_seq`) chỉ tính trên ngày của **snapshot mới**; dòng của `previous` có `_seq = 0`. Lý do: history chỉ chứa thay đổi, nên ngày của `previous` không cho biết khoá có mặt hay vắng ở snapshot trung gian; chỉ các snapshot mới là danh sách đầy đủ.

```sql
WITH s AS (SELECT <cột>, 0 AS _is_prev FROM snapshots UNION ALL SELECT <cột>, 1 AS _is_prev FROM previous),
dates AS (SELECT source_system, _snapshot_date,
                 dense_rank() OVER (PARTITION BY source_system ORDER BY _snapshot_date) AS _dseq
          FROM (SELECT DISTINCT source_system, _snapshot_date FROM snapshots)),
maxseq AS (SELECT source_system, max(_dseq) AS _max_seq FROM dates GROUP BY source_system),
obs0 AS (SELECT s.*, CASE WHEN s._is_prev = 1 THEN 0 ELSE d._dseq END AS _seq
         FROM s LEFT JOIN dates d
           ON s.source_system = d.source_system AND s._snapshot_date = d._snapshot_date AND s._is_prev = 0),
obs AS (SELECT *, lag(_record_hash) OVER w AS _prev_hash, lag(is_deleted) OVER w AS _prev_deleted,
               lag(_seq) OVER w AS _prev_seq, lead(_seq) OVER w AS _next_seq
        FROM obs0 WINDOW w AS (PARTITION BY source_system, <keys> ORDER BY _seq)),
upserts AS (SELECT *, CASE WHEN _prev_hash IS NULL THEN 'INSERT'
                           WHEN _prev_deleted OR _seq > _prev_seq + 1 THEN 'REINSERT' ELSE 'UPDATE' END AS _change_type
            FROM obs
            WHERE _is_prev = 0 AND (_prev_hash IS NULL OR _prev_deleted OR _seq > _prev_seq + 1
                                    OR _record_hash <> _prev_hash)),
deletes AS (SELECT o.*, d2._snapshot_date AS _deleted_on
            FROM obs o JOIN maxseq m ON o.source_system = m.source_system
            JOIN dates d2 ON d2.source_system = o.source_system AND d2._dseq = o._seq + 1
            WHERE NOT o.is_deleted AND o._seq < m._max_seq
              AND (o._next_seq IS NULL OR o._next_seq > o._seq + 1))
SELECT <cột>, _change_type FROM upserts
UNION ALL
SELECT <cột, với is_deleted = true và _snapshot_date = _deleted_on>, 'DELETE' AS _change_type FROM deletes
```
Test: 3 snapshot (S1: A, B; S2: A, B đã sửa; S3: B, rồi S4: A, B) ⇒ INSERT A, INSERT B (S1), UPDATE B (S2), DELETE A (S3), REINSERT A (S4). Incremental: `previous` có A (S1), B (S2); snapshot mới S5 chỉ có A không đổi ⇒ DELETE B tại S5.
### 4.10 Trạng thái và quyết định chạy

| Hàm | Ai | Chữ ký | Hành vi |
|---|---|---|---|
| `log_run` | * | `(run_id, step, layer, event, **cols)` | append `meta.run_log`; cột không truyền ⇒ NULL |
| `log_check` | `nb_dq`, `nb_recon`, `nb_sla_watch`, `nb_test_e2e` | `(run_id, batch_id, check_id, source, check_type, object_name, scope_value, expected, actual, threshold, severity, message=None, sample=None, status=None) -> str` | tính `status` (PASS nếu `|actual − expected| ≤ threshold`, ngược lại FAIL nếu critical, WARN nếu warning) trừ khi truyền `status` (check ba mức như `quarantine_rate`); số đổi sang `Decimal` 4 chữ số; append `meta.check_result`; trả status |
| `record_delivery` | `nb_land`, notebook Silver, `nb_ops_delivery` | `(delivery, status, run_id, **silver_metrics)` | append `meta.delivery_log`; `status` ∈ `LANDED`, `LOADED`, `FAILED`, `PARKED`; `delivery` là manifest hoặc dòng `v_pending_delivery` |
| `record_landed_files` | `nb_plan`, `nb_land` | `(rows, run_id)` | append `meta.landed_file` (`outcome` ∈ `LANDED`, `UNCHANGED`, `SUPERSEDED`) |
| `state_get` | Silver/Gold | `(target) -> dict | None` | dòng `meta.table_state` của bảng |
| `state_put` | Silver/Gold | `(target, logic_version, input_versions, output_version, run_id, exec_mode)` | MERGE theo `target_table` |
| `select_deliveries` | Silver | `(table, mode="normal", load_from=None, load_to=None, full=False) -> list[dict]` | delivery cần xử lý theo [04 §10.4](04-kien-truc.md#104-chọn-delivery-cho-silver), sắp theo `snapshot_date`, `delivered_at_utc`; thêm `delivered_at_str`, `snapshot_date_str` |
| `deliveries_of_run` | `nb_dq`, `nb_recon` | `(run_id, statuses, table="meta.v_delivery_state") -> list[dict]` | delivery có trạng thái mới nhất thuộc `run_id` |
| `decide_run` | Silver/Gold | `(target, full_reload=False, scope="ALL", mode="normal", load_from=None, load_to=None) -> dict` | theo [04 §10.2](04-kien-truc.md#102-quyết-định-chạy-từng-node); tự đọc `table_state`, version input, delivery chờ; trả `{decision, exec_mode, reason, input_versions}` với `input_versions = {"<bảng>": {"from": n, "to": m}}` (chỉ input `full`/`cdf`; Silver là `{}`) |
| `run_node` | Silver/Gold | `(target, build, run_id, full_reload, scope, mode, load_from, load_to) -> dict` | khung chạy một node: `decide_run` ⇒ SKIP ghi `run_log` và trả kết quả `SKIPPED`; RUN ⇒ `run_log STARTED`, `build(decision)` (ghi dữ liệu, trả dict số liệu; lỗi ⇒ `run_log FAILED` rồi raise), `state_put`, `run_log SUCCEEDED`, `worker_result`. `build` có thể trả `exec_mode`, `reason` (ghi đè), `skip_state` |
| `gold_audit` | Gold | `(df, run_id, logic_version) -> DataFrame` | thêm `_run_id`, `_logic_version`, `_updated_at_utc` |
| `resolve_source_ids` | `nb_plan` | `(workspace_name, erp_item, drop_lakehouse) -> dict` | `sempy.fabric.resolve_workspace_id`, `resolve_item_id` (K03) ⇒ `{source_workspace_id, erp_item_id, drop_lakehouse_id}`; không tìm thấy ⇒ `ValueError` |
| `platform_ids` | `nb_plan` | `() -> dict` | `{platform_workspace_id, platform_lakehouse_id}` của notebook đang chạy; đích của Copy activity (không hardcode ID) |
| `preflight` | `nb_plan` | `(p_mode, p_load_date, p_load_date_to, p_scope, p_full_reload) -> dict` | PF01–PF08 ([04 §11.2](04-kien-truc.md#112-kiểm-tra-trước-khi-chạy)); trả `{mode, load_date, load_date_to, scope, full_reload, check_scope, batch_id}` đã chuẩn hoá; sai ⇒ `ValueError` bắt đầu bằng mã `PFxx` |
| `list_drop_zone` | `nb_plan` | `(ids, contract) -> list[dict]` | `notebookutils.fs.ls` thư mục `physicalName` của drop zone, khớp `fileGlob` ⇒ `{name, size, modify_ms}` (`modifyTime` là epoch mili-giây); thư mục chưa có ⇒ `[]`; thiếu `size` hoặc `modifyTime` ⇒ `RuntimeError` |
| `plan_ingest` | `nb_plan` | `(contracts, load_date, watermarks, landed_files, listings, run_start_utc, batch_id, id_fn=new_id) -> dict` | hàm thuần ⇒ `{sources, no_data, errors, superseded}`. `watermarks`: `{(source_system, entity): "yyyy-MM-dd HH:mm:ss"}`; `landed_files`: tập `(source_system, entity, name, size, modify_epoch_seconds)` (so theo giây vì `landed_file` lưu giây); `listings`: `{source_id: list_drop_zone(...)}`. DB: cửa sổ rỗng (watermark ≥ cận trên) ⇒ `no_data reason=up_to_date`. File: `no_data reason=settle_pending:<n>` hoặc `no_new_files`; snapshot không có ngày trong tên ⇒ `errors` |
### 4.11 Worker

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `worker_result` | `(target, exec_mode, status="SUCCEEDED", reason=None, **metrics) -> dict` | đúng [04 §10.3](04-kien-truc.md#103-hợp-đồng-worker); metrics thiếu ⇒ 0; `output_version = table_version(target)` |
| `exit_worker` | `(result)` | `notebookutils.notebook.exit(json.dumps(result, default=str))` |

### 4.12 Check

`run_check(spec, run_id, batch_id, run_scope)` dựng SQL theo `type`, chạy, gọi `log_check`, trả status. Mỗi SQL trả đúng một dòng có cột `actual` (và `sample` nếu có).

| `type` | `actual` | `expected` / ngưỡng |
|---|---|---|
| `unique` | số nhóm khoá có > 1 dòng | 0 |
| `not_null` | số dòng có cột bắt buộc NULL | 0 |
| `quarantine_rate` | % REJECT mỗi delivery của lượt (một dòng check mỗi delivery) | ≤ `quarantineWarnPct` PASS; ≤ `quarantineFailPct` WARN; lớn hơn FAIL |
| `flag_count` | số dòng có mã trong `params.flag` | 0 (warning) |
| `schema_drift` | số delivery của lượt có `schema_added`/`schema_missing` | 0 (warning) |
| `scd2_one_current` | số khoá không có đúng một `is_current` | 0 |
| `scd2_contiguous` | số cặp phiên bản liền kề có `next.valid_from ≠ valid_to + 1` | 0 |
| `unknown_member_rate` | % dòng có SK = −1 hoặc trỏ dòng `is_inferred` | ≤ `threshold` |
| `sk_collision` | số SK ứng với > 1 khoá tự nhiên | 0 |
| `duplicate_file` | số file của lượt có checksum trùng file khác tên | 0 (warning) |
| `freshness` | số contract `daily` chưa có delivery `LOADED` cho batch | 0 (warning) |
| `event_date_vs_file` | số dòng sự kiện từ file `orders_<yyyyMMdd>.csv` có ngày của `updated_at_utc` (đổi về múi giờ nguồn) lệch tên file hơn 1 ngày | 0 (warning) |
| `parked_delivery` | số delivery đang `PARKED` | 0 (warning) |

Contract quality: `run_contract_checks(deliveries, run_id)` đăng ký `read_landing(delivery)` thành temp view, thay `${object}` bằng tên view, chạy `query`, so `mustBe`; `severity: error` ⇒ critical, `warning` ⇒ warning; `source = 'contract'`.

### 4.13 Trạng thái hiện hành

| Hàm | Chữ ký | Hành vi |
|---|---|---|
| `latest_event` | `(events: DataFrame) -> DataFrame` | giữ một dòng mỗi `(source_system, order_no, product_code)` theo R-ORD-04: `updated_at_utc` giảm dần, rồi `status_rank`, `_delivered_at_utc`, `_row_no` |
| `conform_delivery` | `(delivery, silver_spec) -> DataFrame` | `read_landing` + `conform_sql` ([§3.3](#33-conform_sql)); không lọc dòng |
| `delivery_schema` | `(delivery) -> (added, missing, missing_required)` | `schema_drift` giữa `manifest.columns` và contract |
| `conform_master` | `(delivery, silver_spec) -> DataFrame` | `conform_delivery` + R-MST-01 (cột `_is_dup`, mã `duplicate_key_in_snapshot` vào `_dq_flags`) + `self_manager` cho sales_hierarchy (`salesmanager_code` ⇒ NULL, tính lại `_record_hash`) |

Worker Gold dùng hàm này thay cho `slv.v_orders_current`, trên các sự kiện đã lọc theo khoá bị chạm. Phải cho cùng kết quả với view (`test_current_matches_view` ở `nb_test_e2e`); đổi thứ tự sắp xếp ở một nơi thì đổi cả nơi kia.

---

## 5. Mẫu file

### 5.1 Notebook mới `fabric/platform/<name>.Notebook/`

`.platform`:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {"type": "Notebook", "displayName": "<name>"},
  "config": {"version": "2.0", "logicalId": "<UUID4 mới>"}
}
```
Sinh UUID: `uv run python -c "import uuid; print(uuid.uuid4())"`. Notebook ghi đè notebook cũ cùng tên thì giữ `logicalId` cũ (pipeline đang trỏ vào nó).

`notebook-content.py` (worker):

```python
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_gld_fct_order_line
# Rules: R-ORD-04, R-SLM-05. Spec: docs/04-kien-truc.md §6.5, §9.7.

# PARAMETERS CELL ********************

p_run_id = ""
p_full_reload = False
p_scope = "ALL"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_contracts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_std

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

NB_NAME = "nb_gld_fct_order_line"
TARGET = "gld.fct_order_line"
decision = decide_run(TARGET, parse_bool(p_full_reload), p_scope)
if decision["decision"] == "SKIP":
    log_run(p_run_id, TARGET, "gld", "SKIPPED", reason=decision["reason"])
    exit_worker(worker_result(TARGET, "-", status="SKIPPED", reason=decision["reason"]))
# logic theo docs/04-kien-truc.md §9.7, dùng decision["input_versions"] cho CDF
state_put(TARGET, GOLD_SPEC["logic_version"], decision["input_versions"], table_version(TARGET), p_run_id, decision["exec_mode"])
log_run(p_run_id, TARGET, "gld", "SUCCEEDED", exec_mode=decision["exec_mode"], reason=decision["reason"])
exit_worker(worker_result(TARGET, decision["exec_mode"], reason=decision["reason"], rows_in=..., rows_inserted=..., rows_updated=...))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

Quy tắc định dạng: file bắt đầu `# Fabric notebook source`; cell Python `# CELL ********************`; cell tham số `# PARAMETERS CELL ********************`; Markdown `# MARKDOWN ********************` (mỗi dòng `# `); cell SQL dùng `# MAGIC %%sql` / `# MAGIC <sql>` và META `"language": "sparksql"`; `%run` viết trần, một mình một cell; không sửa khối META `dependencies` của notebook đã gắn Lakehouse.

### 5.2 Activity notebook trong pipeline

```json
{
  "type": "TridentNotebook",
  "typeProperties": {
    "notebookId": "<logicalId của notebook>",
    "workspaceId": "00000000-0000-0000-0000-000000000000",
    "parameters": {
      "p_run_id": {"value": {"value": "@pipeline().RunId", "type": "Expression"}, "type": "string"},
      "p_full_reload": {"value": {"value": "@pipeline().parameters.p_full_reload", "type": "Expression"}, "type": "bool"},
      "p_scope": {"value": {"value": "@pipeline().parameters.p_scope", "type": "Expression"}, "type": "string"}
    }
  },
  "policy": {"timeout": "0.02:00:00", "retry": 0, "retryIntervalInSeconds": 30},
  "name": "gld_fct_order_line",
  "dependsOn": [{"activity": "gld_dim_salesman", "dependencyConditions": ["Succeeded"]}]
}
```

---

## 6. Công cụ local

`pyproject.toml` ở gốc repo, dependency duy nhất `pyyaml`. Mọi lệnh chạy qua `uv run`.

### 6.1 `tools/build_contracts.py`

Đặc tả ở [03 §6](03-contract.md#6-toolsbuild_contractspy).

### 6.2 `tools/check_repo.py`

`uv run tools/check_repo.py` — chạy trước mọi commit (Definition of Done). Lỗi in `file:line: Cxx thông điệp`, thoát mã 1.

| Mã | Lỗi khi |
|---|---|
| C01 | `notebook-content.py` không bắt đầu `# Fabric notebook source` |
| C02 | `%run` cùng cell với dòng khác |
| C03 | có `%%configure` |
| C04 | phần Python (bỏ dòng `%run`, `# MAGIC`, comment) không `compile()` được |
| C05 | chuỗi dạng UUID nằm ngoài khối `# META` trong notebook |
| C06 | notebook worker (tên có trong SILVER/GOLD hoặc `nb_land`) không gọi `exit_worker(`, hoặc có `saveAsTable(`, `DROP TABLE`, `spark.conf.set(`, `jdbc` (ingest chỉ bằng Copy activity; bảng chỉ tạo bằng DDL) |
| C07 | `.platform` thiếu, `displayName` khác tên thư mục, hoặc `logicalId` trùng giữa các item |
| C08 | `pipeline-content.json` không phải JSON hợp lệ, hoặc `notebookId` không khớp `logicalId` nào |
| C09 | link tương đối trong `docs/*.md`, `CLAUDE.md`, `README.md` trỏ file không tồn tại |
| C10 | `build_contracts.py --check` lỗi |
| C11 | khối `CREATE TABLE IF NOT EXISTS` / `CREATE OR REPLACE VIEW` trong [04](04-kien-truc.md) khác khối cùng tên trong `nb_setup_ddl` (so sau khi bỏ tiền tố `# MAGIC` và khoảng trắng); chỉ chạy khi `nb_setup_ddl` đã là bản v2 |
| C12 | `nb_config` lệch DDL: tên cột của `SILVER[*].columns` và `keep_raw` không có trong bảng; giá trị của `ORDER_STATUS` không có cột `first_<trạng thái>_at_utc` trong `gld.fct_order_line` (hoặc ngược lại); chỉ chạy khi `nb_config` đã là bản v2 |
