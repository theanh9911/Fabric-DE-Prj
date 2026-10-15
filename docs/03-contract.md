# 03 — Data contract

## 1. Cách làm

Contract là thoả thuận giữa team nguồn (producer) và data team (consumer) về **dữ liệu nguồn giao như thế nào**: cột, kiểu, khoá, cách giao, hạn giao, chất lượng cam kết, người chịu trách nhiệm.

```text
contracts/<source_system>/<entity>.yaml          nguồn sự thật; chuẩn ODCS v3.0.2; review qua PR
        │  uv run tools/build_contracts.py       kiểm tra + sinh code
        ▼
fabric/platform/nb_contracts.Notebook/           dict Python sinh tự động; không sửa tay
        │  %run nb_contracts
        ▼
nb_config, nb_plan, nb_land, nb_slv_*, nb_dq, nb_sla_watch đọc CONTRACTS
```

Vì sao sinh code thay vì đọc YAML lúc chạy: notebook Fabric không đọc được file trong repo Git lúc chạy; sinh code giữ YAML là nguồn sự thật mà không cần bước upload file lên OneLake (DECISIONS D01).

Contract chỉ mô tả **nguồn**. Cách platform biến đổi dữ liệu (cột canonical, chuẩn hoá, bảng Gold, check nội bộ) nằm ở `nb_config` ([05 §2](05-thu-vien.md#2-nb_config)).

## 2. Bố cục

| File | Source ID |
|---|---|
| `contracts/wholesale/orders.yaml` | `wholesale.orders` |
| `contracts/wholesale/customers.yaml` | `wholesale.customers` |
| `contracts/wholesale/products.yaml` | `wholesale.products` |
| `contracts/wholesale/sales_hierarchy.yaml` | `wholesale.sales_hierarchy` |
| `contracts/retail/orders.yaml` | `retail.orders` |
| `contracts/retail/customers.yaml` | `retail.customers` |
| `contracts/retail/products.yaml` | `retail.products` |
| `contracts/retail/sales_hierarchy.yaml` | `retail.sales_hierarchy` |
| `contracts/reference/categories.yaml` | `reference.categories` |

`id` trong file = Source ID = `<thư mục>.<tên file>`.

## 3. Profile ODCS của dự án

Dự án dùng một tập con của ODCS v3.0.2: đủ để file đọc được bởi người và công cụ hiểu ODCS, không dùng hết đặc tả. Trường không có trong các bảng dưới **không được dùng** (không ai đọc); `build_contracts.py` báo lỗi khi gặp trường lạ.

### 3.1 Cấp file

| Trường | Bắt buộc | Giá trị | Ai đọc |
|---|---|---|---|
| `apiVersion` | ✔ | `v3.0.2` | validator |
| `kind` | ✔ | `DataContract` | validator |
| `id` | ✔ | Source ID | mọi nơi |
| `version` | ✔ | semver (§5) | `delivery_log.contract_version` |
| `status` | ✔ | `draft` / `active` / `retired` (§4) | planner, validator |
| `description.purpose`, `.limitations`, `.usage` | ✔ | văn bản | người đọc |
| `schema` | ✔ | đúng **một** object (§3.2) | |
| `team` | ✔ | ≥ 1 phần tử `role: owner` (`username`, `role`) | nội dung cảnh báo |
| `support` | ✔ | ≥ 1 phần tử (`channel`, `tool`) | nội dung cảnh báo |
| `customProperties` | ✔ | §3.4 | platform |

### 3.2 `schema[0]`

| Trường | Bắt buộc | Giá trị | Ai đọc |
|---|---|---|---|
| `name` | ✔ | tên entity | |
| `physicalName` | ✔ | DB: `dbo.<bảng>`; file: thư mục dưới `Files/` của drop zone, ví dụ `inbound/retail/orders` | `nb_plan` |
| `physicalType` | ✔ | `table` / `file` | validator (khớp `loadStrategy`) |
| `logicalType` | ✔ | `object` | validator |
| `description` | ✔ | grain: "One row = …" | người đọc |
| `properties` | ✔ | §3.3 | |
| `quality` | | §3.5 | `nb_dq` |

### 3.3 `properties[]`

| Trường | Bắt buộc | Giá trị | Ai đọc |
|---|---|---|---|
| `name` | ✔ | tên cột đúng như nguồn giao | `conform` (map sang canonical), phát hiện schema drift |
| `logicalType` | ✔ | `string` / `integer` / `number` / `date` / `timestamp` | validator: có `date` ⇒ cần `dateFormats`; có `timestamp` ⇒ cần `timestampFormats`, `sourceTimezone` |
| `physicalType` | ✔ | DB: kiểu SQL Server viết thường (`nvarchar(30)`, `decimal(18,2)`, `datetime2(0)`, …); file: `string` | người đọc (cam kết kiểu của producer) |
| `required` | ✔ | `true` / `false` — producer cam kết cột luôn có và không rỗng | Silver: cột `required` vắng mặt trong delivery ⇒ delivery FAILED (`schema_missing_required`) |
| `primaryKey` | | `true` cho cột khoá | snapshot: phát hiện trùng khoá trong một snapshot (R-MST-01); validator: snapshot phải có đúng một cột `primaryKey` |
| `description` | ✔ | nghĩa của cột, vấn đề đã biết | người đọc |

### 3.4 `customProperties`

Danh sách `- property: <tên>` / `value: <giá trị>`. Chỉ các tên sau:

| `property` | Áp dụng | Giá trị | Ai đọc |
|---|---|---|---|
| `loadStrategy` | mọi nguồn | `db_incremental` / `db_full_snapshot` / `file_new_or_changed` | `nb_plan`, `nb_land` |
| `kind` | mọi nguồn | `event` / `snapshot` | Silver, planner |
| `silverTable` | mọi nguồn | tên bảng Silver đích, phải có trong `nb_config.SILVER` | DAG |
| `watermarkColumn` | `db_incremental` | tên một property `timestamp`, `required: true` | planner |
| `sourceClock` | `db_incremental` | `virtual` (cận trên = `p_load_date + 1 ngày`) / `real` (cận trên = `MAX(wm)` nguồn − `commitLagMinutes`) | planner |
| `commitLagMinutes` | `db_incremental` | số nguyên ≥ 0 | planner (`real`) |
| `lookbackMinutes` | `db_incremental` | số nguyên ≥ 0 | planner |
| `fileGlob` | `file_new_or_changed` | mẫu tên file, ví dụ `*.csv` | `nb_plan` |
| `settleMinutes` | `file_new_or_changed` | số nguyên ≥ 0 | `nb_plan` |
| `sourceTimezone` | có property `timestamp` | IANA, ví dụ `Asia/Ho_Chi_Minh` | `conform` |
| `timestampFormats` | có property `timestamp` | danh sách pattern Spark, thử theo thứ tự | `conform` |
| `dateFormats` | có property `date` | danh sách pattern Spark, thử theo thứ tự | `conform` |
| `deliveryCadence` | mọi nguồn | `daily` (phải có delivery mỗi ngày) / `on_change` (chỉ khi đổi) | `nb_sla_watch` |
| `availableByLocalTime` | `deliveryCadence: daily` | `HH:mm` giờ VN mà dữ liệu ngày D phải sẵn sàng | `nb_sla_watch`, lịch pipeline |
| `quarantineWarnPct` | mọi nguồn | % dòng REJECT của một delivery gây cảnh báo | DQ-SLV-04 |
| `quarantineFailPct` | mọi nguồn | % dòng REJECT gây FAIL; phải > `quarantineWarnPct` | DQ-SLV-04 |

### 3.5 `quality[]`

Chỉ dùng `type: sql`.

| Trường | Bắt buộc | Giá trị |
|---|---|---|
| `type` | ✔ | `sql` |
| `description` | ✔ | câu mô tả cam kết; dùng làm thông điệp check |
| `query` | ✔ | Spark SQL trả **một số**; `${object}` là dữ liệu của **một delivery** đọc từ Landing, mọi cột kiểu chuỗi (dùng `try_cast`) |
| `mustBe` | ✔ | giá trị mong đợi |
| `severity` | ✔ | `warning` (vi phạm ghi WARN; dòng bẩn vẫn do Silver xử lý) hoặc `error` (FAIL ⇒ lượt chạy FAILED) |

Check ID sinh tự động: `CT-<id>-<số thứ tự 2 chữ số>`, ví dụ `CT-retail.orders-01`. Chạy cho mỗi delivery trong lượt, ghi `meta.check_result` với `source = 'contract'`.

## 4. Vòng đời `status`

| `status` | Nghĩa | Platform |
|---|---|---|
| `draft` | đang soạn; chưa xác nhận bằng EDA hoặc chưa được producer duyệt | nạp ở Dev; **chặn promote Prod** |
| `active` | đã duyệt | nạp |
| `retired` | đã ngừng | không nạp; nhánh DAG của nó bị bỏ |

Cả 9 contract là `active` từ 2026-10-15 (T1.2): cột và định dạng đã đối chiếu với EDA ([EDA.md](EDA.md)); múi giờ retail và định dạng thời gian file master là giả định ghi ở [DECISIONS D41, D43](DECISIONS.md#2-mô-hình-và-nghiệp-vụ).

## 5. Phiên bản và quy trình đổi

| Loại thay đổi | Ví dụ | Bump | Platform phải làm |
|---|---|---|---|
| Phá vỡ | đổi tên/xoá cột, đổi kiểu, đổi khoá, đổi `loadStrategy`/`kind` | MAJOR | task riêng: sửa mapping `nb_config.SILVER`, tăng `logic_version` Silver ⇒ full rebuild |
| Thêm | thêm cột không bắt buộc, thêm quality rule, thêm format | MINOR | thêm format ⇒ tăng `logic_version` Silver; thêm cột muốn dùng ⇒ sửa `SILVER` + DDL |
| Mô tả | sửa `description`, support | PATCH | không |

Quy trình: nhánh `contract/<id>-<mô tả>` → sửa YAML + bump `version` → `uv run tools/build_contracts.py` → commit cả YAML và `nb_contracts` → PR có owner của nguồn (cột `team`) và data team duyệt.

## 6. `tools/build_contracts.py`

Chạy: `uv run tools/build_contracts.py` (sinh), `uv run tools/build_contracts.py --check` (chỉ kiểm, lỗi nếu `nb_contracts` lệch YAML — dùng trong `check_repo.py`), `--strict` (thêm: lỗi nếu còn `draft` — dùng trước khi promote Prod). Phụ thuộc duy nhất: `pyyaml` (khai báo trong `pyproject.toml`).

Kiểm tra theo thứ tự; lỗi in `file: mã: thông điệp`, thoát mã 1.

| Mã | Kiểm tra |
|---|---|
| BC01 | YAML đọc được; có đủ trường bắt buộc §3.1–§3.3 với đúng kiểu giá trị |
| BC02 | chỉ dùng trường trong profile §3 |
| BC03 | `id` = `<thư mục>.<tên file>`; `id` không trùng |
| BC04 | `customProperties` đủ theo điều kiện "Áp dụng" §3.4; không thừa |
| BC05 | `physicalType` của schema khớp `loadStrategy` (`db_*` ⇔ `table`, `file_*` ⇔ `file`) |
| BC06 | `watermarkColumn` là property `timestamp` và `required` |
| BC07 | `kind: snapshot` có đúng một property `primaryKey: true` |
| BC08 | `quality[].query` có `${object}`; `severity` hợp lệ |
| BC09 | `quarantineWarnPct < quarantineFailPct` |
| BC10 | `--strict`: không còn `status: draft` |

Kết quả sinh `fabric/platform/nb_contracts.Notebook/notebook-content.py` (và `.platform` nếu chưa có) với một cell:

```python
# GENERATED by tools/build_contracts.py from contracts/**/*.yaml — DO NOT EDIT
CONTRACTS = {
    "wholesale.orders": {
        "id": "wholesale.orders", "version": "1.0.0", "status": "draft",
        "source_system": "wholesale", "entity": "orders",
        "physical_name": "dbo.orders", "physical_type": "table",
        "owner": "erp-team", "support_channel": "erp-dataops",
        "columns": [
            {"name": "order_line_id", "logical_type": "integer", "physical_type": "bigint",
             "required": True, "primary_key": True},
            # …
        ],
        "quality": [
            {"check_id": "CT-wholesale.orders-01", "description": "…", "query": "…",
             "must_be": 0, "severity": "warning"},
        ],
        "load_strategy": "db_incremental", "kind": "event", "silver_table": "slv.orders_event",
        "watermark_column": "updated_at", "source_clock": "virtual",
        "commit_lag_minutes": 0, "lookback_minutes": 1440,
        "file_glob": None, "settle_minutes": None,
        "source_timezone": "UTC", "timestamp_formats": ["yyyy-MM-dd HH:mm:ss"],
        "date_formats": ["yyyy-MM-dd"],
        "delivery_cadence": "daily", "available_by_local_time": "06:00",
        "quarantine_warn_pct": 2, "quarantine_fail_pct": 20,
    },
    # … đủ 9 source, sắp theo id
}
```

Tên khoá Python là `snake_case` của tên ODCS; property không áp dụng mang giá trị `None` để mọi phần tử có cùng khoá.

## 7. Thêm một nguồn mới

1. Viết `contracts/<source_system>/<entity>.yaml` (`status: draft`) theo profile §3.
2. `uv run tools/build_contracts.py`.
3. Nếu là entity mới (chưa có bảng Silver): thêm phần tử `SILVER` trong `nb_config`, DDL trong `nb_setup_ddl`, worker nếu khác họ `orders`/`master` — theo task riêng.
4. Nếu entity đã có (ví dụ kênh bán mới giao orders): chỉ cần contract với `silverTable` trỏ bảng có sẵn, và `source` mapping nếu tên cột khác ([05 §2.3](05-thu-vien.md#23-silver)).
5. EDA delivery đầu tiên ⇒ chỉnh contract ⇒ `active`.
