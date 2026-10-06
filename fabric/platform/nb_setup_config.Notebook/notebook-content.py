# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC { "defaultLakehouse": { "name": "lh_platform" } }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_setup_config
# - **Mục đích:** ghi nội dung bảng `meta.cfg_*` và `meta.ref_*`. **Notebook này là nguồn sự thật** — không sửa tay các bảng đó trên Fabric.
# - **Cách ghi:** mỗi bảng 1 `MERGE … USING VALUES`: thêm dòng mới, cập nhật dòng đổi, **xoá dòng không còn trong script** (`WHEN NOT MATCHED BY SOURCE`).
#   → Bảng luôn khớp đúng nội dung notebook; chạy lại bao nhiêu lần cũng được.
# - **Sửa config:** sửa VALUES ở đây → Commit → chạy lại notebook (Dev, rồi Prod sau khi merge).
# - **Chạy:** sau `nb_setup_ddl`.
# - Bảng chưa có cell (`cfg_pipeline_step`, `cfg_dq_rule`, `ref_holiday_vn`) sẽ được thêm khi tới bước dùng chúng.

# MARKDOWN ********************

# ## `cfg_source_entity` — nguồn nào, lấy thế nào
# - **wholesale** = ERP (Fabric SQL DB): incremental theo `updated_at` (UTC, `sysutcdatetime()`), lùi 1 ngày để bắt dòng commit trễ.
# - **retail / reference** = file CSV trong drop zone: lấy file mới theo LastModified; master gửi **snapshot đầy đủ** mỗi khi có thay đổi.
#   File không có múi giờ → coi là giờ Việt Nam.
# - Khoá orders = `order_no + product_code` (1 dòng sản phẩm của đơn).

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO meta.cfg_source_entity t
# MAGIC USING (
# MAGIC     SELECT * FROM VALUES
# MAGIC     -- source_system, entity,          source_type, source_object,                    load_type,       watermark_type,  watermark_column, lookback_days, source_timezone,    business_keys,          load_order, is_active
# MAGIC     ('reference', 'categories',      'file', 'inbound/reference/categories',       'full_snapshot', 'file_modified', NULL,         0, 'Asia/Ho_Chi_Minh', 'category_code',        10, true),
# MAGIC     ('wholesale', 'customers',       'db',   'dbo.customers',                      'incremental',   'column',        'updated_at', 1, 'UTC',              'customer_code',        20, true),
# MAGIC     ('wholesale', 'products',        'db',   'dbo.products',                       'incremental',   'column',        'updated_at', 1, 'UTC',              'product_code',         20, true),
# MAGIC     ('wholesale', 'sales_hierarchy', 'db',   'dbo.sales_hierarchy',                'incremental',   'column',        'updated_at', 1, 'UTC',              'salesman_code',        20, true),
# MAGIC     ('retail',    'customers',       'file', 'inbound/retail/customers',           'full_snapshot', 'file_modified', NULL,         0, 'Asia/Ho_Chi_Minh', 'customer_code',        20, true),
# MAGIC     ('retail',    'products',        'file', 'inbound/retail/products',            'full_snapshot', 'file_modified', NULL,         0, 'Asia/Ho_Chi_Minh', 'product_code',         20, true),
# MAGIC     ('retail',    'sales_hierarchy', 'file', 'inbound/retail/sales_hierarchy',     'full_snapshot', 'file_modified', NULL,         0, 'Asia/Ho_Chi_Minh', 'salesman_code',        20, true),
# MAGIC     ('wholesale', 'orders',          'db',   'dbo.orders',                         'incremental',   'column',        'updated_at', 1, 'UTC',              'order_no,product_code', 50, true),
# MAGIC     ('retail',    'orders',          'file', 'inbound/retail/orders',              'incremental',   'file_modified', NULL,         0, 'Asia/Ho_Chi_Minh', 'order_no,product_code', 50, true)
# MAGIC     AS v(source_system, entity, source_type, source_object, load_type, watermark_type, watermark_column,
# MAGIC          lookback_days, source_timezone, business_keys, load_order, is_active)
# MAGIC ) s
# MAGIC ON t.source_system = s.source_system AND t.entity = s.entity
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `ref_order_status` — vòng đời đơn & định nghĩa doanh thu
# `Pending → Shipped → Delivered | Cancelled`. **Doanh thu = `is_sales_recognized`** — SQL/DAX không viết `'Delivered'`.

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO meta.ref_order_status t
# MAGIC USING (
# MAGIC     SELECT * FROM VALUES
# MAGIC     -- status,     sequence, is_final, is_sales_recognized
# MAGIC     ('Pending',   1, false, false),
# MAGIC     ('Shipped',   2, false, false),
# MAGIC     ('Delivered', 3, true,  true),
# MAGIC     ('Cancelled', 3, true,  false)
# MAGIC     AS v(status, sequence, is_final, is_sales_recognized)
# MAGIC ) s
# MAGIC ON t.status = s.status
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `ref_value_mapping` — chuẩn hoá giá trị
# - `source_value` = `upper(trim(giá trị nguồn))` → Silver JOIN theo đúng biểu thức đó.
# - Hiện có giá trị chuẩn của `order_status`. Biến thể sai chính tả, gender, position, brand… bổ sung ở **Bước 1** sau khi khám phá dữ liệu (`docs/dq_findings.md`).

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO meta.ref_value_mapping t
# MAGIC USING (
# MAGIC     SELECT * FROM VALUES
# MAGIC     -- domain,        source_value, standard_value
# MAGIC     ('order_status', 'PENDING',   'Pending'),
# MAGIC     ('order_status', 'SHIPPED',   'Shipped'),
# MAGIC     ('order_status', 'DELIVERED', 'Delivered'),
# MAGIC     ('order_status', 'CANCELLED', 'Cancelled')
# MAGIC     AS v(domain, source_value, standard_value)
# MAGIC ) s
# MAGIC ON t.domain = s.domain AND t.source_value = s.source_value
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Kiểm tra

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'cfg_source_entity' AS table_name, count(*) AS row_count FROM meta.cfg_source_entity
# MAGIC UNION ALL SELECT 'ref_order_status',  count(*) FROM meta.ref_order_status
# MAGIC UNION ALL SELECT 'ref_value_mapping', count(*) FROM meta.ref_value_mapping;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
