# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_common
# - **Mục đích:** định nghĩa **1 lần** các hàm làm sạch dùng chung (P4) dưới dạng SQL function → gọi được trong mọi cell `%%sql`.
# - **Dùng:** notebook nghiệp vụ gọi `%run nb_common` ngay sau cell parameters. Không có `%%configure` ở đây (notebook gọi lo).
# - Hàm là `TEMPORARY` → sống trong Spark session hiện tại; chạy lại `%run` thì định nghĩa lại, không lỗi.
# - **Chuỗi rỗng / `"NULL"` dạng chữ → NULL** ở mọi hàm.
# # | Hàm | Trả về | Làm gì |
# |---|---|---|
# | `clean_text(s)` | STRING | trim, gộp khoảng trắng thừa |
# | `clean_code(s)` | STRING | bỏ mọi khoảng trắng, viết HOA (mã khách, mã SP…) |
# | `to_amount(s)` | DECIMAL(18,2) | ép số an toàn (bỏ dấu phẩy ngăn cách nghìn); không ép được → NULL |
# | `parse_ts(s)` | TIMESTAMP | thử lần lượt các format ngày giờ của nguồn; không khớp → NULL |
# | `parse_date(s)` | DATE | như `parse_ts`, lấy phần ngày |

# CELL ********************

# MAGIC %%sql
# MAGIC -- Mọi timestamp lưu UTC (PLAN §5.3)
# MAGIC SET spark.sql.session.timeZone = UTC;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Text & code

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION clean_text(s STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN upper(trim(s)) IN ('', 'NULL') THEN NULL
# MAGIC             ELSE regexp_replace(trim(s), '\\s+', ' ') END;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION clean_code(s STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN upper(trim(s)) IN ('', 'NULL') THEN NULL
# MAGIC             ELSE upper(regexp_replace(s, '\\s+', '')) END;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Số

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION to_amount(s STRING) RETURNS DECIMAL(18,2)
# MAGIC RETURN CASE WHEN upper(trim(s)) IN ('', 'NULL') THEN NULL
# MAGIC             ELSE try_cast(replace(trim(s), ',', '') AS DECIMAL(18,2)) END;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ngày giờ
# Thứ tự format giống simulator: `M/d` trước `d/M` (format chính của nguồn); `d/M` chỉ khớp khi ngày > 12.
# Kết quả là giờ **theo múi giờ nguồn** → Silver đổi sang UTC bằng `to_utc_timestamp(parse_ts(x), cfg.source_timezone)`.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION parse_ts(s STRING) RETURNS TIMESTAMP
# MAGIC RETURN CASE WHEN upper(trim(s)) IN ('', 'NULL') THEN NULL
# MAGIC             ELSE coalesce(
# MAGIC                 try_to_timestamp(trim(s), 'yyyy-MM-dd HH:mm:ss'),
# MAGIC                 try_to_timestamp(trim(s), 'yyyy/MM/dd HH:mm:ss'),
# MAGIC                 try_to_timestamp(trim(s), 'M/d/yyyy H:mm:ss'),
# MAGIC                 try_to_timestamp(trim(s), 'M/d/yyyy H:mm'),
# MAGIC                 try_to_timestamp(trim(s), 'd/M/yyyy H:mm:ss'),
# MAGIC                 try_to_timestamp(trim(s), 'd/M/yyyy H:mm'),
# MAGIC                 try_to_timestamp(trim(s), 'yyyy-MM-dd'),
# MAGIC                 try_to_timestamp(trim(s), 'M/d/yyyy'),
# MAGIC                 try_to_timestamp(trim(s), 'd/M/yyyy')
# MAGIC             ) END;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION parse_date(s STRING) RETURNS DATE
# MAGIC RETURN CAST(parse_ts(s) AS DATE);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
