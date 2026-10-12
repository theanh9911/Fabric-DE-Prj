# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_run_id = ""
p_batch_from = ""
p_batch_to = ""
p_mode = "normal"
p_full_reload = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%run nb_config

# CELL ********************


%run nb_common

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

run_id = p_run_id or str(uuid.uuid4())
fact = "gld.gld_f_sales_line"
agg = "gld.gld_a_sales_month"
order_tables = ["slv.slv_wholesale_orders", "slv.slv_retail_orders"]
available = [t for t in order_tables if table_exists(t)]
if not available:
    notebookutils.notebook.exit(json.dumps({"run_id": run_id, "fact_changed": False, "reason": "no Silver orders"}))

orders = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(t) for t in available])
if p_mode == "rerun" and p_batch_from:
    events = spark.table("slv.slv_orders_event").where(F.col("_batch_id").between(p_batch_from, p_batch_to or p_batch_from))
    keys = events.select("_bk").distinct()
    orders = orders.join(keys, "_bk", "inner")

def parse_amount(column):
    cleaned = F.regexp_replace(F.trim(F.col(column).cast("string")), ",", "")
    return F.expr(f"try_cast(regexp_replace(trim(cast(`{column}` as string)), ',', '') as decimal(18,2))")

def status_standard(col):
    x = F.upper(F.regexp_replace(F.trim(F.col(col)), r"[^A-Z]", ""))
    return (F.when(x.isin("PENDING", "PENDNG"), "Pending")
            .when(x.isin("CONFIRMED", "CONFIRMD"), "Confirmed")
            .when(x == "SHIPPED", "Shipped").when(x == "SHIPP", "Shipped")
            .when(x.isin("DELIVERED", "DELIVERD"), "Delivered")
            .when(x.isin("CANCELLED", "CANCEL"), "Cancelled")
            .when(x.isin("RETURNED", "RETURN"), "Returned"))

orders = orders.withColumn("source_system", F.coalesce(F.col("source_system"), F.lit("wholesale")))
orders = orders.withColumn("order_date", F.coalesce(
    F.to_date(F.col("order_date").cast("string")),
    F.to_date(F.expr("try_to_timestamp(cast(order_date as string), 'M/d/yyyy')")),
    F.to_date(F.expr("try_to_timestamp(cast(order_date as string), 'd/M/yyyy')"))))
orders = orders.withColumn("_quantity_parsed", parse_amount("quantity"))
orders = orders.withColumn("_price_parsed", parse_amount("price"))
orders = orders.withColumn("_tax_rate_parsed", F.expr(
    "try_cast(regexp_replace(trim(cast(tax_rate as string)), '%', '') as decimal(12,6))"))
orders = orders.withColumn("_tax_rate_parsed", F.when(F.col("_tax_rate_parsed") > 1,
                                                        F.col("_tax_rate_parsed") / F.lit(100))
                           .otherwise(F.col("_tax_rate_parsed")))
orders = orders.withColumn("order_status_std", status_standard("order_status"))
orders = orders.withColumn("_status_rank", F.when(F.col("order_status_std") == "Delivered", 6)
                           .when(F.col("order_status_std") == "Returned", 5)
                           .when(F.col("order_status_std") == "Cancelled", 5)
                           .when(F.col("order_status_std") == "Shipped", 4)
                           .when(F.col("order_status_std") == "Confirmed", 3)
                           .when(F.col("order_status_std") == "Pending", 2).otherwise(0))
orders = orders.withColumn("_dq_reason", F.when(F.col("order_status_std").isNull(), "unknown_order_status")
                           .when(F.col("order_date").isNull(), "invalid_order_date")
                           .when(F.col("_quantity_parsed").isNull(), "invalid_quantity")
                           .when(F.col("_price_parsed").isNull(), "invalid_price")
                           .when((F.upper(F.trim(F.col("tax_rate").cast("string"))).isin("", "NULL") == F.lit(False)) &
                                 F.col("tax_rate").isNotNull() & F.col("_tax_rate_parsed").isNull(), "invalid_tax_rate"))
bad = orders.filter(F.col("_dq_reason").isNotNull())
valid_keys = orders.filter(F.col("_dq_reason").isNull()).select("_bk").distinct()
resolve_quarantine_keys("gld", "orders", valid_keys)
q_count = write_quarantine(bad, "gld", "orders", p_batch_from or "", run_id,
                           business_key_col="_bk", updated_col="_updated_at_utc")
orders = orders.filter(F.col("_dq_reason").isNull()).drop("_dq_reason")

# Resolve one current row per order line, preserving the most recent event and source precedence.
w = Window.partitionBy("_bk").orderBy(F.col("_updated_at_utc").desc_nulls_last(),
                                         F.col("_status_rank").desc(),
                                         F.when(F.col("source_system") == "wholesale", 0).otherwise(1),
                                         F.col("_delivered_at_utc").desc_nulls_last())
cur = orders.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
for key_col in ["order_no", "product_code", "customer_code", "salesman_code"]:
    if key_col in cur.columns:
        cur = cur.withColumn(key_col, F.upper(F.regexp_replace(F.trim(F.col(key_col).cast("string")), r"\s+", "")))
cur = (cur.withColumn("order_status", F.col("order_status_std"))
       .withColumn("quantity", F.col("_quantity_parsed"))
       .withColumn("price", F.col("_price_parsed")))
cur = cur.withColumn("gross_amount", (F.col("quantity") * F.col("price")).cast("decimal(18,2)"))
tax_raw = F.regexp_replace(F.trim(F.col("tax_rate").cast("string")), "%", "")
cur = cur.withColumn("tax_rate_decimal", F.col("_tax_rate_parsed"))
cur = cur.withColumn("tax_amount", F.round(F.col("gross_amount") * F.col("tax_rate_decimal"), 2).cast("decimal(18,2)"))
cur = cur.withColumn("net_amount", (F.col("gross_amount") + F.col("tax_amount")).cast("decimal(18,2)"))
cur = cur.withColumn("date_key", F.date_format(F.to_date("order_date"), "yyyyMMdd").cast("int"))
cur = cur.withColumn("customer_sk", F.when(F.col("customer_code").isNull(), -1).otherwise(F.xxhash64(F.upper(F.trim("customer_code")))))
cur = cur.withColumn("product_sk", F.when(F.col("product_code").isNull(), -1).otherwise(F.xxhash64(F.upper(F.trim("product_code")))))
if "salesman_code" in cur.columns:
    salesman_keys = cur.select("source_system", F.upper(F.trim(F.col("salesman_code"))).alias("salesman_code"))\
        .where(F.col("salesman_code").isNotNull()).distinct()
    if table_exists("gld.gld_d_salesman"):
        known_salesmen = spark.table("gld.gld_d_salesman").select("source_system", "salesman_code").distinct()
        missing_salesmen = salesman_keys.join(known_salesmen, ["source_system", "salesman_code"], "left_anti")
    else:
        missing_salesmen = salesman_keys
    if missing_salesmen.limit(1).count():
        inferred_salesmen = (missing_salesmen
            .withColumn("salesman_sk", F.xxhash64("source_system", "salesman_code", F.lit("1900-01-01")))
            .withColumn("salesman_name", F.lit("Unknown (pending)"))
            .withColumn("salesmanager_code", F.lit(None).cast("string"))
            .withColumn("salesmanager_name", F.lit(None).cast("string"))
            .withColumn("division", F.lit(None).cast("string"))
            .withColumn("position", F.lit(None).cast("string"))
            .withColumn("valid_from", F.lit("1900-01-01").cast("date"))
            .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit(True)).withColumn("is_inferred", F.lit(True))
            .withColumn("is_deleted", F.lit(False)).withColumn("_run_id", F.lit(run_id)))
        append_only_merge("gld.gld_d_salesman", inferred_salesmen, ["salesman_sk"])
if table_exists("gld.gld_d_salesman"):
    salesman = spark.table("gld.gld_d_salesman").select(
        F.col("source_system").alias("_dim_source"), F.col("salesman_code").alias("_dim_salesman_code"),
        F.col("valid_from").alias("_valid_from"), F.col("valid_to").alias("_valid_to"),
        F.col("salesman_sk").alias("_salesman_sk"))
    cur = cur.join(salesman,
                   (F.col("source_system") == F.col("_dim_source")) &
                   (F.upper(F.trim(F.col("salesman_code"))) == F.upper(F.trim(F.col("_dim_salesman_code")))) &
                   F.col("order_date").between(F.col("_valid_from"), F.col("_valid_to")), "left")
    cur = cur.withColumn("salesman_sk", F.coalesce(F.col("_salesman_sk"), F.lit(-1))).drop(
        "_dim_source", "_dim_salesman_code", "_valid_from", "_valid_to", "_salesman_sk")
else:
    cur = cur.withColumn("salesman_sk", F.lit(-1))

def add_inferred_member(source_key, target_table, key_name, sk_name, payload_defaults):
    if key_name not in cur.columns:
        return
    source_keys = cur.select(F.upper(F.trim(F.col(key_name))).alias(key_name)).where(F.col(key_name).isNotNull()).distinct()
    if table_exists(target_table):
        known = spark.table(target_table).select(F.upper(F.trim(F.col(key_name))).alias(key_name)).distinct()
        missing = source_keys.join(known, key_name, "left_anti")
    else:
        missing = source_keys
    if missing.limit(1).count() == 0:
        return
    result = missing.withColumn(sk_name, F.xxhash64(F.col(key_name)))
    for column, value in payload_defaults.items():
        result = result.withColumn(column, F.lit(value))
    result = (result.withColumn("_updated_at_utc", F.current_timestamp())
              .withColumn("_delivered_at_utc", F.current_timestamp())
              .withColumn("is_inferred", F.lit(True)).withColumn("is_deleted", F.lit(False))
              .withColumn("_run_id", F.lit(run_id)))
    payload_cols = [c for c in result.columns if c != key_name]
    result = result.withColumn("_dim_hash", F.sha2(F.concat_ws("\u001f", *[
        F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in payload_cols]), 256))
    append_only_merge(target_table, result, [key_name])

add_inferred_member("customer_code", "gld.gld_d_customer", "customer_code", "customer_sk",
                    {"customer_name": "Unknown (pending)", "country": None, "city": None, "gender": None})
add_inferred_member("product_code", "gld.gld_d_product", "product_code", "product_sk",
                    {"product_name": "Unknown (pending)", "brand": None, "category_code": None})
fact_cols = ["order_no", "product_code", "date_key", "customer_sk", "product_sk", "salesman_sk",
             "order_status", "quantity", "gross_amount", "tax_amount", "net_amount", "source_system",
             "_updated_at_utc", "_batch_id", "_run_id"]
src = cur.select(*[c for c in fact_cols if c in cur.columns])
recognized = src.filter(F.col("order_status") == "Delivered")
months = [r[0] for r in src.select(F.date_format(F.to_date(F.col("date_key").cast("string"), "yyyyMMdd"), "yyyyMM").cast("int").alias("m")).distinct().collect() if r[0] is not None]

if p_full_reload and table_exists(fact):
    spark.sql(f"DROP TABLE {fact}")
if not table_exists(fact):
    recognized.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(fact)
else:
    recognized.createOrReplaceTempView("_fact_src")
    insert_cols = ",".join([f"`{c}`" for c in recognized.columns])
    vals = ",".join([f"s.`{c}`" for c in recognized.columns])
    updates = ",".join([f"`{c}`=s.`{c}`" for c in recognized.columns if c not in ("order_no", "product_code")])
    spark.sql(f"MERGE INTO {fact} t USING _fact_src s ON t.order_no=s.order_no AND t.product_code=s.product_code "
              f"WHEN MATCHED AND s._updated_at_utc >= t._updated_at_utc THEN UPDATE SET {updates} "
              f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({vals})")
    nonrecognized = src.filter(F.col("order_status") != "Delivered")
    if table_exists(fact) and nonrecognized.limit(1).count():
        nonrecognized.createOrReplaceTempView("_fact_not_recognized")
        spark.sql(f"MERGE INTO {fact} t USING _fact_not_recognized s ON t.order_no=s.order_no AND t.product_code=s.product_code "
                  "WHEN MATCHED AND s._updated_at_utc >= t._updated_at_utc THEN DELETE")

if table_exists(fact):
    f = spark.table(fact).withColumn("month_key", F.floor(F.col("date_key") / 100).cast("int"))
    if months:
        f = f.where(F.col("month_key").isin(months))
    monthly = (f.groupBy("month_key", "product_sk", "customer_sk", "salesman_sk")
               .agg(F.sum("quantity").alias("quantity"), F.sum("gross_amount").alias("gross_amount"),
                    F.sum("tax_amount").alias("tax_amount"), F.sum("net_amount").alias("net_amount"),
                    F.count(F.lit(1)).alias("sales_line_count")))
    if p_full_reload or not table_exists(agg):
        monthly.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(agg)
    elif months:
        monthly.createOrReplaceTempView("_agg_src")
        spark.sql(f"DELETE FROM {agg} WHERE month_key IN ({','.join(map(str, months))})")
        monthly.write.format("delta").mode("append").saveAsTable(agg)

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "fact_changed": True,
                                        "fact_rows": recognized.count(), "quarantined": q_count,
                                        "month_keys": months}))
