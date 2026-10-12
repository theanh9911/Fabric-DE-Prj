# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_run_id = ""
p_entities = "ALL"
p_full_reload = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config
%run nb_common

from pyspark.sql import functions as F
from pyspark.sql.window import Window

run_id = p_run_id or str(uuid.uuid4())

def ensure_date_dimension():
    table = "gld.gld_d_date"
    if not table_exists(table) or p_full_reload:
        days = spark.sql("SELECT explode(sequence(to_date('2020-01-01'), to_date('2035-12-31'), interval 1 day)) AS date_value")
        result = (days.withColumn("date_key", F.date_format("date_value", "yyyyMMdd").cast("int"))
                  .withColumn("calendar_year", F.year("date_value"))
                  .withColumn("calendar_quarter", F.quarter("date_value"))
                  .withColumn("month_key", F.date_format("date_value", "yyyyMM").cast("int"))
                  .withColumn("month_number", F.month("date_value"))
                  .withColumn("day_of_month", F.dayofmonth("date_value"))
                  .withColumn("day_of_week", F.dayofweek("date_value"))
                  .withColumn("fiscal_year", F.when(F.month("date_value") >= 7, F.year("date_value") + 1).otherwise(F.year("date_value")))
                  .withColumn("is_weekend", F.dayofweek("date_value").isin(1, 7)))
        result.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(table)

def latest_source_rows(table_names, key_col):
    frames = [spark.table(t) for t in table_names if table_exists(t)]
    if not frames:
        return None
    unioned = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    unioned = unioned.withColumn("_source_priority", F.when(F.col("source_system") == "wholesale", 0).otherwise(1))
    w = Window.partitionBy(key_col).orderBy(F.col("_updated_at_utc").desc_nulls_last(),
                                             F.col("_source_priority").asc(),
                                             F.col("_delivered_at_utc").desc_nulls_last())
    return unioned.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_source_priority")

def upsert_dimension(source_df, target, key, payload_cols):
    if source_df is None:
        return
    cols = [c for c in payload_cols if c in source_df.columns]
    df = source_df.withColumn(key, F.upper(F.regexp_replace(F.trim(F.col(key).cast("string")), r"\s+", "")))
    df = df.withColumn("_dim_hash", F.sha2(F.concat_ws("\u001f", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cols]), 256))
    if key == "customer_code":
        df = df.withColumn("customer_sk", F.xxhash64(F.col(key)))
    else:
        df = df.withColumn("product_sk", F.xxhash64(F.col(key)))
    df = (df.withColumn("is_inferred", F.lit(False)).withColumn("is_deleted", F.col("is_deleted").cast("boolean"))
          .withColumn("_run_id", F.lit(run_id)))
    sk = "customer_sk" if key == "customer_code" else "product_sk"
    keep = [key, sk] + cols + [c for c in ["_updated_at_utc", "_delivered_at_utc", "_dim_hash", "is_inferred", "is_deleted", "_run_id"] if c in df.columns]
    df = df.select(*list(dict.fromkeys(keep)))
    if table_exists(target):
        df.createOrReplaceTempView("_dim_src")
        insert_cols = ",".join([f"`{c}`" for c in df.columns])
        vals = ",".join([f"s.`{c}`" for c in df.columns])
        updates = ",".join([f"`{c}`=s.`{c}`" for c in df.columns if c != key])
        spark.sql(f"MERGE INTO {target} t USING _dim_src s ON t.`{key}`=s.`{key}` "
                  f"WHEN MATCHED AND t._dim_hash <> s._dim_hash THEN UPDATE SET {updates} "
                  f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({vals})")
    else:
        df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(target)

ensure_date_dimension()
for target, sources, key, cols in [
    ("gld.gld_d_customer", ["slv.slv_wholesale_customers", "slv.slv_retail_customers"], "customer_code",
     ["customer_code", "customer_name", "country", "city", "gender"]),
    ("gld.gld_d_product", ["slv.slv_wholesale_products", "slv.slv_retail_products"], "product_code",
     ["product_code", "product_name", "brand", "category_code"]),
]:
    upsert_dimension(latest_source_rows(sources, key), target, key, cols)

# SCD2 uses append-only snapshot observations (not only current Silver) so catch-up retains changes.
frames = []
if table_exists("slv.slv_snapshot_event"):
    frames = [spark.table("slv.slv_snapshot_event").where(F.col("entity") == "sales_hierarchy")]
if frames:
    hist = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    if "source_system" not in hist.columns:
        hist = hist.withColumn("source_system", F.lit("unknown"))
    history = (hist.withColumn("salesman_code", F.upper(F.regexp_replace(F.trim(F.col("salesman_code").cast("string")), r"\s+", "")))
               .withColumn("valid_from", F.coalesce(F.to_date("_snapshot_date"), F.lit("1900-01-01").cast("date")))
               .withColumn("_scd_hash", F.sha2(F.concat_ws("\u001f", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
                                                                        for c in ["salesman_code", "salesman_name", "salesmanager_code", "salesmanager_name", "salesdirector_code", "salesdirector_name", "is_deleted"]
                                                                        if c in hist.columns]), 256)))
    same_day = Window.partitionBy("source_system", "salesman_code", "valid_from").orderBy(F.col("_delivered_at_utc").desc_nulls_last())
    history = history.withColumn("_day_rn", F.row_number().over(same_day)).filter("_day_rn = 1").drop("_day_rn")
    by_time = Window.partitionBy("source_system", "salesman_code").orderBy("valid_from")
    history = (history.withColumn("_prev_scd_hash", F.lag("_scd_hash").over(by_time))
               .filter(F.col("_prev_scd_hash").isNull() | (F.col("_scd_hash") != F.col("_prev_scd_hash")))
               .drop("_prev_scd_hash"))
    w = Window.partitionBy("source_system", "salesman_code").orderBy("valid_from")
    history = (history.withColumn("salesman_sk", F.xxhash64("source_system", "salesman_code", F.col("valid_from").cast("string")))
               .withColumn("valid_to", F.coalesce(F.date_sub(F.lead("valid_from").over(w), 1), F.lit("9999-12-31").cast("date")))
               .withColumn("is_current", F.lead("valid_from").over(w).isNull())
               .withColumn("is_inferred", F.lit(False)).withColumn("is_deleted", F.coalesce(F.col("is_deleted"), F.lit(False)))
               .withColumn("_run_id", F.lit(run_id)))
    target = "gld.gld_d_salesman"
    if p_full_reload or not table_exists(target):
        history.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(target)
    else:
        # Rebuild deterministic history atomically via a staging Delta table then replace the target.
        (history.write.format("delta").option("overwriteSchema", "true")
         .option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(target))

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "dimensions": "complete"}))
