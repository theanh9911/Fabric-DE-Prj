# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_config
# Config-as-code. Change and review these specs through Git; there are no cfg_* tables.

# CELL ********************

from datetime import date

SOURCES = [
    dict(source_system="wholesale", entity="orders", load_strategy="db_incremental",
         source_object="dbo.orders", watermark_column="updated_at", source_clock="virtual",
         source_timezone="UTC", commit_lag_minutes=0, lookback_minutes=1440,
         kind="event", business_keys=["order_no", "product_code"],
         event_keys=["order_no", "product_code", "order_status", "updated_at"],
         order_by="updated_at", expected="daily", owner="erp-team", load_order=50,
         is_active=True),
    dict(source_system="wholesale", entity="customers", load_strategy="db_full_snapshot",
         source_object="dbo.customers", kind="snapshot", business_keys=["customer_code"],
         order_by=None, source_timezone="UTC", expected="on_change", owner="erp-team",
         load_order=20, is_active=True),
    dict(source_system="wholesale", entity="products", load_strategy="db_full_snapshot",
         source_object="dbo.products", kind="snapshot", business_keys=["product_code"],
         order_by=None, source_timezone="UTC", expected="on_change", owner="erp-team",
         load_order=20, is_active=True),
    dict(source_system="wholesale", entity="sales_hierarchy", load_strategy="db_full_snapshot",
         source_object="dbo.sales_hierarchy", kind="snapshot", business_keys=["salesman_code"],
         order_by=None, source_timezone="UTC", expected="on_change", owner="erp-team",
         load_order=20, is_active=True),
    dict(source_system="retail", entity="orders", load_strategy="file_new_or_changed",
         source_object="inbound/retail/orders", kind="event",
         business_keys=["order_no", "product_code"],
         event_keys=["order_no", "product_code", "order_status", "updated_at"],
         order_by="updated_at", source_timezone="Asia/Ho_Chi_Minh", expected="daily",
         settle_minutes=5, lookback_minutes=1440, owner="retail-team", load_order=50,
         is_active=True),
    dict(source_system="retail", entity="customers", load_strategy="file_new_or_changed",
         source_object="inbound/retail/customers", kind="snapshot",
         business_keys=["customer_code"], order_by=None,
         source_timezone="Asia/Ho_Chi_Minh", expected="on_change", settle_minutes=5,
         lookback_minutes=1440, owner="retail-team", load_order=20, is_active=True),
    dict(source_system="retail", entity="products", load_strategy="file_new_or_changed",
         source_object="inbound/retail/products", kind="snapshot",
         business_keys=["product_code"], order_by=None,
         source_timezone="Asia/Ho_Chi_Minh", expected="on_change", settle_minutes=5,
         lookback_minutes=1440, owner="retail-team", load_order=20, is_active=True),
    dict(source_system="retail", entity="sales_hierarchy", load_strategy="file_new_or_changed",
         source_object="inbound/retail/sales_hierarchy", kind="snapshot",
         business_keys=["salesman_code"], order_by=None,
         source_timezone="Asia/Ho_Chi_Minh", expected="on_change", settle_minutes=5,
         lookback_minutes=1440, owner="retail-team", load_order=20, is_active=True),
    dict(source_system="reference", entity="categories", load_strategy="file_new_or_changed",
         source_object="inbound/reference/categories", kind="snapshot",
         business_keys=["category_code"], order_by=None,
         source_timezone="Asia/Ho_Chi_Minh", expected="on_change", settle_minutes=5,
         lookback_minutes=1440, owner="data-team", load_order=10, is_active=True),
]

TABLES = [
    dict(table_name="gld.gld_d_date", notebook="nb_gld_dims", depends_on=[],
         load_pattern="date_dimension", business_keys=["date_key"], cluster_by=None,
         dq=dict(not_null="date_key,date_value", unique="date_key"),
         owner="de-team", is_active=True),
    dict(table_name="gld.gld_d_customer", notebook="nb_gld_dims",
         depends_on=["slv.slv_wholesale_customers", "slv.slv_retail_customers"],
         load_pattern="dim_scd1", business_keys=["customer_code"], cluster_by=None,
         dq=dict(not_null="customer_sk,customer_code", unique="customer_sk"),
         owner="de-team", is_active=True),
    dict(table_name="gld.gld_d_product", notebook="nb_gld_dims",
         depends_on=["slv.slv_wholesale_products", "slv.slv_retail_products"],
         load_pattern="dim_scd1", business_keys=["product_code"], cluster_by=None,
         dq=dict(not_null="product_sk,product_code", unique="product_sk"),
         owner="de-team", is_active=True),
    dict(table_name="gld.gld_d_salesman", notebook="nb_gld_dims",
         depends_on=["slv.slv_wholesale_sales_hierarchy", "slv.slv_retail_sales_hierarchy"],
         load_pattern="dim_scd2", business_keys=["source_system,salesman_code,valid_from"],
         cluster_by=None, dq=dict(not_null="salesman_sk,salesman_code", unique="salesman_sk"),
         owner="de-team", is_active=True),
    dict(table_name="gld.gld_f_sales_line", notebook="nb_gld_fact",
         depends_on=["slv.slv_wholesale_orders", "slv.slv_retail_orders",
                     "gld.gld_d_customer", "gld.gld_d_product", "gld.gld_d_salesman",
                     "gld.gld_d_date"],
         load_pattern="fact_merge", business_keys=["order_no", "product_code"],
         cluster_by=None,
         dq=dict(not_null="order_no,product_code,date_key", unique="order_no,product_code",
                 quarantine_max_pct=5), owner="de-team", is_active=True),
    dict(table_name="gld.gld_a_sales_month", notebook="nb_gld_fact",
         depends_on=["gld.gld_f_sales_line"], load_pattern="aggregate_replace_month",
         business_keys=["month_key,product_sk,customer_sk,salesman_sk"], cluster_by=None,
         dq=dict(not_null="month_key", unique="month_key,product_sk,customer_sk,salesman_sk"),
         owner="de-team", is_active=True),
]

def validate_config(sources, tables):
    source_keys = [(s["source_system"], s["entity"]) for s in sources]
    if len(source_keys) != len(set(source_keys)):
        raise ValueError("Duplicate source/entity in SOURCES")
    table_names = [t["table_name"] for t in tables]
    if len(table_names) != len(set(table_names)):
        raise ValueError("Duplicate table_name in TABLES")
    known = set(table_names) | {
        f"slv.slv_{s['source_system']}_{s['entity']}" for s in sources
    }
    for s in sources:
        required = {"source_system", "entity", "load_strategy", "source_object", "kind",
                    "business_keys", "load_order", "is_active"}
        missing = required - set(s)
        if missing:
            raise ValueError(f"{s.get('source_system')}.{s.get('entity')} missing {missing}")
        if s["kind"] not in ("event", "snapshot"):
            raise ValueError(f"Invalid kind for {s['source_system']}.{s['entity']}")
    for table in tables:
        unknown = set(table.get("depends_on", [])) - known
        if unknown:
            raise ValueError(f"{table['table_name']} has unknown dependencies: {unknown}")
    return True

assert validate_config(SOURCES, TABLES)
print(f"Validated {len(SOURCES)} source entities and {len(TABLES)} Gold tables")
