# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_00_sim_reset
#
# - **Mục đích:** đưa Source về trạng thái rỗng để chạy lại simulator từ đầu (initial load lại).
# - **Xoá:** dữ liệu `dbo.*` trong ERP, file trong `lh_retail_drop/Files/inbound/`, `sim_state`, `sim_release_log`, `sim_reject_log`.
# - **Giữ:** seed (`Files/seed`, `seed_*`), schema ERP.
# - ⚠️ Platform đang giữ watermark/manifest của dữ liệu cũ → reset Source thì cũng phải reset state phía Platform.
# - Chốt an toàn: chỉ chạy khi `p_confirm = "RESET"`.

# PARAMETERS CELL ********************

p_confirm = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %run nb_00_sim_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if p_confirm != "RESET":
    notebookutils.notebook.exit("SKIPPED: set p_confirm = 'RESET' to run")

# Orders trước (bảng con), master sau
erp_exec("; ".join(f"DELETE FROM dbo.[{e}]" for e in reversed(list(ENTITIES))) + ";")

for entity in ENTITIES:
    folder = inbound_dir(entity)
    if notebookutils.fs.exists(folder):
        notebookutils.fs.rm(folder, True)

for table in ["sim_state", "sim_release_log", "sim_reject_log"]:
    path = sim_table(table)
    if notebookutils.fs.exists(path):
        notebookutils.fs.rm(path, True)

print("Source reset done.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
