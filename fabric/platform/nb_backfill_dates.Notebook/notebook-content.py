# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_backfill_dates
# Sinh danh sách ngày `p_start_date`…`p_end_date` (gồm cả hai đầu) cho `pl_backfill`.

# PARAMETERS CELL ********************

p_start_date = ""
p_end_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date, timedelta

start, end = date.fromisoformat(p_start_date), date.fromisoformat(p_end_date)
if end < start:
    raise ValueError("p_end_date < p_start_date")
if (end - start).days > 400:
    raise ValueError("khoảng quá dài (> 400 ngày)")
dates = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
notebookutils.notebook.exit(json.dumps({"dates": dates}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
