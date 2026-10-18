# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "79063a99-fe42-4078-8659-25c7c2894c06",
# META       "default_lakehouse_name": "lh_platform",
# META       "default_lakehouse_workspace_id": "02594272-f3b4-47d6-a983-347bdaefe9be",
# META       "known_lakehouses": [
# META         {
# META           "id": "79063a99-fe42-4078-8659-25c7c2894c06"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_ops_delivery
# Đường duy nhất để người đổi trạng thái delivery (L13). `park`: delivery `FAILED` hoặc `LANDED` ⇒ `PARKED`;
# `release`: `PARKED` ⇒ `LANDED`; `resolve`: đóng các dòng REJECT đang mở của delivery trong `slv.quarantine`.
# Cần `p_confirm = "CONFIRM"`, `p_operator`, và `p_reason` với `park`/`resolve`. Không chạy khi
# lượt đang chạy; báo nhóm khi chạy (D33). Spec: docs/04-kien-truc.md §10.7.

# PARAMETERS CELL ********************

p_run_id = ""
p_action = ""
p_delivery_id = ""
p_reason = ""
p_operator = ""
p_confirm = ""

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

NB_NAME = "nb_ops_delivery"
import json

RUN_ID = p_run_id or new_id()
if p_confirm != "CONFIRM":
    print('p_confirm khác "CONFIRM": không làm gì.')
    exit_worker({"status": "NOOP", "reason": "not_confirmed"})
if p_action not in ("park", "release", "resolve"):
    raise ValueError(f"p_action phải là park, release hoặc resolve, nhận {p_action!r}")
if not p_operator.strip():
    raise ValueError("p_operator bắt buộc")
if p_action in ("park", "resolve") and not p_reason.strip():
    raise ValueError(f"p_reason bắt buộc với {p_action}")


def runtime_user():
    """Danh tính thật của người/dịch vụ chạy notebook; p_operator là chữ người gõ nên chỉ là ghi chú."""
    try:
        ctx = notebookutils.runtime.context
        return ctx.get("userName") or ctx.get("userId") or "unknown"
    except Exception:  # noqa: BLE001 — không có context thì vẫn phải ghi được hành động
        return "unknown"

IDS = [x.strip() for x in p_delivery_id.split(",") if x.strip()]
if not IDS:
    raise ValueError("p_delivery_id trống")

done, refused = [], []
for did in IDS:
    rows = spark.sql(
        "SELECT *, date_format(delivered_at_utc, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS delivered_at_str, "
        "date_format(snapshot_date, 'yyyy-MM-dd') AS snapshot_date_str "
        f"FROM meta.v_delivery_state WHERE delivery_id = '{did}'").collect()
    if not rows:
        refused.append({"delivery_id": did, "why": "không tồn tại"})
        continue
    d = rows[0].asDict()
    if p_action == "resolve":
        resolve_quarantine(did, RUN_ID)
    elif p_action == "park":
        if d["status"] not in ("FAILED", "LANDED"):
            refused.append({"delivery_id": did, "why": f"status {d['status']} không park được"})
            continue
        record_delivery(d, "PARKED", RUN_ID, error_message=f"parked by {p_operator}: {p_reason}")
    else:
        if d["status"] != "PARKED":
            refused.append({"delivery_id": did, "why": f"status {d['status']} không phải PARKED"})
            continue
        record_delivery(d, "LANDED", RUN_ID)
    done.append(did)
log_run(RUN_ID, f"ops.{p_action}", "ops", "SUCCEEDED" if done else "FAILED",
        params={"delivery_ids": IDS, "reason": p_reason, "operator": p_operator, "actor": runtime_user(),
                "done": done, "refused": refused},
        error_message=None if done else "không delivery nào được xử lý")
print(json.dumps({"done": done, "refused": refused}, ensure_ascii=False))
exit_worker({"status": "SUCCEEDED" if done else "REFUSED", "done": done, "refused": refused})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
