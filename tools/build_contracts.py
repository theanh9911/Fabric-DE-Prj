"""Validate contracts/**/*.yaml and generate fabric/platform/nb_contracts.Notebook.

Spec: docs/03-contract.md §6.

    uv run tools/build_contracts.py            generate
    uv run tools/build_contracts.py --check    validate; fail if nb_contracts differs from the YAML
    uv run tools/build_contracts.py --strict   as --check, and fail on status: draft
"""

import json
import re
import sys
import uuid
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_DIR = ROOT / "contracts"
NB_DIR = ROOT / "fabric" / "platform" / "nb_contracts.Notebook"
NB_FILE = NB_DIR / "notebook-content.py"
PLATFORM_FILE = NB_DIR / ".platform"

TOP_FIELDS = {"apiVersion", "kind", "id", "version", "status", "description", "schema", "team",
              "support", "customProperties"}
DESCRIPTION_FIELDS = {"purpose", "limitations", "usage"}
SCHEMA_FIELDS = {"name", "physicalName", "physicalType", "logicalType", "description", "properties",
                 "quality"}
PROPERTY_FIELDS = {"name", "logicalType", "physicalType", "required", "primaryKey", "description"}
QUALITY_FIELDS = {"type", "description", "query", "mustBe", "severity"}
TEAM_FIELDS = {"username", "role"}
SUPPORT_FIELDS = {"channel", "tool"}

LOGICAL_TYPES = {"string", "integer", "number", "date", "timestamp"}
STATUSES = {"draft", "active", "retired"}
LOAD_STRATEGIES = {"db_incremental", "db_full_snapshot", "file_new_or_changed"}
KINDS = {"event", "snapshot"}
SOURCE_CLOCKS = {"virtual", "real"}
CADENCES = {"daily", "on_change"}

# property -> (python key, type check)
INT_PROPS = {"commitLagMinutes", "lookbackMinutes", "settleMinutes", "quarantineWarnPct",
             "quarantineFailPct"}
STR_PROPS = {"watermarkColumn", "fileGlob", "sourceTimezone", "silverTable"}
LIST_PROPS = {"timestampFormats", "dateFormats"}
ENUM_PROPS = {"loadStrategy": LOAD_STRATEGIES, "kind": KINDS, "sourceClock": SOURCE_CLOCKS,
              "deliveryCadence": CADENCES}
ALL_PROPS = INT_PROPS | STR_PROPS | LIST_PROPS | set(ENUM_PROPS) | {"availableByLocalTime"}
ALWAYS_PROPS = {"loadStrategy", "kind", "silverTable", "deliveryCadence", "quarantineWarnPct",
                "quarantineFailPct"}

PARAM_ORDER = [
    ("load_strategy", "loadStrategy"), ("kind", "kind"), ("silver_table", "silverTable"),
    ("watermark_column", "watermarkColumn"), ("source_clock", "sourceClock"),
    ("commit_lag_minutes", "commitLagMinutes"), ("lookback_minutes", "lookbackMinutes"),
    ("file_glob", "fileGlob"), ("settle_minutes", "settleMinutes"),
    ("source_timezone", "sourceTimezone"), ("timestamp_formats", "timestampFormats"),
    ("date_formats", "dateFormats"), ("delivery_cadence", "deliveryCadence"),
    ("available_by_local_time", "availableByLocalTime"),
    ("quarantine_warn_pct", "quarantineWarnPct"), ("quarantine_fail_pct", "quarantineFailPct"),
]


def is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def nonempty_str(v):
    return isinstance(v, str) and v.strip() != ""


class Errors:
    def __init__(self):
        self.items = []

    def add(self, file, code, msg):
        self.items.append(f"{file}: {code}: {msg}")


def unknown_fields(errs, file, where, obj, allowed):
    for k in sorted(set(obj) - allowed):
        errs.add(file, "BC02", f"{where}: trường '{k}' không thuộc profile")


def validate(path, doc, errs):
    """Validate one contract. Returns the generated dict, or None if BC01 failed."""
    rel = path.relative_to(ROOT).as_posix()
    n0 = len(errs.items)

    if not isinstance(doc, dict):
        errs.add(rel, "BC01", "gốc file phải là mapping")
        return None
    for k in sorted(TOP_FIELDS - set(doc)):
        errs.add(rel, "BC01", f"thiếu trường '{k}'")
    unknown_fields(errs, rel, "file", doc, TOP_FIELDS)
    if len(errs.items) > n0:
        return None

    # --- cấp file
    if doc["apiVersion"] != "v3.0.2":
        errs.add(rel, "BC01", "apiVersion phải là v3.0.2")
    if doc["kind"] != "DataContract":
        errs.add(rel, "BC01", "kind phải là DataContract")
    if not (isinstance(doc["version"], str) and re.fullmatch(r"\d+\.\d+\.\d+", doc["version"])):
        errs.add(rel, "BC01", "version phải là semver dạng chuỗi 'x.y.z'")
    if doc["status"] not in STATUSES:
        errs.add(rel, "BC01", f"status phải thuộc {sorted(STATUSES)}")

    desc = doc["description"]
    if not isinstance(desc, dict):
        errs.add(rel, "BC01", "description phải là mapping")
    else:
        for k in sorted(DESCRIPTION_FIELDS - set(desc)):
            errs.add(rel, "BC01", f"description thiếu '{k}'")
        unknown_fields(errs, rel, "description", desc, DESCRIPTION_FIELDS)
        for k in DESCRIPTION_FIELDS & set(desc):
            if not nonempty_str(desc[k]):
                errs.add(rel, "BC01", f"description.{k} phải là chuỗi không rỗng")

    owner = channel = None
    team = doc["team"]
    if not (isinstance(team, list) and team and all(isinstance(t, dict) for t in team)):
        errs.add(rel, "BC01", "team phải là danh sách ≥ 1 phần tử")
    else:
        for t in team:
            unknown_fields(errs, rel, "team", t, TEAM_FIELDS)
            if not (nonempty_str(t.get("username")) and nonempty_str(t.get("role"))):
                errs.add(rel, "BC01", "team[] cần username và role")
        owners = [t["username"] for t in team if t.get("role") == "owner" and nonempty_str(t.get("username"))]
        if not owners:
            errs.add(rel, "BC01", "team cần ≥ 1 phần tử role: owner")
        else:
            owner = owners[0]
    support = doc["support"]
    if not (isinstance(support, list) and support and all(isinstance(s, dict) for s in support)):
        errs.add(rel, "BC01", "support phải là danh sách ≥ 1 phần tử")
    else:
        for s in support:
            unknown_fields(errs, rel, "support", s, SUPPORT_FIELDS)
            if not (nonempty_str(s.get("channel")) and nonempty_str(s.get("tool"))):
                errs.add(rel, "BC01", "support[] cần channel và tool")
        channel = next((s["channel"] for s in support if nonempty_str(s.get("channel"))), None)

    # --- schema
    schema = doc["schema"]
    if not (isinstance(schema, list) and len(schema) == 1 and isinstance(schema[0], dict)):
        errs.add(rel, "BC01", "schema phải có đúng một object")
        return None
    obj = schema[0]
    for k in sorted((SCHEMA_FIELDS - {"quality"}) - set(obj)):
        errs.add(rel, "BC01", f"schema[0] thiếu '{k}'")
    unknown_fields(errs, rel, "schema[0]", obj, SCHEMA_FIELDS)
    if obj.get("physicalType") not in {"table", "file"}:
        errs.add(rel, "BC01", "schema[0].physicalType phải là table hoặc file")
    if obj.get("logicalType") != "object":
        errs.add(rel, "BC01", "schema[0].logicalType phải là object")
    for k in ("name", "physicalName", "description"):
        if k in obj and not nonempty_str(obj[k]):
            errs.add(rel, "BC01", f"schema[0].{k} phải là chuỗi không rỗng")

    props = obj.get("properties")
    columns = []
    if not (isinstance(props, list) and props and all(isinstance(p, dict) for p in props)):
        errs.add(rel, "BC01", "schema[0].properties phải là danh sách ≥ 1 phần tử")
        props = []
    names = set()
    for p in props:
        for k in sorted((PROPERTY_FIELDS - {"primaryKey"}) - set(p)):
            errs.add(rel, "BC01", f"property '{p.get('name', '?')}' thiếu '{k}'")
        unknown_fields(errs, rel, f"property '{p.get('name', '?')}'", p, PROPERTY_FIELDS)
        if p.get("logicalType") not in LOGICAL_TYPES:
            errs.add(rel, "BC01", f"property '{p.get('name')}': logicalType phải thuộc {sorted(LOGICAL_TYPES)}")
        if not isinstance(p.get("required"), bool):
            errs.add(rel, "BC01", f"property '{p.get('name')}': required phải là bool")
        if "primaryKey" in p and not isinstance(p["primaryKey"], bool):
            errs.add(rel, "BC01", f"property '{p.get('name')}': primaryKey phải là bool")
        for k in ("name", "physicalType", "description"):
            if k in p and not nonempty_str(p[k]):
                errs.add(rel, "BC01", f"property '{p.get('name')}': {k} phải là chuỗi không rỗng")
        if p.get("name") in names:
            errs.add(rel, "BC01", f"property '{p.get('name')}' bị trùng")
        names.add(p.get("name"))
        columns.append({"name": p.get("name"), "logical_type": p.get("logicalType"),
                        "physical_type": p.get("physicalType"), "required": p.get("required"),
                        "primary_key": bool(p.get("primaryKey", False))})

    # --- customProperties
    cp_list = doc["customProperties"]
    cp = {}
    if not (isinstance(cp_list, list) and all(isinstance(e, dict) for e in cp_list)):
        errs.add(rel, "BC01", "customProperties phải là danh sách {property, value}")
        cp_list = []
    for e in cp_list:
        unknown_fields(errs, rel, "customProperties[]", e, {"property", "value"})
        if "property" not in e or "value" not in e:
            errs.add(rel, "BC01", "customProperties[] cần property và value")
            continue
        name = e["property"]
        if name not in ALL_PROPS:
            errs.add(rel, "BC02", f"customProperties: '{name}' không thuộc profile")
        elif name in cp:
            errs.add(rel, "BC04", f"customProperties: '{name}' bị trùng")
        else:
            cp[name] = e["value"]

    for name, v in cp.items():
        if name in ENUM_PROPS and v not in ENUM_PROPS[name]:
            errs.add(rel, "BC04", f"{name} phải thuộc {sorted(ENUM_PROPS[name])}")
        elif name in INT_PROPS and not (is_int(v) and v >= 0):
            errs.add(rel, "BC04", f"{name} phải là số nguyên ≥ 0")
        elif name in STR_PROPS and not nonempty_str(v):
            errs.add(rel, "BC04", f"{name} phải là chuỗi không rỗng")
        elif name in LIST_PROPS and not (isinstance(v, list) and v and all(nonempty_str(x) for x in v)):
            errs.add(rel, "BC04", f"{name} phải là danh sách chuỗi không rỗng")
        elif name == "availableByLocalTime" and not (isinstance(v, str) and re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", v)):
            errs.add(rel, "BC04", "availableByLocalTime phải là chuỗi 'HH:mm' (đặt trong ngoặc kép)")

    strategy = cp.get("loadStrategy")
    logical_types = {c["logical_type"] for c in columns}
    applicable = set(ALWAYS_PROPS)
    if strategy == "db_incremental":
        applicable |= {"watermarkColumn", "sourceClock", "commitLagMinutes", "lookbackMinutes"}
    if strategy == "file_new_or_changed":
        applicable |= {"fileGlob", "settleMinutes"}
    if "timestamp" in logical_types:
        applicable |= {"sourceTimezone", "timestampFormats"}
    if "date" in logical_types:
        applicable.add("dateFormats")
    if cp.get("deliveryCadence") == "daily":
        applicable.add("availableByLocalTime")
    for name in sorted(applicable - set(cp)):
        errs.add(rel, "BC04", f"customProperties thiếu '{name}'")
    for name in sorted(set(cp) - applicable):
        errs.add(rel, "BC04", f"customProperties thừa '{name}' (không áp dụng cho contract này)")

    # --- BC03
    expected_id = f"{path.parent.name}.{path.stem}"
    if doc["id"] != expected_id:
        errs.add(rel, "BC03", f"id '{doc['id']}' phải là '{expected_id}'")

    # --- BC05
    ptype = obj.get("physicalType")
    if strategy in {"db_incremental", "db_full_snapshot"} and ptype != "table":
        errs.add(rel, "BC05", f"loadStrategy {strategy} cần physicalType: table")
    if strategy == "file_new_or_changed" and ptype != "file":
        errs.add(rel, "BC05", "loadStrategy file_new_or_changed cần physicalType: file")

    # --- BC06
    wm = cp.get("watermarkColumn")
    if strategy == "db_incremental" and wm is not None:
        col = next((c for c in columns if c["name"] == wm), None)
        if col is None or col["logical_type"] != "timestamp" or col["required"] is not True:
            errs.add(rel, "BC06", f"watermarkColumn '{wm}' phải là property timestamp và required: true")

    # --- BC07
    if cp.get("kind") == "snapshot":
        pks = [c["name"] for c in columns if c["primary_key"]]
        if len(pks) != 1:
            errs.add(rel, "BC07", f"kind: snapshot cần đúng một primaryKey, hiện có {len(pks)}")

    # --- quality (BC08)
    quality = []
    q_list = obj.get("quality", [])
    if not isinstance(q_list, list):
        errs.add(rel, "BC01", "schema[0].quality phải là danh sách")
        q_list = []
    for i, q in enumerate(q_list, start=1):
        if not isinstance(q, dict):
            errs.add(rel, "BC01", f"quality[{i}] phải là mapping")
            continue
        for k in sorted((QUALITY_FIELDS - {"mustBe"}) - set(q)):
            errs.add(rel, "BC01", f"quality[{i}] thiếu '{k}'")
        if "mustBe" not in q:
            errs.add(rel, "BC01", f"quality[{i}] thiếu 'mustBe'")
        unknown_fields(errs, rel, f"quality[{i}]", q, QUALITY_FIELDS)
        if q.get("type") != "sql":
            errs.add(rel, "BC08", f"quality[{i}].type phải là sql")
        if not (isinstance(q.get("query"), str) and "${object}" in q["query"]):
            errs.add(rel, "BC08", f"quality[{i}].query phải chứa ${{object}}")
        if q.get("severity") not in {"warning", "error"}:
            errs.add(rel, "BC08", f"quality[{i}].severity phải là warning hoặc error")
        quality.append({"check_id": f"CT-{doc['id']}-{i:02d}",
                        "description": str(q.get("description", "")).strip(),
                        "query": " ".join(str(q.get("query", "")).split()),
                        "must_be": q.get("mustBe"), "severity": q.get("severity")})

    # --- BC09
    warn, fail = cp.get("quarantineWarnPct"), cp.get("quarantineFailPct")
    if is_int(warn) and is_int(fail) and not warn < fail:
        errs.add(rel, "BC09", "quarantineWarnPct phải nhỏ hơn quarantineFailPct")

    if len(errs.items) > n0:
        return None

    source_system, entity = doc["id"].split(".", 1)
    out = {
        "id": doc["id"], "version": doc["version"], "status": doc["status"],
        "source_system": source_system, "entity": entity,
        "physical_name": obj["physicalName"], "physical_type": ptype,
        "owner": owner, "support_channel": channel,
        "columns": columns, "quality": quality,
    }
    for py_key, prop in PARAM_ORDER:
        out[py_key] = cp.get(prop)
    return out


def load_all(errs):
    files = sorted(CONTRACTS_DIR.glob("*/*.yaml"))
    if not files:
        errs.add("contracts", "BC01", "không có file contract nào")
    result, seen = {}, {}
    for f in files:
        rel = f.relative_to(ROOT).as_posix()
        try:
            doc = yaml.safe_load(f.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            errs.add(rel, "BC01", f"YAML không đọc được: {e}")
            continue
        c = validate(f, doc, errs)
        if c is None:
            continue
        if c["id"] in seen:
            errs.add(rel, "BC03", f"id '{c['id']}' trùng với {seen[c['id']]}")
            continue
        seen[c["id"]] = rel
        result[c["id"]] = c
    return dict(sorted(result.items()))


# --- sinh code -----------------------------------------------------------------------------

def lit(v):
    if v is None:
        return "None"
    if v is True:
        return "True"
    if v is False:
        return "False"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ", ".join(lit(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{lit(k)}: {lit(x)}" for k, x in v.items()) + "}"
    raise TypeError(f"không biểu diễn được {type(v).__name__}")


def render(contracts):
    lines = ["# GENERATED by tools/build_contracts.py from contracts/**/*.yaml — DO NOT EDIT",
             "CONTRACTS = {"]
    for cid, c in contracts.items():
        lines.append(f"    {lit(cid)}: {{")
        for k, v in c.items():
            if k in ("columns", "quality"):
                if v:
                    lines.append(f"        {lit(k)}: [")
                    lines.extend(f"            {lit(item)}," for item in v)
                    lines.append("        ],")
                else:
                    lines.append(f"        {lit(k)}: [],")
            else:
                lines.append(f"        {lit(k)}: {lit(v)},")
        lines.append("    },")
    lines.append("}")
    code = "\n".join(lines)

    meta_lang = ['# META {', '# META   "language": "python",', '# META   "language_group": "synapse_pyspark"',
                 '# META }']
    out = [
        "# Fabric notebook source", "",
        "# METADATA ********************", "",
        "# META {", '# META   "kernel_info": {', '# META     "name": "synapse_pyspark"', "# META   },",
        '# META   "dependencies": {}', "# META }", "",
        "# MARKDOWN ********************", "",
        "# # nb_contracts",
        "# Generated by `uv run tools/build_contracts.py` from `contracts/**/*.yaml`. Do not edit by hand.", "",
        "# CELL ********************", "", code, "",
        "# METADATA ********************", "", *meta_lang, "",
    ]
    return "\n".join(out)


def ensure_platform_file():
    if PLATFORM_FILE.exists():
        return
    PLATFORM_FILE.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "nb_contracts"},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
    }
    PLATFORM_FILE.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8", newline="\n")


def main(argv):
    check = "--check" in argv or "--strict" in argv
    strict = "--strict" in argv
    unknown = [a for a in argv if a not in {"--check", "--strict"}]
    if unknown:
        print(f"tham số không hợp lệ: {unknown}", file=sys.stderr)
        return 2

    errs = Errors()
    contracts = load_all(errs)
    if strict:
        for c in contracts.values():
            if c["status"] == "draft":
                errs.add(f"contracts/{c['source_system']}/{c['entity']}.yaml", "BC10", "status còn là draft")
    if errs.items:
        print("\n".join(errs.items), file=sys.stderr)
        return 1

    text = render(contracts)
    if check:
        current = NB_FILE.read_text(encoding="utf-8").replace("\r\n", "\n") if NB_FILE.exists() else None
        if current != text:
            print(f"{NB_FILE.relative_to(ROOT).as_posix()}: nb_contracts lệch YAML — chạy "
                  "`uv run tools/build_contracts.py`", file=sys.stderr)
            return 1
        print(f"OK: {len(contracts)} contract, nb_contracts khớp")
        return 0

    ensure_platform_file()
    NB_FILE.write_text(text, encoding="utf-8", newline="\n")
    print(f"Wrote {NB_FILE.relative_to(ROOT).as_posix()} ({len(contracts)} contract)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
