"""Repo lint for the Fabric platform. Spec: docs/05-thu-vien.md §6.2.

    uv run tools/check_repo.py

Prints `file:line: Cxx message` per finding; exits 1 if any.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FABRIC = ROOT / "fabric"
DOCS = ROOT / "docs"

CELL_MARKERS = ("# CELL ********************", "# PARAMETERS CELL ********************",
                "# MARKDOWN ********************")
META_MARKER = "# METADATA ********************"
UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

findings = []


def rel(p):
    return Path(p).resolve().relative_to(ROOT).as_posix()


def add(path, line, code, msg):
    findings.append(f"{rel(path)}:{line}: {code} {msg}")


def read(path):
    return Path(path).read_text(encoding="utf-8").replace("\r\n", "\n")


def items():
    """Every Fabric item directory (has a `.platform` or a `<name>.<Type>` suffix)."""
    return sorted(p for p in FABRIC.glob("*/*") if p.is_dir() and "." in p.name)


def notebooks():
    return sorted(FABRIC.glob("*/*.Notebook/notebook-content.py"))


def cells(text):
    """Yield (start_line, marker, [(lineno, line), ...]) for each cell of a notebook source."""
    cur = None
    for no, line in enumerate(text.split("\n"), start=1):
        if line in CELL_MARKERS:
            if cur:
                yield cur
            cur = (no, line, [])
        elif line == META_MARKER:
            if cur:
                yield cur
            cur = None
        elif cur is not None:
            cur[2].append((no, line))
    if cur:
        yield cur


def python_source(text):
    keep = []
    for line in text.split("\n"):
        s = line.lstrip()
        if s.startswith("%") or s.startswith("# MAGIC"):
            keep.append("")
        else:
            keep.append(line)
    return "\n".join(keep)


def check_notebooks():
    worker_names = {"nb_land"}
    cfg = FABRIC / "platform" / "nb_config.Notebook" / "notebook-content.py"
    if cfg.exists():
        worker_names |= set(re.findall(r'notebook="(nb_\w+)"', read(cfg)))

    for path in notebooks():
        text = read(path)
        name = path.parent.name.removesuffix(".Notebook")
        if not text.startswith("# Fabric notebook source"):
            add(path, 1, "C01", "notebook-content.py không bắt đầu bằng '# Fabric notebook source'")

        for start, _marker, lines in cells(text):
            body = [(n, l) for n, l in lines if l.strip()]
            if any(l.startswith("%run") for _, l in body) and len(body) > 1:
                add(path, body[0][0], "C02", "%run phải nằm một mình trong cell")
        for no, line in enumerate(text.split("\n"), start=1):
            if "%%configure" in line:
                add(path, no, "C03", "không dùng %%configure (xem docs/TROUBLESHOOTING.md)")

        try:
            compile(python_source(text), str(path), "exec")
        except SyntaxError as e:
            add(path, e.lineno or 1, "C04", f"Python không compile được: {e.msg}")

        for no, line in enumerate(text.split("\n"), start=1):
            if line.startswith("# META") or not UUID_RE.search(line):
                continue
            add(path, no, "C05", "UUID nằm ngoài khối # META (không hardcode item/workspace ID)")

        if name in worker_names:
            if "exit_worker(" not in text:
                add(path, 1, "C06", "notebook worker không gọi exit_worker(")
            for pat, label in ((r"saveAsTable\(", "saveAsTable("), (r"(?i)drop\s+table", "DROP TABLE"),
                               (r"spark\.conf\.set\(", "spark.conf.set("), (r"(?i)jdbc", "jdbc")):
                for no, line in enumerate(text.split("\n"), start=1):
                    if re.search(pat, line):
                        add(path, no, "C06", f"worker không được dùng {label}")


def check_items():
    ids = {}
    for d in items():
        pf = d / ".platform"
        if not pf.exists():
            add(d, 1, "C07", ".platform thiếu")
            continue
        try:
            meta = json.loads(read(pf))
            display = meta["metadata"]["displayName"]
            lid = meta["config"]["logicalId"]
        except (ValueError, KeyError) as e:
            add(pf, 1, "C07", f".platform không hợp lệ: {e}")
            continue
        if display != d.name.rsplit(".", 1)[0]:
            add(pf, 1, "C07", f"displayName '{display}' khác tên thư mục '{d.name}'")
        if lid in ids:
            add(pf, 1, "C07", f"logicalId trùng với {ids[lid]}")
        ids[lid] = rel(pf)

    for pj in sorted(FABRIC.glob("*/*.DataPipeline/pipeline-content.json")):
        try:
            doc = json.loads(read(pj))
        except ValueError as e:
            add(pj, 1, "C08", f"JSON không hợp lệ: {e}")
            continue
        for nid in find_keys(doc, "notebookId"):
            if nid not in ids and nid != "00000000-0000-0000-0000-000000000000":
                add(pj, 1, "C08", f"notebookId {nid} không khớp logicalId nào")


def find_keys(node, key):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key and isinstance(v, str):
                yield v
            else:
                yield from find_keys(v, key)
    elif isinstance(node, list):
        for v in node:
            yield from find_keys(v, key)


def check_links():
    files = sorted(DOCS.glob("*.md")) + [ROOT / "CLAUDE.md", ROOT / "README.md"]
    for f in files:
        if not f.exists():
            continue
        in_fence = False
        for no, line in enumerate(read(f).split("\n"), start=1):
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for target in re.findall(r"\]\(([^)\s]+)\)", line):
                if re.match(r"^(https?:|mailto:|#)", target):
                    continue
                path = target.split("#", 1)[0]
                if path and not (f.parent / path).resolve().exists():
                    add(f, no, "C09", f"link trỏ file không tồn tại: {target}")


def check_contracts():
    script = ROOT / "tools" / "build_contracts.py"
    if not script.exists():
        return
    r = subprocess.run([sys.executable, str(script), "--check"], capture_output=True, text=True,
                       encoding="utf-8")
    if r.returncode != 0:
        for line in (r.stderr or r.stdout).strip().split("\n"):
            findings.append(f"{line} [C10]")


# --- C11, C12: chỉ chạy khi nb_setup_ddl / nb_config là bản v2 ---------------------------------

def ddl_statements(text):
    """name -> normalized statement for CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW."""
    out = {}
    for stmt in re.split(r";", text):
        m = re.search(r"CREATE\s+(?:TABLE\s+IF\s+NOT\s+EXISTS|OR\s+REPLACE\s+VIEW)\s+([\w.<>]+)", stmt, re.I)
        if not m:
            continue
        start = m.start()
        out[m.group(1).lower()] = re.sub(r"\s+", " ", stmt[start:]).strip()
    return out


def sql_blocks(md):
    return "\n".join(re.findall(r"```sql\n(.*?)```", md, re.S))


def strip_magic(text):
    lines = []
    for line in text.split("\n"):
        if line.startswith("# MAGIC"):
            line = line[len("# MAGIC"):].lstrip(" ")
            if line.startswith("%%sql"):
                continue
            lines.append(line)
    return "\n".join(lines)


def table_columns(stmt):
    m = re.search(r"\(", stmt)
    if not m:
        return []
    depth, cur, parts = 0, "", []
    for ch in stmt[m.start():]:
        if ch == "(":
            depth += 1
            if depth == 1:
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        if ch == "," and depth == 1:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    cols = []
    for p in parts:
        tok = p.strip().split()
        if tok:
            cols.append(tok[0].lower())
    return cols


def check_ddl_and_config():
    ddl = FABRIC / "platform" / "nb_setup_ddl.Notebook" / "notebook-content.py"
    cfg = FABRIC / "platform" / "nb_config.Notebook" / "notebook-content.py"
    arch = DOCS / "04-kien-truc.md"
    ddl_v2 = ddl.exists() and "meta.delivery_log" in read(ddl)
    cfg_v2 = cfg.exists() and "def validate_config" in read(cfg)

    ddl_stmts = {}
    if ddl_v2:
        ddl_stmts = ddl_statements(strip_magic(read(ddl)))
        doc_stmts = ddl_statements(sql_blocks(read(arch)))
        for name, stmt in doc_stmts.items():
            if "<" in name:
                continue
            if name not in ddl_stmts:
                add(ddl, 1, "C11", f"{name} có trong 04-kien-truc.md nhưng không có trong nb_setup_ddl")
            elif ddl_stmts[name] != stmt:
                add(ddl, 1, "C11", f"{name} khác khối trong 04-kien-truc.md")

    if cfg_v2 and ddl_v2:
        ns = {}
        sources = [FABRIC / "platform" / "nb_contracts.Notebook" / "notebook-content.py", cfg]
        for src in sources:
            try:
                exec(compile(python_source(read(src)), str(src), "exec"), ns)
            except Exception as e:  # noqa: BLE001 — report any failure of the config notebook as a lint error
                add(src, 1, "C12", f"không nạp được để kiểm tra: {e!r}")
                return
        for spec in ns.get("SILVER", []):
            cols = set(table_columns(ddl_stmts.get(spec["table"], "")))
            if not cols:
                add(cfg, 1, "C12", f"{spec['table']} không có DDL")
                continue
            for c in spec["columns"]:
                for k in ("name", "keep_raw"):
                    v = c.get(k)
                    if v and v.lower() not in cols:
                        add(cfg, 1, "C12", f"{spec['table']}: cột '{v}' không có trong DDL")
        fct = set(table_columns(ddl_stmts.get("gld.fct_order_line", "")))
        statuses = {v.lower() for v in ns.get("ORDER_STATUS", {}).values()}
        fct_status = {c[len("first_"):-len("_at_utc")] for c in fct
                      if c.startswith("first_") and c.endswith("_at_utc")}
        for s in sorted(statuses - fct_status):
            add(cfg, 1, "C12", f"trạng thái '{s}' không có cột first_{s}_at_utc trong gld.fct_order_line")
        for s in sorted(fct_status - statuses):
            add(cfg, 1, "C12", f"cột first_{s}_at_utc không có trạng thái '{s}' trong ORDER_STATUS")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    check_notebooks()
    check_items()
    check_links()
    check_contracts()
    check_ddl_and_config()
    if findings:
        print("\n".join(findings))
        print(f"\n{len(findings)} lỗi")
        return 1
    print("OK: repo sạch")
    return 0


if __name__ == "__main__":
    sys.exit(main())
