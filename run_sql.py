"""
Run one section of warehouse.sql against RDS MySQL and save the results.

  python3 run_sql.py STAR         build the star schema (DDL + ELT + SCD2 change)
  python3 run_sql.py CHECKS       row counts and SCD2 proof
  python3 run_sql.py ANALYTICS    Part 2b analytical queries
  python3 run_sql.py PROFILE      EXPLAIN ANALYZE every ANALYTICS query (5 runs each) to find the most expensive
  python3 run_sql.py OPTIMIZE     Part 2b: baseline EXPLAIN ANALYZE, apply the optimization, re-measure
  python3 run_sql.py SHOWCASE     Part 4a showcase queries

Results are printed and written to results/<SECTION>.txt.
Needs RDS_HOST and RDS_USER env vars; the password is prompted and never stored.
"""
import getpass
import os
import re
import sys

import pymysql

HERE = os.path.dirname(os.path.abspath(__file__))


def section_sql(name):
    sql = open(os.path.join(HERE, "warehouse.sql"), encoding="utf-8").read()
    try:
        return sql.split(f"-- @@{name}_START")[1].split(f"-- @@{name}_END")[0]
    except IndexError:
        sys.exit(f"section {name} not found in warehouse.sql")


def split_statements(body):
    """Yield (label, statement). A '-- @@QUERY <label>' comment names the next statement."""
    label, buf = None, []
    for line in body.splitlines():
        m = re.match(r"\s*-- @@QUERY (.*)", line)
        if m:
            label = m.group(1).strip(); continue
        buf.append(re.sub(r"--.*$", "", line) if line.strip().startswith("--") else line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                yield label, stmt
            label, buf = None, []


def fmt_table(cols, rows, limit=200):
    rows = [["NULL" if v is None else str(v) for v in r] for r in rows[:limit]]
    w = [max([len(c)] + [len(r[i]) for r in rows]) for i, c in enumerate(cols)]
    line = lambda vals: " | ".join(v.ljust(w[i]) for i, v in enumerate(vals))
    return "\n".join([line(cols), "-+-".join("-" * x for x in w)] + [line(r) for r in rows])


def root_ms(plan):
    m = re.search(r"actual time=[\d.]+\.\.([\d.]+)", plan)
    return float(m.group(1)) if m else float("nan")


def profile(cur, out, runs=5):
    import statistics
    rows = []
    for label, stmt in split_statements(section_sql("ANALYTICS")):
        times, plan = [], ""
        for _ in range(runs):
            cur.execute("EXPLAIN ANALYZE " + stmt)
            plan = cur.fetchone()[0]; times.append(root_ms(plan))
        med = statistics.median(times[1:])
        rows.append((label.split(":")[0], f"{med:.3f}", ", ".join(f"{t:.2f}" for t in times)))
        out.append(f"\n=== EXPLAIN ANALYZE {label}\n{plan}")
    rows.sort(key=lambda r: -float(r[1]))
    block = "\n=== PROFILE: median server-side time of runs 2-5 (ms), most expensive first\n" + fmt_table(["query", "median_ms", "all_runs_ms"], rows)
    print(block); out.insert(0, block)


def measure(cur, stmt, runs=5):
    import statistics
    times, plan = [], ""
    for _ in range(runs):
        cur.execute("EXPLAIN ANALYZE " + stmt)
        plan = cur.fetchone()[0]; times.append(root_ms(plan))
    cur.execute(stmt)
    return statistics.median(times[1:]), times, plan, [tuple(str(v) for v in r) for r in cur.fetchall()]


def optimize(cur, out):
    a1 = next(s for l, s in split_statements(section_sql("ANALYTICS")) if l.startswith("A1"))
    cur.execute("DROP TABLE IF EXISTS mv_zone_conditions_perf")
    b_med, b_runs, b_plan, b_rows = measure(cur, a1)
    out.append(f"\n=== BEFORE: EXPLAIN ANALYZE A1 (original query on fact_order + dimensions)\n{b_plan}")
    a1_mv = None
    for label, stmt in split_statements(section_sql("OPTIMIZE")):
        if label and label.startswith("A1_MV"):
            a1_mv = stmt; continue
        cur.execute(stmt)
        out.append(f"ok ({cur.rowcount} rows): {' '.join(stmt.split())[:90]}")
    a_med, a_runs, a_plan, a_rows = measure(cur, a1_mv)
    out.append(f"\n=== AFTER: EXPLAIN ANALYZE A1_MV (summary table)\n{a_plan}")
    cur.execute("SELECT COUNT(*) FROM mv_zone_conditions_perf"); mv_rows = cur.fetchone()[0]
    summary = fmt_table(["metric", "before (A1)", "after (A1_MV)"], [
        ("median server time, runs 2-5 (ms)", f"{b_med:.3f}", f"{a_med:.3f}"),
        ("all runs (ms)", ", ".join(f"{t:.2f}" for t in b_runs), ", ".join(f"{t:.2f}" for t in a_runs)),
        ("rows read from the base table", "2499 (fact_order)", f"{mv_rows} (mv_zone_conditions_perf)"),
        ("change", "", f"{(a_med - b_med) / b_med * 100:+.1f}%"),
        ("identical result rows (order-insensitive)", "", str(sorted(b_rows) == sorted(a_rows)) + f" ({len(a_rows)} rows)")])
    block = "\n=== OPTIMIZATION SUMMARY\n" + summary
    print(block); out.insert(0, block)
    for title, plan in (("BEFORE (A1)", b_plan), ("AFTER (A1_MV)", a_plan)):
        print(f"\n--- {title} plan, top lines ---\n" + "\n".join(plan.splitlines()[:8]))


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    name = sys.argv[1].upper()
    conn = pymysql.connect(host=os.environ.get("RDS_HOST") or input("RDS endpoint: ").strip(),
                           user=os.environ.get("RDS_USER") or input("Master username: ").strip(),
                           password=os.environ.get("RDS_PASSWORD") or getpass.getpass("Password: "),
                           database="kitchen_dw", port=3306, autocommit=True)
    out = []
    with conn.cursor() as cur:
        if name == "PROFILE":
            profile(cur, out)
        elif name == "OPTIMIZE":
            optimize(cur, out)
        for label, stmt in ([] if name in ("PROFILE", "OPTIMIZE") else split_statements(section_sql(name))):
            cur.execute(stmt)
            if cur.description:
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                block = f"\n=== {label or stmt[:60]}  ({len(rows)} rows)\n" + fmt_table(cols, rows)
                print(block); out.append(block)
            else:
                msg = f"ok ({cur.rowcount} rows): {' '.join(stmt.split())[:90]}"
                print(msg); out.append(msg)
    conn.close()
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    with open(os.path.join(HERE, "results", f"{name}.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    print(f"\nsaved results/{name}.txt")


if __name__ == "__main__":
    main()
