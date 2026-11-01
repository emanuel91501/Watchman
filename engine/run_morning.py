#!/usr/bin/env python3
"""Morning-run helper for RUN.md (d). Keeps every Bash call short and every db write small.

  python3 run_morning.py start            # launches engine.py --mode morning detached; returns at once
  python3 run_morning.py wait             # blocks up to 270 s; prints RUNNING (call again) or DONE/FAILED
  python3 run_morning.py writes           # after apply_versions: writes_NNN.json = batch entries with file_path
  python3 run_morning.py status --step S [--error TEXT]   # out/run_status.json for doc meta/run_status

Why (ENGINE-RUN-DIAG2, 2026-11-01): the engine takes 5-9 min. A foreground call near the 600 s Bash ceiling
or a run_in_background call followed by the end of the turn (which ends a scheduled session and kills the
engine) both left nothing written. `start` + repeated `wait` never needs more than about 5 min per call.
`writes` turns each multi-hundred-KB batch into a short array whose entries point at per-doc JSON files,
so ArtifactData batch `writes` is a few KB to pass, never the data itself.
"""
import argparse, datetime as dt, glob, json, os, re, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out", "morning")
LOG = os.path.join(HERE, "out", "morning.log")
EXIT = os.path.join(HERE, "out", "morning.exit")
PID = os.path.join(HERE, "out", "morning.pid")


def now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:   # a zombie counts as finished
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


def start(extra):
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    if os.path.exists(PID) and not os.path.exists(EXIT):
        pid = int(open(PID).read().split()[0])
        if alive(pid):
            print(f"ALREADY RUNNING pid {pid}; run: python3 run_morning.py wait"); return 0
    for p in (EXIT, LOG):
        if os.path.exists(p):
            os.remove(p)
    cmd = " ".join(["python3", "-u", "engine.py", "--mode", "morning", "--out", "out/morning"] + extra)
    sh = f"cd {HERE!r} && {cmd} > {LOG!r} 2>&1; echo $? > {EXIT!r}"
    p = subprocess.Popen(["sh", "-c", sh], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)
    with open(PID, "w") as f:
        f.write(f"{p.pid} {now()}\n")
    print(f"STARTED pid {p.pid} at {now()}. Next: python3 run_morning.py wait (repeat until DONE or FAILED; "
          f"do not end your turn while it says RUNNING)")
    return 0


def tail(n=6):
    try:
        return "".join(open(LOG, encoding="utf-8", errors="replace").readlines()[-n:]).strip()
    except OSError:
        return "(no log)"


def wait(max_s):
    if not os.path.exists(PID):
        print("FAILED step 1: not started; run: python3 run_morning.py start"); return 1
    pid, t0 = open(PID).read().split()[:2]
    pid = int(pid)
    end = time.time() + max_s
    while not os.path.exists(EXIT):
        if not alive(pid) and not os.path.exists(EXIT):
            time.sleep(1)
            if not os.path.exists(EXIT):
                print(f"FAILED step 1: engine process vanished without an exit code. Log tail:\n{tail()}"); return 1
        if time.time() >= end:
            print(f"RUNNING since {t0} (now {now()}). Call python3 run_morning.py wait again; do not end your turn.")
            return 0
        time.sleep(3)
    code = open(EXIT).read().strip()
    if code != "0":
        print(f"FAILED step 1: engine exit {code}. Log tail:\n{tail()}"); return 1
    r = subprocess.run([sys.executable, "test_engine.py", "--out", "out/morning"], cwd=HERE, capture_output=True, text=True)
    lines = [l for l in r.stdout.splitlines() if re.search(r"\b(PASS|FAIL)\b", l)]
    bad = [l for l in lines if "FAIL" in l and "idempotent_ids" not in l]
    rep = json.load(open(os.path.join(OUT, "report.json"), encoding="utf-8"))
    nb = len(glob.glob(os.path.join(OUT, "batch_*.json")))
    print(f"DONE engine exit 0 (started {t0}, finished {now()}); run {rep.get('run')}; {nb} batches; "
          f"tests {len(lines) - len(bad)} PASS, {len(bad)} FAIL (idempotent_ids ignored)")
    for l in bad:
        print("  " + l)
    if bad:
        print("FAILED step 2: tests failed (lines above)"); return 1
    return 0


def writes():
    done = 0
    for d in (OUT, os.path.join(OUT, "hourly")):
        files = sorted(f for f in glob.glob(os.path.join(d, "batch_*.json")) if re.fullmatch(r"batch_\d+\.json", os.path.basename(f)))
        for f in glob.glob(os.path.join(d, "writes_*.json")):
            os.remove(f)
        docdir = os.path.join(d, "docs")
        os.makedirs(docdir, exist_ok=True)
        for f in files:
            out = []
            for w in json.load(open(f, encoding="utf-8")):
                p = os.path.join(docdir, f"{w['collection'].replace('/', '__')}__{w['doc_id']}.json")
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(w["data"], fh, ensure_ascii=False, separators=(",", ":"))
                e = {"op": w["op"], "collection": w["collection"], "doc_id": w["doc_id"], "file_path": p}
                if "if_version" in w:
                    e["if_version"] = w["if_version"]
                out.append(e)
            wp = os.path.join(d, "writes_" + os.path.basename(f)[len("batch_"):])
            with open(wp, "w", encoding="utf-8") as fh:
                json.dump(out, fh, indent=0)
            print(f"{wp}: {len(out)} writes ({sum('if_version' in e for e in out)} with if_version)")
            done += 1
    if not done:
        print("FAILED: no batch_*.json in out/morning; run start/wait first"); return 1
    return 0


def status(step, error):
    rep = {}
    try:
        rep = json.load(open(os.path.join(OUT, "report.json"), encoding="utf-8"))
    except (OSError, ValueError):
        pass
    ok = step == "done" and not error
    doc = {"ok": ok, "step": step, "error": (error or "")[:500], "run": rep.get("run"), "at": now(),
           "label": "morning run complete" if ok else f"morning run failed at step {step}: {(error or '')[:200]}"}
    p = os.path.join(HERE, "out", "run_status.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False)
    print(f"{p}: {doc['label']}")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start"); s.add_argument("--static-once", action="store_true", help="first ever run only")
    w = sub.add_parser("wait"); w.add_argument("--max", type=int, default=270)
    sub.add_parser("writes")
    st = sub.add_parser("status"); st.add_argument("--step", required=True); st.add_argument("--error", default="")
    a = ap.parse_args(argv)
    if a.cmd == "start":
        return start(["--static-once"] if a.static_once else [])
    if a.cmd == "wait":
        return wait(a.max)
    if a.cmd == "writes":
        return writes()
    return status(a.step, a.error)


if __name__ == "__main__":
    sys.exit(main())
