#!/usr/bin/env python3
"""P1 clock tests (stdlib; `python3 test_clock.py` or pytest).
One wall-clock reader (clock.py); frozen clock; next_due at :51; a frozen-clock engine run is byte-stable."""
import datetime as dt, glob, os, re, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import clock

REPLAY = os.path.join(HERE, "cache", "v3_hourly_1")
FROZEN = "2026-10-31T09:05:00Z"      # just after the replay cache was written (09:00Z), so nothing in it is "from the future"


def test_only_clock_reads_wall_clock():
    """No engine module except clock.py calls now_utc() or datetime.now()/utcnow() (P1-owned modules and engine.py)."""
    bad = []
    for f in ("engine.py", "freshness.py", "timeline.py", "gates.py", "display.py"):
        src = open(os.path.join(HERE, f), encoding="utf-8").read()
        for m in re.finditer(r"now_utc\(\)|datetime\.now\(|datetime\.utcnow\(|date\.today\(", src):
            bad.append(f"{f}: {m.group(0)}")
    assert not bad, bad


def test_frozen_clock():
    clock.freeze("2026-11-03T13:00:00Z")
    try:
        assert clock.now() == dt.datetime(2026, 11, 3, 13, 0, tzinfo=clock.UTC)
        assert clock.now() == clock.now()
    finally:
        clock.freeze(None)
    assert abs((clock.now() - dt.datetime.now(dt.timezone.utc)).total_seconds()) < 5


def test_next_due_51():
    ref = clock.parse("2026-10-31T23:53:46Z")          # 19:53 ET
    assert clock.next_due("hourly", ref) == "2026-11-01T00:51:00Z"
    assert clock.next_due("morning", ref) == "2026-11-01T13:51:00Z"   # 08:51 EST: DST ends Nov 1 at 02:00 (UTC-5)
    assert clock.next_due("hourly", clock.parse("2026-10-31T23:50:59Z")) == "2026-10-31T23:51:00Z"


def _run(out):
    env = dict(os.environ, WATCHMAN_NOW=FROZEN, HTTPS_PROXY="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9")
    r = subprocess.run([sys.executable, os.path.join(HERE, "engine.py"), "--mode", "hourly", "--out", out, "--replay", REPLAY],
                       env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-500:]
    return {os.path.basename(p): open(p, "rb").read() for p in sorted(glob.glob(os.path.join(out, "batch_*.json")))}


def test_frozen_run_byte_stable():
    if not os.path.isdir(REPLAY):
        raise AssertionError(f"replay cache missing: {REPLAY}")
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        ra, rb = _run(a), _run(b)
        assert ra and ra == rb, "two frozen-clock runs over the same replay wrote different batches"
        assert FROZEN.encode() in ra["batch_001.json"]


MORNING_REPLAY = os.path.join(HERE, "cache", "v3_morning_2")


def _tree_hashes(d):
    import hashlib
    out = {}
    for root, _, files in os.walk(d):
        for f in files:
            p = os.path.join(root, f)
            out[os.path.relpath(p, d)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
    return out


def test_morning_byte_stable():
    """Two frozen-clock MORNING runs over the same replay cache and the same (empty) state write identical files:
    every batch, report.json, collections.txt, img/manifest.json and every image. Runs go side by side (~2.5 min)."""
    if not os.path.isdir(MORNING_REPLAY):
        raise AssertionError(f"replay cache missing: {MORNING_REPLAY}")
    env = dict(os.environ, WATCHMAN_NOW=FROZEN, HTTPS_PROXY="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9")
    with tempfile.TemporaryDirectory() as t:
        procs, outs = [], []
        for r in ("a", "b"):
            out, st = os.path.join(t, "out_" + r), os.path.join(t, "state_" + r)
            os.makedirs(st)
            outs.append(out)
            procs.append(subprocess.Popen([sys.executable, os.path.join(HERE, "engine.py"), "--mode", "morning", "--out", out,
                                           "--replay", MORNING_REPLAY, "--state", st], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        for p in procs:
            log = p.communicate(timeout=600)[0]
            assert p.returncode == 0, log[-500:]
        ha, hb = _tree_hashes(outs[0]), _tree_hashes(outs[1])
        assert any(k.startswith("batch_") for k in ha), sorted(ha)
        diff = sorted(k for k in set(ha) | set(hb) if ha.get(k) != hb.get(k))
        assert not diff, f"{len(diff)} file(s) differ between two frozen morning runs: {diff[:10]}"


if __name__ == "__main__":
    rc = 0
    for n, f in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
        try:
            f(); print("PASS", n)
        except AssertionError as e:
            rc = 1; print("FAIL", n, "-", str(e)[:300])
    sys.exit(rc)
