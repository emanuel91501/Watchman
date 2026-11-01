"""R17 vintages tests. python3 test_vintages.py [--out DIR]  (demo: bad copy must fail, real must pass)"""
import sys, json, os, glob
import vintages

def check(rows):
    errs = []
    if not rows or len(rows) != 3: errs.append("need 3 months")
    for r in rows or []:
        if not (r["date"][:2] == "20" and r["first"]["published"] and r["current"]["as_of"]): errs.append("missing dates " + r["date"])
        if r["first"]["published"] <= r["date"]: errs.append("published not after month " + r["date"])
        if not (0 < r["first"]["value"] < 1e7 and 0 < r["current"]["value"] < 1e7): errs.append("insane value")
    return errs

def demo():
    f = {"2026-07-01": (332.8, "2026-08-12"), "2026-08-01": (334.1, "2026-09-11"), "2026-09-01": (336.3, "2026-10-14"), "2026-06-01": (332.5, "2026-07-14")}
    c = {k: (v[0] + 0.1, "2026-10-31") for k, v in f.items()}
    good = vintages.build_vintages(f, c)
    assert not check(good), check(good)
    assert [r["date"] for r in good] == ["2026-09-01", "2026-08-01", "2026-07-01"]
    bad = json.loads(json.dumps(good)); bad[0]["first"]["published"] = "2026-09-01"; bad[1]["current"]["value"] = 9999
    assert check(bad), "bad copy must fail"
    print("demo ok")

if __name__ == "__main__":
    demo()
    if "--out" in sys.argv:
        d = sys.argv[sys.argv.index("--out") + 1]; bad = 0
        seen = {}
        for fp in sorted(glob.glob(os.path.join(d, "batch_*.json"))):
            for x in json.load(open(fp)):
                if x.get("collection") == "series" and x.get("doc_id") in ("cpiaucsl", "cpiaucns"):
                    seen[x["doc_id"]] = x["data"].get("vintages")
        for sid in ("cpiaucsl", "cpiaucns"):
            e = check(seen.get(sid)); print(sid, "PASS" if not e else e); bad += bool(e)
        sys.exit(1 if bad else 0)
