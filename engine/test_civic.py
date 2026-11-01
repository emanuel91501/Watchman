#!/usr/bin/env python3
"""Checks for civic.py output. Usage: python3 test_civic.py DIR [--demo]  (--demo runs each check on a deliberately bad copy; each must FAIL there)."""
import copy, json, os, re, sys

def load(d):
    """civic.py's own file, else the civic/indiana-2026-general write inside an engine run's batch_*.json."""
    import glob
    p = os.path.join(d, "civic_indiana-2026-general.json")
    if os.path.exists(p):
        return json.load(open(p))[0]["data"]
    for f in sorted(glob.glob(os.path.join(d, "batch_*.json"))):
        for w in json.load(open(f)):
            if w.get("collection") == "civic" and w.get("doc_id") == "indiana-2026-general":
                return w["data"]
    raise SystemExit(f"no civic/indiana-2026-general in {d}")

URL = re.compile(r"^https://[a-z0-9.-]+\.(gov|us)/", re.I)
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def has_src(s): return isinstance(s, dict) and URL.match(s.get("url", "")) and DATE.match(s.get("fetched", ""))

def checks(d):
    r = {}
    r["shape"] = [] if d.get("id") == "indiana-2026-general" and d.get("election", {}).get("date") == "2026-11-03" and DATE.match(d.get("fetched", "")) else ["id/election/fetched wrong"]
    r["official_hosts_only"] = [u for u in re.findall(r"https?://[^\"\s]+", json.dumps(d)) if not re.match(r"https?://(vote\.indy\.gov|www\.in\.gov|indianavoters\.in\.gov|www\.google\.com/maps)/", u)]
    sites = d["vote_centers"] + d["early_sites"]
    r["sites_have_place"] = [s["name"] for s in sites if not (s.get("address") and isinstance(s.get("lat"), float) and s["gmaps"].startswith("https://www.google.com/maps/") and URL.match(s["url"]))]
    r["sites_source"] = [] if (not sites) or has_src(d.get("sites_source")) else ["sites without source"]
    r["early_hours_are_fetched"] = [s["name"] for s in d["early_sites"] if s.get("hours") and not has_src(s.get("hours_source"))]
    r["polls_sourced"] = [] if (d.get("polls") is None or (has_src(d["polls"].get("source")) and d["polls"]["open"] and d["polls"]["close"])) else ["polls"]
    p = d.get("photo_id")
    r["photo_id_sourced"] = [] if (p is None or (has_src(p.get("source")) and p.get("rule") and p.get("must"))) else ["photo_id"]
    bad = []
    for q in d["ballot"]["public_questions"]:
        if not has_src(q.get("source")): bad.append(q["body"] + ": no source")
        if "annual_increase" in q and not has_src(q.get("cost_source")): bad.append(q["body"] + ": cost without source")
        if q.get("question") and not has_src(q.get("question_source")): bad.append(q["body"] + ": wording without source")
    r["ballot_sourced"] = bad
    r["nothing_silent"] = [] if (sites and d["polls"] and d["photo_id"] and d["ballot"]["public_questions"]) or d["unreached"] else ["a section is empty and unreached lists no reason"]
    r["unreached_have_reason"] = [u for u in d["unreached"] if not (u.get("item") and u.get("url") and u.get("reason"))]
    r["doc_size_ok"] = [] if len(json.dumps(d)) <= 220 * 1024 else ["over 220 KB"]
    return r

def main():
    d = load(sys.argv[1]); demo = "--demo" in sys.argv
    good = checks(d); fail = 0
    for k, v in good.items():
        print(("PASS" if not v else "FAIL"), k, ("" if not v else "  e.g. " + str(v[0])[:90])); fail += bool(v)
    if demo:
        b = copy.deepcopy(d)
        b["vote_centers"][0].pop("address"); b["polls"]["source"] = {}; b["photo_id"]["source"]["fetched"] = "yesterday"
        b["ballot"]["public_questions"][1]["cost_source"] = None; b["early_sites"][0]["hours_source"] = None; b["early_sites"][0]["hours"] = "11-6"
        b["early_sites"][1]["url"] = "http://evil.example.com/"; b["vote_centers"][1]["gmaps"] = "https://example.com/x"
        bad = checks(b)
        for k in ("sites_have_place", "polls_sourced", "photo_id_sourced", "ballot_sourced", "early_hours_are_fetched", "official_hosts_only"):
            print(("FAIL (ok)" if bad[k] else "PASSED (BUG)"), "demo", k); fail += not bad[k]
        e = copy.deepcopy(d); e["vote_centers"] = []; e["early_sites"] = []; e["polls"] = None; e["unreached"] = []
        print(("FAIL (ok)" if checks(e)["nothing_silent"] else "PASSED (BUG)"), "demo nothing_silent")
    sys.exit(1 if fail else 0)

if __name__ == "__main__": main()
