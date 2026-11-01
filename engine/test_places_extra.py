"""test places_extra: values present, dated, sane ranges. Run: python3 test_places_extra.py"""
import json, os, re, sys
from places_extra import fetch_places_extra

def test_places_extra(r):
    bad = []
    m = {n["label"]: n for n in r["marion"]}
    i = {n["label"]: n for n in r["indianapolis"]}
    for grp in (r["marion"], r["indianapolis"]):
        for n in grp:
            if set(n) != {"label", "value", "unit", "date", "source", "prov"} or n["prov"] != "fetched":
                bad.append("shape " + n.get("label", "?"))
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(n.get("date"))) or not n.get("source"):
                bad.append("undated/unsourced " + n.get("label", "?"))
    p = m.get("Population"); u = m.get("Unemployment rate"); ip = i.get("Population (city balance)")
    if not p or not 900_000 <= p["value"] <= 1_100_000: bad.append("marion pop range/missing")
    if not u or not 0 <= u["value"] <= 20: bad.append("marion rate range/missing")
    if not ip or not 700_000 <= ip["value"] <= 1_000_000: bad.append("indy pop range/missing")
    for c in r["checks"]:
        if not c["agree"]: bad.append("second source disagrees: " + c["what"])
    return bad

if __name__ == "__main__":
    r = fetch_places_extra()
    od = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out", "r12-places"); os.makedirs(od, exist_ok=True)
    json.dump(r, open(os.path.join(od, "places_extra.json"), "w"), indent=1)
    bad = test_places_extra(r)
    print("PASS places_extra" if not bad else "FAIL places_extra")
    for b in bad: print("   ", b)
    sys.exit(1 if bad else 0)
