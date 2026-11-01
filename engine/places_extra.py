"""places_extra.py (Round 12): Marion County / Indianapolis population and Marion County unemployment.
Official sources only: Census Bureau Population Estimates files (www2.census.gov), FRED (BLS LAUS, Census PEP mirrors).
Hook: see app/ROUND12-NOTES.md.  Never prints or stores the FRED key.
Public API: fetch_places_extra() -> {"marion": [numbers...], "indianapolis": [numbers...], "checks": [...], "errors": [...]}
Each number: {label, value, unit, date, source, prov:"fetched"} (CONTRACT.md places/<id>.numbers)."""
import csv, io, json, os, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Watchman/1.0 (personal civic dashboard)"}
CENSUS_YEARS = (2025, 2024)  # newest vintage first
FRED = "https://api.stlouisfed.org/fred/series/observations"


def _key():
    k = os.environ.get("FRED_API_KEY")
    if k:
        return k
    try:
        for line in open(os.path.join(HERE, ".keys.env"), encoding="utf-8"):
            if line.startswith("FRED_API_KEY="):
                return line.split("=", 1)[1].strip().strip("'\"")
    except OSError:
        pass
    return None


def _get(url, timeout=90):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def _fred(series, limit=1):
    k = _key()
    if not k:
        raise RuntimeError("FRED_API_KEY missing")
    q = urllib.parse.urlencode({"series_id": series, "file_type": "json", "sort_order": "desc", "limit": limit, "api_key": k})
    obs = json.loads(_get(FRED + "?" + q, 30))["observations"]
    return [(o["date"], float(o["value"])) for o in obs if o["value"] not in (".", "")]


def _num(label, value, unit, date, source):
    return {"label": label, "value": value, "unit": unit, "date": date, "source": source, "prov": "fetched"}


def _census(kind):
    """Return {(year): {'marion': pop, 'indy': pop}} for the newest vintage that downloads."""
    for y in CENSUS_YEARS:
        span = "2020-%d" % y
        try:
            if kind == "county":
                url = "https://www2.census.gov/programs-surveys/popest/datasets/%s/counties/totals/co-est%d-alldata.csv" % (span, y)
            else:
                url = "https://www2.census.gov/programs-surveys/popest/datasets/%s/cities/totals/sub-est%d.csv" % (span, y)
            rows = csv.DictReader(io.StringIO(_get(url, 120).decode("latin-1")))
            for r in rows:
                if r["STATE"] == "18" and kind == "county" and r["COUNTY"] == "097":
                    return y, int(r["POPESTIMATE%d" % y]), url
                if r["STATE"] == "18" and kind == "city" and r["SUMLEV"] == "162" and r["PLACE"] == "36003":
                    return y, int(r["POPESTIMATE%d" % y]), url
        except Exception:
            continue
    return None


def _indiana_pop(y, url):
    """Indiana state total (COUNTY 000) from the SAME co-est file as Marion, so both counts share the date."""
    for r in csv.DictReader(io.StringIO(_get(url, 120).decode("latin-1"))):
        if r["STATE"] == "18" and r["COUNTY"] == "000":
            return int(r["POPESTIMATE%d" % y])
    return None


def fetch_places_extra():
    out = {"marion": [], "indianapolis": [], "indiana": [], "checks": [], "errors": []}
    # --- Marion County population: Census PEP file (primary), FRED INMARI0POP (cross-check)
    try:
        c = _census("county")
        if not c:
            raise RuntimeError("Census county file unreachable")
        y, pop, url = c
        out["marion"].append(_num("Population", pop, "people", "%d-07-01" % y,
                                  "U.S. Census Bureau, Population Estimates (vintage %d), county 18097: %s" % (y, url)))
        try:
            ip = _indiana_pop(y, url)
            if ip:
                out["indiana"].append(_num("Population", ip, "people", "%d-07-01" % y,
                                           "U.S. Census Bureau, Population Estimates (vintage %d), state total (Indiana), same file as Marion County: %s" % (y, url)))
        except Exception as e:
            out["errors"].append("indiana population: %s" % e)
        try:
            d, v = _fred("INMARI0POP")[0]
            agree = d[:4] == str(y) and round(v * 1000) == pop
            out["checks"].append({"what": "Marion population", "primary": pop, "second": round(v * 1000), "second_source": "FRED INMARI0POP (Census PEP mirror), obs %s" % d, "agree": agree})
        except Exception as e:
            out["errors"].append("fred pop check: %s" % type(e).__name__)
    except Exception as e:
        out["errors"].append("marion population: %s" % e)
    # --- Marion County unemployment: FRED INMARI0URN (BLS LAUS, monthly, not seasonally adjusted)
    try:
        d, v = _fred("INMARI0URN")[0]
        out["marion"].append(_num("Unemployment rate", v, "%", d,
                                  "U.S. Bureau of Labor Statistics LAUS via FRED series INMARI0URN (not seasonally adjusted)"))
        try:
            ad, av = _fred("LAUCN180970000000003A")[0]
            out["checks"].append({"what": "Marion unemployment", "primary": v, "second": av,
                                  "second_source": "FRED LAUCN180970000000003A (BLS LAUS annual average %s); same BLS program, annual not monthly, so consistency only (BLS site/API blocked from this host)" % ad[:4],
                                  "agree": abs(v - av) < 2.0})
        except Exception as e:
            out["errors"].append("fred rate check: %s" % type(e).__name__)
    except Exception as e:
        out["errors"].append("marion unemployment: %s" % e)
    # --- Indianapolis city population (balance): Census sub-county estimates
    try:
        c = _census("city")
        if not c:
            raise RuntimeError("Census place file unreachable")
        y, pop, url = c
        out["indianapolis"].append(_num("Population (city balance)", pop, "people", "%d-07-01" % y,
                                        "U.S. Census Bureau, Population Estimates (vintage %d), Indianapolis city (balance): %s" % (y, url)))
    except Exception as e:
        out["errors"].append("indianapolis population: %s" % e)
    out["indianapolis_unemployment_note"] = "No clean official city series on FRED; BLS LAUS city data not reachable from this host. Not fabricated."
    return out


if __name__ == "__main__":
    r = fetch_places_extra()
    od = os.path.join(HERE, "out", "r12-places"); os.makedirs(od, exist_ok=True)
    json.dump(r, open(os.path.join(od, "places_extra.json"), "w"), indent=1)
    print(json.dumps({k: v for k, v in r.items()}, indent=1))
