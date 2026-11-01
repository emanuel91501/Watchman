"""coverage.py (P2): a place doc for every US state and every country today's items name (MASTER-PLAN C class 8).

Named = an item's structured place.admin, or a state/country name in its title. Each derived doc uses the places/<id> skeleton
(id, name, level, who_runs, numbers, alerts_n, news_n, why, limits, gmaps, hero) plus `derived: true`, `fips` or `iso3`.
Numbers are fetched by FIPS (US Census ACS 5-year population) or ISO3 (World Bank population, GDP); a failed fetch leaves
`numbers: []` and says so in `limits`. Nothing is typed in. The hand-built PLACES list (17) stays the hero-series seed and is never overwritten.
"""
import json, re, urllib.request
from urllib.parse import quote

UA = {"User-Agent": "Watchman/1.0 (personal civic dashboard)"}

STATE_FIPS = {"Alabama": "01", "Alaska": "02", "Arizona": "04", "Arkansas": "05", "California": "06", "Colorado": "08", "Connecticut": "09", "Delaware": "10",
              "District of Columbia": "11", "Florida": "12", "Georgia": "13", "Hawaii": "15", "Idaho": "16", "Illinois": "17", "Indiana": "18", "Iowa": "19",
              "Kansas": "20", "Kentucky": "21", "Louisiana": "22", "Maine": "23", "Maryland": "24", "Massachusetts": "25", "Michigan": "26", "Minnesota": "27",
              "Mississippi": "28", "Missouri": "29", "Montana": "30", "Nebraska": "31", "Nevada": "32", "New Hampshire": "33", "New Jersey": "34", "New Mexico": "35",
              "New York": "36", "North Carolina": "37", "North Dakota": "38", "Ohio": "39", "Oklahoma": "40", "Oregon": "41", "Pennsylvania": "42", "Rhode Island": "44",
              "South Carolina": "45", "South Dakota": "46", "Tennessee": "47", "Texas": "48", "Utah": "49", "Vermont": "50", "Virginia": "51", "Washington": "53",
              "West Virginia": "54", "Wisconsin": "55", "Wyoming": "56", "Puerto Rico": "72"}
# ISO 3166-1 alpha-3 for every name engine.COUNTRIES can produce (aliases map to the same code).
COUNTRY_ISO3 = {"Afghanistan": "AFG", "Algeria": "DZA", "Angola": "AGO", "Argentina": "ARG", "Armenia": "ARM", "Australia": "AUS", "Austria": "AUT", "Azerbaijan": "AZE",
                "Bahamas": "BHS", "Bangladesh": "BGD", "Belarus": "BLR", "Belgium": "BEL", "Bolivia": "BOL", "Brazil": "BRA", "Britain": "GBR", "Bulgaria": "BGR",
                "Burkina Faso": "BFA", "Burundi": "BDI", "Cambodia": "KHM", "Cameroon": "CMR", "Canada": "CAN", "Chad": "TCD", "Chile": "CHL", "China": "CHN",
                "Colombia": "COL", "Costa Rica": "CRI", "Cuba": "CUB", "Cyprus": "CYP", "Czech Republic": "CZE", "Democratic Republic of the Congo": "COD", "DR Congo": "COD",
                "Denmark": "DNK", "Djibouti": "DJI", "Dominican Republic": "DOM", "Ecuador": "ECU", "Egypt": "EGY", "El Salvador": "SLV", "Eritrea": "ERI", "Ethiopia": "ETH",
                "Fiji": "FJI", "Finland": "FIN", "France": "FRA", "Gaza": "PSE", "Georgia (country)": "GEO", "Germany": "DEU", "Ghana": "GHA", "Greece": "GRC",
                "Guatemala": "GTM", "Haiti": "HTI", "Honduras": "HND", "Hungary": "HUN", "Iceland": "ISL", "India": "IND", "Indonesia": "IDN", "Iran": "IRN", "Iraq": "IRQ",
                "Ireland": "IRL", "Israel": "ISR", "Italy": "ITA", "Jamaica": "JAM", "Japan": "JPN", "Jordan": "JOR", "Kazakhstan": "KAZ", "Kenya": "KEN", "Kosovo": "XKX",
                "Kuwait": "KWT", "Kyrgyzstan": "KGZ", "Laos": "LAO", "Lebanon": "LBN", "Libya": "LBY", "Madagascar": "MDG", "Malaysia": "MYS", "Mali": "MLI", "Mexico": "MEX",
                "Moldova": "MDA", "Mongolia": "MNG", "Morocco": "MAR", "Mozambique": "MOZ", "Myanmar": "MMR", "Nepal": "NPL", "Netherlands": "NLD", "New Zealand": "NZL",
                "Nicaragua": "NIC", "Niger": "NER", "Nigeria": "NGA", "North Korea": "PRK", "Norway": "NOR", "Pakistan": "PAK", "Palestine": "PSE", "Panama": "PAN",
                "Papua New Guinea": "PNG", "Peru": "PER", "Philippines": "PHL", "Poland": "POL", "Portugal": "PRT", "Qatar": "QAT", "Romania": "ROU", "Russia": "RUS",
                "Rwanda": "RWA", "Saudi Arabia": "SAU", "Senegal": "SEN", "Serbia": "SRB", "Singapore": "SGP", "Slovakia": "SVK", "Solomon Islands": "SLB", "Somalia": "SOM",
                "South Africa": "ZAF", "South Korea": "KOR", "South Sudan": "SSD", "Spain": "ESP", "Sri Lanka": "LKA", "Sudan": "SDN", "Sweden": "SWE", "Switzerland": "CHE",
                "Syria": "SYR", "Taiwan": "TWN", "Tajikistan": "TJK", "Tanzania": "TZA", "Thailand": "THA", "Tonga": "TON", "Tunisia": "TUN", "Turkey": "TUR",
                "Turkmenistan": "TKM", "Uganda": "UGA", "Ukraine": "UKR", "United Arab Emirates": "ARE", "United Kingdom": "GBR", "Uruguay": "URY", "Uzbekistan": "UZB",
                "Vanuatu": "VUT", "Venezuela": "VEN", "Vietnam": "VNM", "West Bank": "PSE", "Yemen": "YEM", "Zambia": "ZMB", "Zimbabwe": "ZWE"}
ISO3_NAME = {"GBR": "United Kingdom", "COD": "Democratic Republic of the Congo", "PSE": "Palestine", "GEO": "Georgia"}
# Titles: Washington and Georgia are ambiguous in prose, so only place.admin names them (as engine.STATE_NAME_RE does).
_STATE_RE = re.compile(r"\b(" + "|".join(sorted([re.escape(n) for n in STATE_FIPS if n not in ("Washington", "Georgia", "District of Columbia")], key=len, reverse=True)) + r")\b")
_COUNTRY_RE = re.compile(r"\b(" + "|".join(sorted([re.escape(n) for n in COUNTRY_ISO3 if n != "Georgia (country)"], key=len, reverse=True)) + r")\b")
NOT_PLACES = {"united states", "world", "not stated", ""}


def slug(s):
    return re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")


def place_id(level, code):
    """Stable id: states by name slug (texas), countries by ISO3 name slug; the hand-built ids (indiana, saudi_arabia) are the same form."""
    if level == "state":
        return slug(code)
    return slug(ISO3_NAME.get(code) or next(n for n, c in COUNTRY_ISO3.items() if c == code))


def named(items):
    """-> {place_id: {level, name, fips|iso3, item_ids[]}} for every state/country the items name."""
    out = {}

    def add(name, it):
        if name in STATE_FIPS:
            pid, rec = slug(name), {"level": "state", "name": name, "fips": STATE_FIPS[name]}
        elif name in COUNTRY_ISO3:
            iso = COUNTRY_ISO3[name]
            pid, rec = place_id("country", iso), {"level": "country", "name": ISO3_NAME.get(iso, name), "iso3": iso}
        else:
            return
        out.setdefault(pid, dict(rec, item_ids=[]))
        if it.get("id") and it["id"] not in out[pid]["item_ids"]:
            out[pid]["item_ids"].append(it["id"])

    for it in items or []:
        p = it.get("place") or {}
        adm = p.get("admin")
        if adm and adm.lower() not in NOT_PLACES:
            add(adm, it)
        title = it.get("title") or ""
        for m in _STATE_RE.finditer(title):
            add(m.group(1), it)
        for m in _COUNTRY_RE.finditer(title):
            add(m.group(1), it)
    return out


# ---------------------------------------------------------------- fetch (injected; tests pass their own or none)
def http_json(url, timeout=30):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return json.loads(r.read())


def census_state(fips, get, fetched_at):
    url = f"https://api.census.gov/data/2023/acs/acs5?get=NAME,B01003_001E&for=state:{fips}"
    rows = get(url)
    name, pop = rows[1][0], int(rows[1][1])
    return [{"label": "Population (ACS 5-year 2019-2023)", "value": pop, "display": f"{pop:,}", "unit": "people", "date": "2023",
             "source": f"U.S. Census Bureau, American Community Survey 5-year, table B01003, {name} (FIPS {fips}) {url}", "url": url, "prov": "fetched", "fetched_at": fetched_at}]


def worldbank(iso3, get, fetched_at):
    out = []
    for ind, label, unit in (("SP.POP.TOTL", "Population, total", "people"), ("NY.GDP.MKTP.CD", "GDP (current US$)", "US dollars")):
        url = f"https://api.worldbank.org/v2/country/{iso3}/indicator/{ind}?format=json&mrnev=1"
        js = get(url)
        rows = js[1] if isinstance(js, list) and len(js) > 1 and js[1] else []
        for r in rows:
            if r.get("value") is not None:
                v = r["value"]
                out.append({"label": label, "value": v, "display": f"{v:,.0f}", "unit": unit, "date": r.get("date"),
                            "source": f"World Bank Open Data https://data.worldbank.org/indicator/{ind}?locations={iso3}", "url": url, "prov": "fetched", "fetched_at": fetched_at})
                break
    return out


def derived_places(items, existing_ids, get=None, fetched_at=None, clusters=None):
    """-> [("places", id, doc)] for every named state/country without a places/<id> doc yet.
    `get(url) -> json` does the fetch (None = do not fetch: numbers [] and limits say so)."""
    from images import contract as C
    docs = []
    names = named(list(items or []) + list(clusters or []))
    for pid, rec in sorted(names.items()):
        if pid in existing_ids:
            continue
        nums, limits = [], []
        code = rec.get("fips") or rec.get("iso3")
        if get is None:
            limits.append("numbers not fetched in this run")
        else:
            try:
                nums = census_state(rec["fips"], get, fetched_at) if rec["level"] == "state" else worldbank(rec["iso3"], get, fetched_at)
                if not nums:
                    limits.append("source returned no value")
            except Exception as e:
                limits.append(f"{'Census' if rec['level'] == 'state' else 'World Bank'} fetch failed ({type(e).__name__})")
        ids = set(rec["item_ids"])
        alerts_n = sum(1 for i in items or [] if i.get("id") in ids)
        news_n = sum(1 for i in clusters or [] if i.get("id") in ids)
        query = rec["name"] + (", United States" if rec["level"] == "state" else "")
        docs.append(("places", pid, {
            "id": pid, "name": rec["name"], "level": rec["level"], "derived": True,
            ("fips" if rec["level"] == "state" else "iso3"): code,
            "who_runs": {"kind": "not confirmed", "name": "not confirmed", "source": "Derived place: no official page was read for its head of government."},
            "numbers": nums, "alerts_n": alerts_n, "news_n": news_n, "named_by": rec["item_ids"][:50],
            "why": f"Named by {len(rec['item_ids'])} item(s) in today's run.",
            "limits": "; ".join(limits) or "population/GDP only; head of government not fetched",
            "gmaps": "https://www.google.com/maps/search/?api=1&query=" + quote(query),
            "hero": C.plate(pid, "derived place: no licensed photograph fetched; outline plate"),
        }))
    return docs


# ---------------------------------------------------------------- scope (P10, LEAP-PLAN §1.9): state courts are never filed under Washington
FEDERAL_COURT = re.compile(r"\b(U\.?S\.? Supreme Court|Supreme Court of the United States|U\.?S\.? Court of Appeals|United States Court of Appeals|U\.?S\.? District Court|United States District Court|Court of Federal Claims|Tax Court|SCOTUS)\b", re.I)
STATE_COURT = re.compile(r"\b(" + "|".join(sorted([re.escape(n) for n in STATE_FIPS], key=len, reverse=True)) + r")\s+(Supreme Court|Court of Appeals|Appellate Court|Tax Court|Court of Criminal Appeals|Superior Court|Circuit Court|District Court)\b")
COURT_HOST_STATE = {"courtlistener.com/opinion": None}


def scope(item, group=None):
    """-> {"level": "federal"|"state", "under": "Washington"|<state name>, "why"} for a record.
    A court named "<State> Supreme Court / Court of Appeals ..." is a state court and files under that state, never Washington.
    Everything else in records/* is federal (Washington)."""
    text = " ".join(str(item.get(k) or "") for k in ("title", "plain", "court"))
    p = item.get("place") or {}
    m = STATE_COURT.search(text) or STATE_COURT.search(str(p.get("name") or ""))
    if m and not FEDERAL_COURT.search(m.group(0)):
        return {"level": "state", "under": m.group(1), "why": f"{m.group(0)} is a state court"}
    if group in ("rulings", "cases") and item.get("layer") in ("indiana", "local") and not FEDERAL_COURT.search(text):
        return {"level": "state", "under": p.get("admin") or "Indiana", "why": "court record on the Indiana layer"}
    return {"level": "federal", "under": "Washington", "why": "federal record"}


def refile_scope(rec_final):
    """Every record gets `scope`; state-court rulings move from records/rulings to records/state_rulings (never under Washington).
    Mutates rec_final in place; returns {"moved": n}."""
    moved = []
    for group, items in list((rec_final or {}).items()):
        keep = []
        for it in items:
            it["scope"] = scope(it, group)
            if group == "rulings" and it["scope"]["level"] == "state":
                moved.append(it)
            else:
                keep.append(it)
        rec_final[group] = keep
    if moved or "rulings" in (rec_final or {}):
        rec_final["state_rulings"] = sorted(rec_final.get("state_rulings", []) + moved, key=lambda i: i.get("t") or "", reverse=True)
    return {"moved": len(moved)}
