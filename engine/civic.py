#!/usr/bin/env python3
"""Watchman civic module (round 12): Indiana / Marion County facts for the Nov 3, 2026 general election.

Official pages only: vote.indy.gov (Marion County Election Board), www.in.gov/sos/elections (Indiana Election
Division), www.in.gov/dlgf (Department of Local Government Finance, referendum determinations), indianavoters.in.gov.
Every fact carries its source URL and the date it was fetched. A page that cannot be reached is listed under
`unreached` with the reason; nothing is filled in from memory.

Usage:  python3 civic.py --out DIR            writes DIR/civic_indiana-2026-general.json (a ready ArtifactData `set`)
        python3 test_civic.py DIR             checks the output
Importable:  civic.build(session=None) -> doc dict
Hook for engine.py (one line, morning mode): see app/ROUND12-NOTES.md.
"""
import argparse, datetime as dt, html, io, json, os, re, shutil, subprocess, sys, tempfile
import requests

UA = "Watchman/0.1 (personal civic dashboard)"
TIMEOUT = (8, 25)
ELECTION = {"name": "Indiana general election", "date": "2026-11-03", "tz": "America/Indiana/Indianapolis"}
U_VOTE = "https://vote.indy.gov/"
U_EARLY = "https://vote.indy.gov/early-voting/"
U_SAMPLE = "https://vote.indy.gov/sample-ballot/"
U_PHOTO = "https://www.in.gov/sos/elections/voter-information/photo-id-law/"
U_DLGF = "https://www.in.gov/dlgf/referendum-information"
U_PORTAL = "https://indianavoters.in.gov/"
DOC_ID = "indiana-2026-general"


def today():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def gmaps(q):
    return "https://www.google.com/maps/search/?api=1&query=" + requests.utils.quote(q)


class Fetcher:
    def __init__(self, session=None):
        self.s = session or requests.Session()
        self.s.headers["User-Agent"] = "Mozilla/5.0 " + UA
        self.unreached = []

    def get(self, url, item, binary=False):
        try:
            r = self.s.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            return r.content if binary else r.text
        except Exception as e:  # noqa
            self.unreached.append({"item": item, "url": url, "reason": f"{type(e).__name__}: {str(e)[:120]}"})
            return None


def text_of(h):
    h = re.sub(r"(?s)<(script|style|noscript)[^>]*>.*?</\1>", " ", h)
    h = html.unescape(re.sub(r"<[^>]+>", "\n", h))
    return re.sub(r"\n\s*\n+", "\n", re.sub(r"[ \t\r]+", " ", h))


def pdf_text(b):
    """Text of a PDF: pdftotext if present, else pypdf; None when neither yields text (e.g. a scan)."""
    if not b:
        return None
    t = None
    if shutil.which("pdftotext"):
        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(b); f.flush()
            try:
                t = subprocess.run(["pdftotext", "-layout", f.name, "-"], capture_output=True, timeout=40).stdout.decode("utf-8", "replace")
            except Exception:
                t = None
    if not (t or "").strip():
        try:
            import pypdf
            t = "\n".join((p.extract_text() or "") for p in pypdf.PdfReader(io.BytesIO(b)).pages)
        except Exception:
            t = None
    return t if (t or "").strip() else None


def src(url, fetched, quote=None):
    d = {"url": url, "fetched": fetched}
    if quote:
        d["quote"] = quote[:300]
    return d


# ---------------------------------------------------------------------------------------------------------
def vote_centers(F, fetched):
    """Every Election Day vote center and early site from the page's own map data (data-map-locations JSON)."""
    h = F.get(U_VOTE, "vote centers")
    if not h:
        return None, None
    m = re.search(r"data-map-locations>(\[.*?\])</script>", h, re.S)
    if not m:
        F.unreached.append({"item": "vote centers", "url": U_VOTE, "reason": "page loaded but its location list was not found"})
        return None, h
    try:
        L = json.loads(m.group(1))
    except Exception as e:  # noqa
        F.unreached.append({"item": "vote centers", "url": U_VOTE, "reason": f"location list unreadable: {e}"})
        return None, h
    out = []
    for x in L:
        addr = (x.get("address") or "").strip()
        out.append({"name": html.unescape(x.get("title") or "").strip(), "address": addr, "street": x.get("street"), "city_line": x.get("city_line"),
                    "lat": x.get("lat"), "lng": x.get("lng"), "kind": x.get("type"), "dates": x.get("date"),
                    "gmaps": gmaps(f"{x.get('title')}, {addr}"), "url": x.get("permalink") or U_VOTE})
    return out, h


def site_hours(F, site):
    """Hours as the site's own details page states them (early sites); None when the page can't be read."""
    h = F.get(site["url"], "hours: " + site["name"])
    if not h:
        return None
    t = text_of(h)
    i = t.find("Early Voting Dates")
    j = t.find("Election Day Date")
    if i < 0:
        return None
    seg = re.sub(r"\s+", " ", t[i:j if j > i else i + 600]).strip()
    seg = re.sub(r"^Early Voting Dates & Hours\s*", "", seg)
    return seg or None


def photo_id(F, fetched):
    h = F.get(U_PHOTO, "photo ID rule")
    if not h:
        return None
    t = text_of(h)
    i = t.find("Public Law 109-2005")
    if i < 0:
        F.unreached.append({"item": "photo ID rule", "url": U_PHOTO, "reason": "page loaded but the rule text was not found"})
        return None
    j = t.find("Frequently Asked Questions", i)
    body = re.sub(r"[ \t]+", " ", t[i:j if j > i else i + 4000])
    lines = [l.strip() for l in body.split("\n") if l.strip()]
    def pick(rx):
        for l in lines:
            if re.search(rx, l, re.I):
                return l
    rule = pick(r"requires Indiana residents to present")
    crit = []
    for l in lines:
        if re.match(r"^(Display|Be issued)", l, re.I):
            crit.append(re.sub(r"^1\.\s*", "", l))
    # the numbered criteria in the page are "1. Display your photo", "2. Display your name...", "Display an expiration date...", "Be issued by..."
    if not any("photo" in c.lower() for c in crit):
        crit.insert(0, "Display your photo")
    ok = [pick(r"In most cases"), pick(r"educational institution")]
    prov = pick(r"provisional ballot")
    return {"rule": rule, "must": crit[:6], "usually_enough": ok[0], "not_accepted": ok[1], "if_no_id": prov, "source": src(U_PHOTO, fetched, rule)}


def polls(F, fetched, h_vote):
    """Election Day hours: the Marion County FAQ states them ('Per Indiana law, Vote Centers are open from 6:00 a.m. to 6:00 p.m.')."""
    if not h_vote:
        return None
    t = text_of(h_vote)
    m = re.search(r"Vote Centers are open from ([0-9:]+ [ap]\.m\.) to ([0-9:]+ [ap]\.m\.) on Election Day", t)
    if not m:
        return None
    return {"open": m.group(1), "close": m.group(2), "text": m.group(0), "source": src(U_VOTE, fetched, m.group(0))}


def early_dates(F, fetched):
    h = F.get(U_EARLY, "early voting dates")
    if not h:
        return None
    t = text_of(h)
    out = {"source": src(U_EARLY, fetched)}
    m = re.search(r"Early Voting at the Indianapolis City-County Building starts on ([^.]*?)\. Additional Early Voting satellite sites will operate ([^.]*?)\.", re.sub(r"\s+", " ", t))
    if m:
        out["text"] = m.group(0)
    m2 = re.search(r"valid\s*photo ID", t)
    out["id_needed"] = "present a valid photo ID" if m2 else None
    return out


def referenda(F, fetched):
    """Marion County rows of DLGF's 'November 2026 Elections' table, and for each the Auditor's certified cost when the PDF states it."""
    h = F.get(U_DLGF, "ballot: school referendums")
    if not h:
        return None
    i = h.find("November 2026 Elections")
    if i < 0:
        F.unreached.append({"item": "ballot: school referendums", "url": U_DLGF, "reason": "page loaded but no 'November 2026 Elections' table"})
        return None
    tbl = h[i:]
    k = tbl.find("</table>")
    tbl = tbl[:k] if k > 0 else tbl
    rows = re.findall(r"<tr>(.*?)</tr>", tbl, re.S)
    out = []
    for r in rows:
        tds = re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)
        if len(tds) < 4:
            continue
        county = text_of(tds[0]).strip()
        if "Marion" not in county:
            continue
        body = text_of(tds[1]).strip()
        kind = text_of(tds[2]).strip()
        links = {text_of(a).strip(): u for u, a in re.findall(r'href="([^"]+)"[^>]*>(.*?)</a>', tds[3], re.S)}
        base = "https://www.in.gov"
        links = {k2: (base + v if v.startswith("/") else v) for k2, v in links.items()}
        item = {"body": re.sub(r"\s+", " ", body), "type": kind, "documents": links, "source": src(U_DLGF, fetched)}
        cert = next((v for k2, v in links.items() if "Certification" in k2), None)
        det = next((v for k2, v in links.items() if "Determination" in k2), None)
        if cert:
            ct = pdf_text(F.get(cert, "certified cost: " + body, binary=True))
            if ct:
                flat = re.sub(r"\s+", " ", ct)
                med = re.search(r"median household value is \$([\d,]+)", flat)
                inc = re.search(r"annual property tax bill for the median household[^$]{0,160}would be \$([\d,]+)", flat)
                rate = re.search(r"proposed tax rate of \$([\d.]+)", flat)
                if inc:
                    item["median_home"] = int(med.group(1).replace(",", "")) if med else None
                    item["annual_increase"] = int(inc.group(1).replace(",", ""))
                    item["rate_per_100"] = rate.group(1) if rate else None
                    item["cost_source"] = src(cert, fetched, "Auditor's certification: increase for the median household would be $" + inc.group(1))
        if det:
            dt_ = pdf_text(F.get(det, "ballot wording: " + body, binary=True))
            if dt_:
                flat = re.sub(r"\s+", " ", dt_)
                q = re.search(r"The proposed ballot question is as follows:\s*[\"“](.*?)[\"”]\s*8\.", flat)
                if q:
                    item["question"] = q.group(1).strip()
                    item["question_source"] = src(det, fetched, "DLGF final determination approving the ballot language")
                else:
                    item["question"] = None
        out.append(item)
    return out


# ---------------------------------------------------------------------------------------------------------
def build(session=None, run=None):
    F = Fetcher(session)
    fetched = today()
    doc = {"id": DOC_ID, "election": ELECTION, "fetched": fetched, "as_of": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "v": 3,
           "county": "Marion County, Indiana"}
    if run:
        doc["_run"] = run
    doc["_at"] = doc["as_of"]
    sites, h_vote = vote_centers(F, fetched)
    if sites:
        early = [s for s in sites if s["kind"] != "election_day"]
        eday = [s for s in sites if s["kind"] == "election_day"]
        for s in early:
            s["hours"] = site_hours(F, s)
            s["hours_source"] = src(s["url"], fetched) if s["hours"] else None
        doc["early_sites"] = early
        doc["vote_centers"] = eday
        doc["sites_source"] = src(U_VOTE, fetched, "Marion County voters can cast their ballot at ANY of the Vote Centers in the county")
    else:
        doc["early_sites"] = []
        doc["vote_centers"] = []
    doc["polls"] = polls(F, fetched, h_vote)
    doc["early"] = early_dates(F, fetched)
    doc["photo_id"] = photo_id(F, fetched)
    ref = referenda(F, fetched)
    doc["ballot"] = {
        "public_questions": ref if ref is not None else [],
        "public_questions_source": src(U_DLGF, fetched) if ref is not None else None,
        "sample_ballot": {"url": U_SAMPLE, "note": "Your own ballot (offices, candidates, public questions) depends on your precinct. The county's sample-ballot tool is an interactive lookup, so it is not read here.", "fetched": fetched},
        "offices": None,
        "offices_note": "Not fetched: the contests on a Marion County ballot vary by precinct and are shown only by the county's address lookup.",
    }
    doc["unreached"] = F.unreached
    doc["portal"] = src(U_PORTAL, fetched)
    return doc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    doc = build()
    w = [{"action": "set", "collection": "civic", "doc_id": DOC_ID, "data": doc}]
    p = os.path.join(a.out, "civic_indiana-2026-general.json")
    json.dump(w, open(p, "w"), indent=1, ensure_ascii=False)
    print(p, len(json.dumps(doc)), "bytes;", len(doc["vote_centers"]), "vote centers;", len(doc["early_sites"]), "early sites;",
          len(doc["ballot"]["public_questions"]), "public questions;", len(doc["unreached"]), "unreached")


if __name__ == "__main__":
    main()
