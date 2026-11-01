"""records.py (P2): stage ladders on every record (CONTRACT-v4 section 7).

stage = {kind, ladder:[{key, label, at, source}], current, since, next:{what, when, source}|null}

Only facts the record itself states are dated. A rung the record proves was passed but does not date keeps at=null
(e.g. a bill presented to the President passed the House and Senate, but the feed gives only the presentment date).
Rungs after `current` are never dated unless the record names a scheduled date, which goes in `next`.

Contract kinds: rule (published -> comment window -> effective), bill (introduced -> committee -> House -> Senate -> President),
case (cert -> argued -> decided), order (signed -> FR published -> amends/revokes).
Record groups outside those four (nominations, declarations, sanctions, outbreaks) get the ladder their own source defines,
under EXTRA_KINDS; flagged in app/P2-NOTES.md for a lead edit of CONTRACT section 7.
"""
import re

LADDERS = {
    "rule": [("inspection", "Filed for public inspection"), ("published", "Published in the Federal Register"), ("comment", "Comment window"), ("effective", "Effective")],
    "bill": [("introduced", "Introduced"), ("committee", "Committee"), ("house", "Passed House"), ("senate", "Passed Senate"), ("president", "Presented to the President")],
    "case": [("cert", "Certiorari"), ("argued", "Argued"), ("decided", "Decided")],
    "order": [("signed", "Signed"), ("fr_published", "Published in the Federal Register"), ("amends", "Amends or revokes")],
    # EXTRA_KINDS (not in CONTRACT-v4 section 7 yet)
    "nomination": [("received", "Received in the Senate"), ("committee", "Committee"), ("calendar", "On the Executive Calendar"), ("confirmed", "Confirmed"), ("returned", "Returned to the President"), ("withdrawn", "Withdrawn")],
    "declaration": [("declared", "Declared"), ("amended", "Amended")],
    "sanction": [("action", "Action published"), ("amended", "Amended or removed")],
    "outbreak": [("reported", "Reported"), ("update", "Update published"), ("over", "Declared over")],
}
CONTRACT_KINDS = ("rule", "bill", "case", "order")
EXTRA_KINDS = ("nomination", "declaration", "sanction", "outbreak")
GROUP_KIND = {"rules_proposed": "rule", "public_inspection": "rule", "rules_final": "rule", "rules": "rule", "bills_presented": "bill", "bills": "bill",
              "rulings": "case", "state_rulings": "case", "cases": "case", "presidential": "order", "nominations": "nomination", "declarations": "declaration",
              "sanctions": "sanction", "outbreaks": "outbreak"}
ISO_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})(T[0-9:.]+(?:Z|[+-]\d{2}:\d{2})?)?\b")


def _src(it):
    s = (it.get("sources") or [{}])[0]
    return {"url": s.get("url"), "fetched": s.get("fetched")} if s.get("url") else None


def _iso(d):
    if not d:
        return None
    return d if "T" in d else d + "T00:00:00Z"


def _ladder(kind):
    return [{"key": k, "label": l, "at": None, "source": None} for k, l in LADDERS[kind]]


def _set(lad, key, at, src):
    for r in lad:
        if r["key"] == key:
            r["at"], r["source"] = at, src


def stage_for(item, group):
    """-> stage dict for one record item of record group `group` (records/<group>). Never raises; never invents a date."""
    kind = GROUP_KIND.get(group) or GROUP_KIND.get(item.get("kind", "") + "s") or None
    if kind is None:
        return None
    lad, src, t = _ladder(kind), _src(item), item.get("t")
    plain = (item.get("plain") or "") + " " + (item.get("title") or "")
    current, since, nxt = None, t, None

    if kind == "rule":
        if group == "public_inspection" or "public inspection" in plain.lower():
            _set(lad, "inspection", t, src); current = "inspection"
            m = re.search(r"scheduled for publication (\d{4}-\d{2}-\d{2})", plain)
            if m:
                nxt = {"what": "Publication in the Federal Register", "when": _iso(m.group(1)), "source": src}
        else:
            lad = [r for r in lad if r["key"] != "inspection"]
            _set(lad, "published", t, src); current = "published"
            m = re.search(r"comments? (?:are )?due(?: by| on)? (\d{4}-\d{2}-\d{2})", plain, re.I)
            cc = item.get("comments_close") or (m.group(1) if m else None)
            if cc:
                _set(lad, "comment", t, src); current = "comment"
                nxt = {"what": "Comment window closes", "when": _iso(cc), "source": src}
            eff = item.get("effective_on")
            if eff:
                nxt = nxt or {"what": "Takes effect", "when": _iso(eff), "source": src}
    elif kind == "bill":
        low = plain.lower()
        if "presented to the president" in low:
            current = "president"; _set(lad, "president", t, src)
        elif "passed senate" in low or "passed the senate" in low:
            current = "senate"; _set(lad, "senate", t, src)
        elif "passed house" in low or "passed the house" in low:
            current = "house"; _set(lad, "house", t, src)
        elif "committee" in low:
            current = "committee"; _set(lad, "committee", t, src)
        else:
            current = "introduced"; _set(lad, "introduced", t, src)
    elif kind == "case":
        low = plain.lower()
        if re.search(r"\b(decided|opinion|judgment)\b", low):
            current = "decided"; _set(lad, "decided", t, src)
        elif "argued" in low or "argument" in low:
            current = "argued"; _set(lad, "argued", t, src)
        else:
            current = "cert"; _set(lad, "cert", t, src)
    elif kind == "order":
        low = plain.lower()
        if "federal register" in low and re.search(r"\bpublished\b", low):
            current = "fr_published"; _set(lad, "fr_published", t, src)
        else:
            # the White House posting is the dated fact; it is the signing step as published by the signer
            current = "signed"; _set(lad, "signed", t, src)
            for r in lad:
                if r["key"] == "signed":
                    r["label"] = "Signed (date the White House posted it)"
        if re.search(r"\b(amend|revok|rescind|supersed)", low):
            nxt = None
    elif kind == "nomination":
        m = re.search(r"Latest action \d{4}-\d{2}-\d{2}:\s*(.+)", plain)
        low = (m.group(1) if m else plain).lower()  # TRUTH-FIX-M4: stage comes from the stored latest action only
        if low.startswith("returned to the president"):
            current = "returned"
        elif "withdrawn" in low or "message of withdrawal" in low:
            current = "withdrawn"
        elif low.startswith("confirmed"):
            current = "confirmed"
        elif "executive calendar" in low:
            current = "calendar"
        elif "committee" in low:
            current = "committee"
        else:
            current = "received"
        m = re.search(r"Latest action (\d{4}-\d{2}-\d{2})", plain)
        _set(lad, current, _iso(m.group(1)) if m else t, src)
        since = _iso(m.group(1)) if m else t
    elif kind == "declaration":
        current = "declared"; _set(lad, "declared", t, src)
    elif kind == "sanction":
        low = plain.lower()
        current = "amended" if re.search(r"\b(amend|remov|delist)", low) else "action"
        _set(lad, current, t, src)
    elif kind == "outbreak":
        low = plain.lower()
        current = "over" if re.search(r"declared (the )?(end|over)|outbreak (is )?over", low) else ("update" if re.search(r"since the publication of the previous|update", low) else "reported")
        _set(lad, current, t, src)

    return {"kind": kind, "ladder": lad, "current": current, "since": since, "next": nxt}


def attach_stages(rec_final):
    """rec_final {group: [items]} -> sets item['stage'] on every record. Returns {group: n_staged}."""
    out = {}
    for group, items in (rec_final or {}).items():
        n = 0
        for it in items:
            st = stage_for(it, group)
            if st:
                it["stage"] = st; n += 1
        out[group] = n
    return out


def check(stage):
    """-> problems with one stage (empty = valid)."""
    bad = []
    if not isinstance(stage, dict):
        return ["no stage"]
    if stage.get("kind") not in LADDERS:
        bad.append(f"kind {stage.get('kind')!r}")
    keys = [r.get("key") for r in stage.get("ladder") or []]
    if not stage.get("current") or stage["current"] not in keys:
        bad.append(f"current {stage.get('current')!r} not on ladder {keys}")
    else:
        i = keys.index(stage["current"])
        for r in stage["ladder"][i + 1:]:
            if r.get("at"):
                bad.append(f"rung {r['key']} after current is dated")
        cur = stage["ladder"][i]
        if cur.get("at") and not cur.get("source"):
            bad.append("current rung dated without a source")
    nx = stage.get("next")
    if nx is not None and not (nx.get("what") and "when" in nx):
        bad.append("next without what/when")
    return bad


# ---------------------------------------------------------------- who_runs (P10, LEAP-PLAN §4): official page only, else "not confirmed"
NOT_CONFIRMED = "not confirmed"


def who_runs(off, heads=None, fetched=None):
    """Head of government for a place. A name is printed ONLY when the official page was read in this run and the name was
    extracted from that page. Anything else (page unreadable, no name on it, only a crowd-edited Wikidata value) is
    {"name": "not confirmed", "kind": "not confirmed"} with the reason; a Wikidata value is never printed as the name.
    -> {name, kind: "official" | "not confirmed", source[, note]}"""
    heads = [h for h in dict.fromkeys(heads or []) if h]
    if off and off.get("readable") and off.get("name"):
        who = {"name": off["name"], "kind": "official", "source": f"Official page {off['url']}, read {fetched}"}
        sur = off["name"].split()[-1].lower()
        if heads and not any(h.split()[-1].lower() == sur for h in heads):
            who["note"] = "Wikidata (crowd-edited) lists a different person; the official page is used."
        return who
    if off and off.get("readable"):
        why = f"Official page {off['url']} was read {fetched} but names no head of government that could be extracted"
    elif off:
        why = f"Official page {off.get('url')} could not be read ({off.get('why') or 'no readable text'})"
    else:
        why = "No official page is listed for this place"
    note = " Wikidata (crowd-edited) lists a name; it is not shown because no official page confirmed it." if heads else ""
    return {"name": NOT_CONFIRMED, "kind": NOT_CONFIRMED, "source": why + "." + note}
