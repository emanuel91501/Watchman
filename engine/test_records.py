"""P2 record-stage tests over the C1 fixture (real r23 records). Run: python3 test_records.py"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import records
FX = os.path.join(HERE, "..", "app", "dev", "fixtures", "C1.json")


def _rec():
    d = json.load(open(FX))
    return {k.split("/", 1)[1]: list(v.get("items") or []) for k, v in d.items() if k.startswith("records/")}


def test_every_record_has_stage_current():
    rf = _rec(); records.attach_stages(rf); n = 0
    for g, items in rf.items():
        for it in items:
            n += 1
            assert it.get("stage") and it["stage"].get("current"), (g, it.get("id"))
            assert records.check(it["stage"]) == [], (g, it["id"], records.check(it["stage"]))
    assert n >= 250, n


def test_contract_kinds_map():
    for g in ("rules_proposed", "public_inspection", "bills_presented", "rulings", "presidential"):
        assert records.GROUP_KIND[g] in records.CONTRACT_KINDS


def test_public_inspection_next_is_stated_date():
    rf = _rec(); records.attach_stages(rf)
    pi = rf["public_inspection"]; withnext = [i for i in pi if i["stage"]["next"]]
    assert withnext, "no public inspection item states its publication date"
    for i in withnext:
        assert i["stage"]["next"]["when"][:10] in i["plain"], i["id"]


def test_bill_presented_is_president_rung():
    rf = _rec(); records.attach_stages(rf)
    for i in rf["bills_presented"]:
        assert i["stage"]["current"] == "president" and i["stage"]["since"] == i["t"]


def test_no_invented_dates():
    rf = _rec(); records.attach_stages(rf)
    for items in rf.values():
        for it in items:
            dated = [r for r in it["stage"]["ladder"] if r["at"]]
            assert len(dated) <= 1 and all(r["at"] == it["stage"]["since"] for r in dated), it["id"]


def test_nomination_stage_is_latest_action():
    """TRUTH-FIX-M4: a returned nomination is 'returned', never 'received'; the dated rung is the stated action's."""
    it = {"t": "2026-01-03T00:00:00Z", "title": "PN655-2: X, of Y, to be United States District Judge.", "sources": [{"url": "https://www.congress.gov/nomination/119th-congress/655"}],
          "plain": "Latest action 2026-01-03: Returned to the President under the provisions of Senate Rule XXXI, paragraph 6 of the Standing Rules of the Senate. (organization: The Judiciary)."}
    st = records.stage_for(it, "nominations")
    assert st["current"] == "returned", st["current"]
    assert [r["key"] for r in st["ladder"] if r["at"]] == ["returned"], st["ladder"]
    assert records.check(st) == [], records.check(st)


if __name__ == "__main__":
    bad = 0
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            try:
                f(); print("PASS", n)
            except AssertionError as e:
                bad += 1; print("FAIL", n, str(e)[:300])
    sys.exit(1 if bad else 0)

