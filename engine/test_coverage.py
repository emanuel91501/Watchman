"""P2 derived-place tests over the C1 fixture. Run: python3 test_coverage.py (offline; fetch is injected)."""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import coverage
from images import contract as C
FX = os.path.join(HERE, "..", "app", "dev", "fixtures", "C1.json")


def _items():
    d = json.load(open(FX)); items, clusters = [], []
    for k, v in d.items():
        c = k.split("/")[0]
        if c in ("events", "wire", "records", "live"):
            for key in ("items", "events", "wire", "breaking"):
                items += [i for i in v.get(key) or [] if isinstance(i, dict)]
        if c == "news":
            clusters += [i for i in v.get("clusters") or [] if isinstance(i, dict)]
    existing = {k.split("/", 1)[1] for k in d if k.startswith("places/")}
    return items, clusters, existing


def test_every_named_place_has_doc():
    items, clusters, existing = _items()
    docs = coverage.derived_places(items, existing, get=None, clusters=clusters)
    have = existing | {d for _, d, _ in docs}
    names = coverage.named(items + clusters)
    assert len(names) >= 10, len(names)
    missing = sorted(set(names) - have)
    assert not missing, missing
    for _, pid, doc in docs:
        assert doc["derived"] is True and (doc.get("fips") or doc.get("iso3")), pid
        assert C.validate(doc["hero"], "place") == [] and doc["hero"]["kind"] == "plate", pid
        assert doc["numbers"] == [] and "not fetched" in doc["limits"], pid   # no fetch -> no numbers, said so


def test_texas_has_page():
    items = [{"id": "x1", "title": "Flood Warning", "place": {"admin": "Texas"}}]
    docs = coverage.derived_places(items, set())
    assert [d for _, d, _ in docs] == ["texas"] and docs[0][2]["fips"] == "48"


def test_fetch_by_code_and_failure_is_stated():
    calls = []
    def get(url):
        calls.append(url)
        if "census" in url:
            return [["NAME", "B01003_001E", "state"], ["Texas", "29640343", "48"]]
        raise OSError("blocked")
    items = [{"id": "a", "place": {"admin": "Texas"}}, {"id": "b", "title": "Talks in Japan", "place": {}}]
    docs = {d: doc for _, d, doc in coverage.derived_places(items, set(), get=get, fetched_at="2026-10-31T23:00:00Z")}
    assert any("state:48" in u for u in calls) and any("/country/JPN/" in u for u in calls)
    assert docs["texas"]["numbers"][0]["value"] == 29640343 and docs["texas"]["numbers"][0]["prov"] == "fetched"
    assert docs["japan"]["numbers"] == [] and "World Bank fetch failed" in docs["japan"]["limits"]


def test_existing_places_not_overwritten():
    items = [{"id": "a", "place": {"admin": "Indiana"}}, {"id": "b", "place": {"admin": "Iran"}}]
    assert coverage.derived_places(items, {"indiana", "iran"}) == []


if __name__ == "__main__":
    bad = 0
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            try:
                f(); print("PASS", n)
            except AssertionError as e:
                bad += 1; print("FAIL", n, str(e)[:300])
    sys.exit(1 if bad else 0)
