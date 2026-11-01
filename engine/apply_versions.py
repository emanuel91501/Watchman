#!/usr/bin/env python3
"""Add if_version to batch writes and re-split the batches.

  python3 apply_versions.py --out out/morning --versions versions.json

versions.json: {"collection/doc_id": version}, built by the runner from ArtifactData list results.
Every `set` whose doc exists in the map gets "if_version": <n>; docs not in the map are new and carry none.
Batches are rewritten in place (batch_001.json ...) with <= 50 writes and <= 900 KB each; write order is kept.
"""
import argparse, glob, json, os, re, sys

MAX_WRITES, MAX_BYTES = 50, 900 * 1024


def dump(o):
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"))


def apply(writes, versions):
    out, with_v, new = [], 0, 0
    for w in writes:
        w = {k: v for k, v in w.items() if k != "if_version"}   # idempotent: rebuild from the map every time
        key = f"{w['collection']}/{w['doc_id']}"
        if w.get("op") == "set" and key in versions:
            w["if_version"] = versions[key]; with_v += 1
        else:
            new += 1
        out.append(w)
    return out, with_v, new


def split(writes):
    batches, cur, size = [], [], 2
    for w in writes:
        b = len(dump(w).encode("utf-8")) + 1
        if b + 2 > MAX_BYTES:
            raise ValueError(f"{w['collection']}/{w['doc_id']} alone is {b} bytes > {MAX_BYTES}")
        if cur and (len(cur) >= MAX_WRITES or size + b > MAX_BYTES):
            batches.append(cur); cur, size = [], 2
        cur.append(w); size += b
    if cur:
        batches.append(cur)
    return batches


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True); ap.add_argument("--versions", required=True)
    a = ap.parse_args(argv)
    versions = json.load(open(a.versions, encoding="utf-8"))
    files = sorted(f for f in glob.glob(os.path.join(a.out, "batch_*.json")) if re.fullmatch(r"batch_\d+\.json", os.path.basename(f)))
    if not files:
        sys.exit("no batch files in " + a.out)
    writes = [w for f in files for w in json.load(open(f, encoding="utf-8"))]
    new_w, with_v, new = apply(writes, versions)
    batches = split(new_w)
    for f in files:
        os.remove(f)
    sizes = []
    for i, b in enumerate(batches, 1):
        p = os.path.join(a.out, f"batch_{i:03d}.json")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(dump(b))
        sizes.append(os.path.getsize(p))
    unused = len(set(versions) - {f"{w['collection']}/{w['doc_id']}" for w in writes})
    print(f"apply_versions: {len(writes)} writes: {with_v} with if_version, {new} new; {len(files)} -> {len(batches)} batches "
          f"(max {max(len(b) for b in batches)} writes, max {max(sizes)} bytes); {unused} versions in the map not used")
    return {"writes": new_w, "batches": len(batches)}


if __name__ == "__main__":
    main()
