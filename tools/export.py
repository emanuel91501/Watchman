#!/usr/bin/env python3
"""Apply one engine run to the static site.

  python3 tools/export.py --out engine/out/hourly            # after engine.py --mode hourly
  python3 tools/export.py --out engine/out/morning           # after engine.py --mode morning (also reads out/morning/hourly)
  python3 tools/export.py --seed DB.json --blobs DIR --map blob-map.json   # one-time seed from an artifact dump

site/data/db.json is the whole store: {"<collection>/<doc_id>": doc}. The app loads it once per visit.
Photos live in site/img/<id>.jpg and are referenced as "img/<id>.jpg".
"""
import argparse, datetime as dt, glob, json, os, re, shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'site', 'data', 'db.json')
IMG = os.path.join(ROOT, 'site', 'img')
DATED = ('events', 'wire', 'news', 'time', 'brief')
KEEP_DAYS = 8
KEEP_LIVE_H = 72


def load():
    return json.load(open(DB)) if os.path.exists(DB) else {}


def save(db):
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    tmp = DB + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(db, f, separators=(',', ':'), ensure_ascii=False)
    os.replace(tmp, DB)


def apply_dir(db, d):
    n = 0
    for f in sorted(glob.glob(os.path.join(d, 'batch_*.json'))):
        for w in json.load(open(f)):
            if w.get('op', 'set') != 'set':
                continue
            data = w.get('data')
            if data is None and w.get('file_path'):
                data = json.load(open(os.path.join(d, w['file_path'])))
            db[f"{w['collection']}/{w['doc_id']}"] = data
            n += 1
    return n


def images(db, d, doc):
    man = os.path.join(d, 'img', 'manifest.json')
    if not os.path.exists(man):
        return 0
    rows = []
    os.makedirs(IMG, exist_ok=True)
    for r in json.load(open(man)):
        src = os.path.join(d, r['file'])
        if not os.path.exists(src):
            continue
        name = re.sub(r'[^A-Za-z0-9._-]', '_', r['id']) + '.jpg'
        shutil.copyfile(src, os.path.join(IMG, name))
        row = {k: v for k, v in r.items() if k != 'file'}
        row['url'] = 'img/' + name
        rows.append(row)
    if doc == 'img/index':
        old = {x['id']: x for x in (db.get('img/index') or {}).get('images', [])}
        old.update({x['id']: x for x in rows})
        rows = list(old.values())
    db[doc] = {'images': rows}
    return len(rows)


def prune(db):
    now = dt.datetime.now(dt.timezone.utc)
    cut_day = (now - dt.timedelta(days=KEEP_DAYS)).strftime('%Y-%m-%d')
    cut_live = (now - dt.timedelta(hours=KEEP_LIVE_H)).strftime('%Y-%m-%dT%H%M')
    for k in list(db):
        c, _, i = k.partition('/')
        if c in DATED and i[:10] < cut_day and re.match(r'\d{4}-\d{2}-\d{2}', i):
            del db[k]
        elif c == 'live' and i < cut_live:
            del db[k]
        elif c == 'img' and i.startswith('h') and i[1:] < cut_live:
            del db[k]
    used = set(re.findall(r'img/([A-Za-z0-9._-]+\.jpg)', json.dumps(db)))
    for f in os.listdir(IMG) if os.path.isdir(IMG) else []:
        if f not in used:
            os.remove(os.path.join(IMG, f))


def run(out):
    db = load()
    rep = json.load(open(os.path.join(out, 'report.json')))
    mode = rep.get('mode', 'hourly')
    n = apply_dir(db, out)
    if mode == 'morning':
        n_img = images(db, out, 'img/index')
        hd = os.path.join(out, 'hourly')
        if os.path.isdir(hd):
            n += apply_dir(db, hd)
            hr = json.load(open(os.path.join(hd, 'report.json'))) if os.path.exists(os.path.join(hd, 'report.json')) else {}
            ids = hr.get('doc_ids') or []
            if ids:
                n_img += images(db, hd, 'img/h' + ids[0])
    else:
        ids = rep.get('doc_ids') or []
        n_img = images(db, out, 'img/h' + ids[0]) if ids else 0
    prune(db)
    save(db)
    print(f'{mode}: {n} docs written, {n_img} photos, store now {len(db)} docs')


def seed(path, blobs, bmap):
    db = json.load(open(path))
    m = json.load(open(bmap))
    os.makedirs(IMG, exist_ok=True)
    for k, f in m.items():
        bid = k.rsplit('/', 1)[1]
        src = os.path.join(blobs, os.path.basename(f))
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(IMG, bid + '.jpg'))
    s = re.sub(r'/_blob/([A-Za-z0-9]+)', r'img/\1.jpg', json.dumps(db, ensure_ascii=False))
    db = json.loads(s)
    db.pop('__blobs__', None)
    save(db)
    print(f'seeded {len(db)} docs, {len(os.listdir(IMG))} photos')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--out'); ap.add_argument('--seed'); ap.add_argument('--blobs'); ap.add_argument('--map')
    a = ap.parse_args()
    if a.seed:
        seed(a.seed, a.blobs, a.map)
    else:
        run(a.out)
