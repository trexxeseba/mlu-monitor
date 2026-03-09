"""
detector_bajas.py
- Lee output/.run_meta.json para saber qué sellers procesó monitor.js OK
- Solo compara sellers que tuvieron run exitoso (ok=true)
- Sellers fallidos → reportados en resumen.txt como SKIP, nunca como bajas
- Compara penúltimo snapshot vs último (por timestamp compartido del run)
"""

import os, sys, json, csv, urllib.request
from datetime import datetime, timezone
from collections import defaultdict

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('FATAL: faltan SUPABASE_URL / SUPABASE_KEY')
    sys.exit(1)

OUTPUT = 'output'
os.makedirs(OUTPUT, exist_ok=True)

# ─── Leer metadata del run ────────────────────────────────────────────────────

def load_run_meta():
    path = os.path.join(OUTPUT, '.run_meta.json')
    if not os.path.exists(path):
        print('WARN: .run_meta.json no encontrado — se asume que monitor.js no corrió')
        return None
    with open(path) as f:
        return json.load(f)

# ─── Supabase REST ────────────────────────────────────────────────────────────

def supabase_get(table, params=''):
    url = f'{SUPABASE_URL}/rest/v1/{table}?{params}'
    req = urllib.request.Request(url, headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

def load_snapshots():
    print('Cargando snapshots desde Supabase...')
    try:
        rows = supabase_get(
            'snapshots',
            'select=seller_id,meli_item_id,title,price,timestamp&order=timestamp.asc'
        )
    except Exception as e:
        print(f'ERROR cargando snapshots: {e}')
        return {}

    print(f'  Rows recibidas: {len(rows)}')

    # { seller_id: { timestamp: { item_id: {title, price} } } }
    by_seller = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        sid = str(row['seller_id'])
        ts  = row['timestamp'][:19]   # truncar a segundos para agrupar bien
        iid = row['meli_item_id']
        by_seller[sid][ts][iid] = {
            'title': row.get('title') or '(sin título)',
            'price': row.get('price'),
        }
    return by_seller

# ─── Comparar snapshots ───────────────────────────────────────────────────────

def detect(by_seller, sellers_ok):
    """
    sellers_ok: set de seller_ids que monitor.js procesó correctamente.
    Solo esos participan en la detección de bajas.
    """
    bajas        = []
    reaparecidos = []
    now          = datetime.now(timezone.utc).isoformat()

    for sid, snapshots in by_seller.items():
        sorted_ts = sorted(snapshots.keys())

        if sid not in sellers_ok:
            print(f'  Seller {sid}: SKIP — no procesado OK en este run (no se buscan bajas)')
            continue

        if len(sorted_ts) < 2:
            print(f'  Seller {sid}: solo {len(sorted_ts)} snapshot — necesita ≥2 para comparar')
            continue

        ts_prev = sorted_ts[-2]
        ts_curr = sorted_ts[-1]
        prev    = snapshots[ts_prev]
        curr    = snapshots[ts_curr]

        print(f'  Seller {sid}: {ts_prev} → {ts_curr}')
        print(f'    Prev: {len(prev)} items | Curr: {len(curr)} items')

        # Bajas: en prev, no en curr
        for iid, meta in prev.items():
            if iid not in curr:
                bajas.append({
                    'detected_at':     now,
                    'seller_id':       sid,
                    'item_id':         iid,
                    'title_last_seen': meta['title'],
                    'price_last_seen': meta['price'],
                    'event_type':      'baja_detectada',
                })

        # Reaparecidos: en curr, no en prev, pero sí en algún snapshot anterior
        ever_seen = set()
        for ts in sorted_ts[:-1]:
            ever_seen.update(snapshots[ts].keys())

        for iid, meta in curr.items():
            if iid not in prev and iid in ever_seen:
                reaparecidos.append({
                    'detected_at': now,
                    'seller_id':   sid,
                    'item_id':     iid,
                    'title':       meta['title'],
                    'price':       meta['price'],
                    'event_type':  'reaparecio',
                })

    return bajas, reaparecidos

# ─── Escribir outputs ─────────────────────────────────────────────────────────

def write_csv(path, rows, fieldnames):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f'  {path} — {len(rows)} filas')

def write_resumen(path, run_meta, by_seller, bajas, reaparecidos):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    sellers_results = run_meta['sellers'] if run_meta else []
    ok_sellers   = [s for s in sellers_results if s['ok']]
    fail_sellers = [s for s in sellers_results if not s['ok']]

    total_items = 0
    for s in ok_sellers:
        sid = str(s['sellerId'])
        if sid in by_seller:
            ts_last = sorted(by_seller[sid].keys())[-1]
            total_items += len(by_seller[sid][ts_last])

    lines = [
        'MLU Monitor — Resumen de ejecución',
        f'Fecha/hora: {now}',
        f'Run timestamp: {run_meta["run_timestamp"] if run_meta else "desconocido"}',
        '',
        f'Sellers procesados OK:  {len(ok_sellers)}',
        f'Sellers fallidos/skip:  {len(fail_sellers)}',
        f'Total items actuales:   {total_items}',
        f'Bajas nuevas detectadas: {len(bajas)}',
        f'Reaparecidos:            {len(reaparecidos)}',
    ]

    if fail_sellers:
        lines.append('')
        lines.append('SELLERS FALLIDOS (excluidos de detección):')
        for s in fail_sellers:
            lines.append(f'  SKIP seller {s["sellerId"]} — {s.get("reason","error")}')

    lines += [
        '',
        f'Estado monitor.js:       {"OK" if run_meta else "ERROR — .run_meta.json no encontrado"}',
        f'Estado detector_bajas.py: OK',
    ]

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'  {path}')

def write_html(path, bajas, reaparecidos, run_meta):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    run_ts = run_meta['run_timestamp'] if run_meta else '?'

    def make_table(rows, cols, keys):
        if not rows:
            return '<p style="color:#888">Sin registros en este run.</p>'
        th = ''.join(f'<th>{c}</th>' for c in cols)
        trs = ''
        for r in rows:
            trs += '<tr>' + ''.join(f'<td>{r.get(k,"")}</td>' for k in keys) + '</tr>\n'
        return f'<table>\n<tr>{th}</tr>\n{trs}</table>'

    bajas_html = make_table(
        bajas,
        ['Detectado', 'Seller', 'Item ID', 'Título', 'Precio', 'Evento'],
        ['detected_at','seller_id','item_id','title_last_seen','price_last_seen','event_type'],
    )
    reap_html = make_table(
        reaparecidos,
        ['Detectado', 'Seller', 'Item ID', 'Título', 'Precio', 'Evento'],
        ['detected_at','seller_id','item_id','title','price','event_type'],
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>MLU Monitor — {now}</title>
<style>
  body  {{ font-family: sans-serif; margin: 24px; color: #222; }}
  h1   {{ margin-bottom: 4px; }}
  .meta {{ color: #666; font-size: 0.9em; margin-bottom: 20px; }}
  h2   {{ margin-top: 32px; border-bottom: 2px solid #ddd; padding-bottom: 4px; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
  th   {{ background: #333; color: #fff; padding: 8px 10px; text-align: left; }}
  td   {{ padding: 6px 10px; border-bottom: 1px solid #eee; }}
  tr:hover td {{ background: #f9f9f9; }}
  .badge-r {{ background:#fdd; color:#900; padding:3px 10px; border-radius:12px; font-weight:bold; }}
  .badge-g {{ background:#dfd; color:#060; padding:3px 10px; border-radius:12px; font-weight:bold; }}
</style>
</head>
<body>
<h1>MLU Monitor</h1>
<div class="meta">
  Generado: <strong>{now}</strong> &nbsp;|&nbsp;
  Run timestamp: <strong>{run_ts[:19]}</strong>
</div>
<p>
  <span class="badge-r">🔴 Bajas detectadas: {len(bajas)}</span>
  &nbsp;&nbsp;
  <span class="badge-g">🟢 Reaparecidos: {len(reaparecidos)}</span>
</p>

<h2>🔴 Bajas detectadas ({len(bajas)})</h2>
{bajas_html}

<h2>🟢 Reaparecidos ({len(reaparecidos)})</h2>
{reap_html}
</body>
</html>"""

    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  {path}')

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('=== Detector Bajas inicio ===')
    print(datetime.now(timezone.utc).isoformat())

    run_meta = load_run_meta()
    if run_meta:
        print(f'  Run timestamp: {run_meta["run_timestamp"]}')
        sellers_ok = {str(s['sellerId']) for s in run_meta['sellers'] if s['ok']}
        sellers_fail = {str(s['sellerId']) for s in run_meta['sellers'] if not s['ok']}
        print(f'  Sellers OK: {len(sellers_ok)} | Fallidos/skip: {len(sellers_fail)}')
        if sellers_fail:
            print(f'  Sellers excluidos de detección: {sellers_fail}')
    else:
        sellers_ok = set()

    by_seller = load_snapshots()

    if not by_seller:
        print('WARN: snapshots vacíos')
        bajas, reaparecidos = [], []
    else:
        print(f'\nComparando snapshots (solo sellers OK):')
        bajas, reaparecidos = detect(by_seller, sellers_ok)

    print(f'\nResultados: {len(bajas)} bajas | {len(reaparecidos)} reaparecidos')

    print('\nEscribiendo output/:')
    write_csv(f'{OUTPUT}/bajas_detectadas.csv', bajas,
              ['detected_at','seller_id','item_id','title_last_seen','price_last_seen','event_type'])
    write_csv(f'{OUTPUT}/reaparecidos.csv', reaparecidos,
              ['detected_at','seller_id','item_id','title','price','event_type'])
    write_resumen(f'{OUTPUT}/resumen.txt', run_meta, by_seller, bajas, reaparecidos)
    write_html(f'{OUTPUT}/reporte.html', bajas, reaparecidos, run_meta)

    print('\n=== Detector Bajas fin ===')

main()
