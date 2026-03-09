"""
detector_bajas.py
Compara penúltimo snapshot vs último snapshot por seller.
- baja_detectada  = item en penúltimo que NO está en último
- reaparecio      = item que faltó antes y volvió al último
Salida: output/bajas_detectadas.csv, output/reaparecidos.csv,
        output/reporte.html, output/resumen.txt
"""

import os, sys, json, csv
from datetime import datetime, timezone
from collections import defaultdict

# ─── Supabase REST helper ──────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('FATAL: faltan SUPABASE_URL / SUPABASE_KEY')
    sys.exit(1)

try:
    import urllib.request
except ImportError:
    pass

def supabase_get(table, params=''):
    url = f'{SUPABASE_URL}/rest/v1/{table}?{params}'
    req = urllib.request.Request(url, headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

# ─── Cargar snapshots ──────────────────────────────────────────────────────────

def load_snapshots():
    print('Cargando snapshots desde Supabase...')
    try:
        rows = supabase_get('snapshots', 'select=seller_id,meli_item_id,title,price,timestamp&order=timestamp.asc')
    except Exception as e:
        print(f'ERROR cargando snapshots: {e}')
        return {}

    print(f'  Rows recibidas: {len(rows)}')

    # Agrupar por seller → lista de snapshots (cada snapshot = timestamp + set de items)
    # Estructura: { seller_id: { timestamp: { item_id: {title, price} } } }
    by_seller = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        sid = str(row['seller_id'])
        ts  = row['timestamp']
        iid = row['meli_item_id']
        by_seller[sid][ts][iid] = {
            'title': row.get('title') or '(sin título)',
            'price': row.get('price'),
        }

    return by_seller

# ─── Comparar penúltimo vs último ─────────────────────────────────────────────

def detect(by_seller):
    bajas       = []  # items que desaparecieron
    reaparecidos = []  # items que volvieron

    now = datetime.now(timezone.utc).isoformat()

    for sid, snapshots in by_seller.items():
        sorted_ts = sorted(snapshots.keys())

        if len(sorted_ts) < 2:
            print(f'  Seller {sid}: solo {len(sorted_ts)} snapshot(s) — necesita al menos 2 para comparar')
            continue

        ts_prev = sorted_ts[-2]
        ts_curr = sorted_ts[-1]
        prev    = snapshots[ts_prev]  # { item_id: {title, price} }
        curr    = snapshots[ts_curr]  # { item_id: {title, price} }

        print(f'  Seller {sid}: comparando {ts_prev[:19]} → {ts_curr[:19]}')
        print(f'    Prev: {len(prev)} items | Curr: {len(curr)} items')

        # Bajas: estaban en prev, no están en curr
        for iid, meta in prev.items():
            if iid not in curr:
                bajas.append({
                    'detected_at':    now,
                    'seller_id':      sid,
                    'item_id':        iid,
                    'title_last_seen': meta['title'],
                    'price_last_seen': meta['price'],
                    'event_type':     'baja_detectada',
                })

        # Reaparecidos: no estaban en prev, sí están en curr
        # (para ser reaparición real, tiene que haber estado en algún snapshot anterior)
        all_ts     = sorted_ts[:-1]  # todos menos el último
        ever_seen  = set()
        for ts in all_ts:
            ever_seen.update(snapshots[ts].keys())

        for iid, meta in curr.items():
            if iid not in snapshots[ts_prev] and iid in ever_seen:
                reaparecidos.append({
                    'detected_at': now,
                    'seller_id':   sid,
                    'item_id':     iid,
                    'title':       meta['title'],
                    'price':       meta['price'],
                    'event_type':  'reaparecio',
                })

        nuevos = len([i for i in curr if i not in ever_seen and i not in prev])
        if nuevos:
            print(f'    Nuevos (nunca vistos): {nuevos} — no se reportan como reaparecidos')

    return bajas, reaparecidos

# ─── Escribir outputs ─────────────────────────────────────────────────────────

def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f'  Escrito: {path} ({len(rows)} filas)')

def write_resumen(path, by_seller, bajas, reaparecidos, monitor_ok):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    total_items = sum(
        len(items)
        for snapshots in by_seller.values()
        for ts, items in snapshots.items()
        if ts == sorted(snapshots.keys())[-1]
    )
    lines = [
        f'MLU Monitor — Resumen de ejecución',
        f'Fecha/hora: {now}',
        f'',
        f'Sellers procesados: {len(by_seller)}',
        f'Total items actuales (último snapshot): {total_items}',
        f'Bajas nuevas detectadas: {len(bajas)}',
        f'Reaparecidos: {len(reaparecidos)}',
        f'',
        f'Estado monitor.js: {"OK" if monitor_ok else "ERROR — ver log del run"}',
        f'Estado detector_bajas.py: OK',
    ]
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'  Escrito: {path}')

def write_html(path, bajas, reaparecidos):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    def table(rows, cols, keys):
        if not rows:
            return '<p>Sin registros en este run.</p>'
        th = ''.join(f'<th>{c}</th>' for c in cols)
        trs = ''
        for r in rows:
            trs += '<tr>' + ''.join(f'<td>{r.get(k,"")}</td>' for k in keys) + '</tr>'
        return f'<table border="1" cellpadding="6" cellspacing="0">\n<tr>{th}</tr>\n{trs}\n</table>'

    bajas_html = table(
        bajas,
        ['Fecha detección','Seller ID','Item ID','Título','Precio','Evento'],
        ['detected_at','seller_id','item_id','title_last_seen','price_last_seen','event_type'],
    )
    reap_html = table(
        reaparecidos,
        ['Fecha detección','Seller ID','Item ID','Título','Precio','Evento'],
        ['detected_at','seller_id','item_id','title','price','event_type'],
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8">
<title>MLU Monitor — {now}</title>
<style>
  body {{ font-family: sans-serif; margin: 20px; }}
  h1   {{ color: #333; }}
  h2   {{ margin-top: 30px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th   {{ background: #444; color: #fff; padding: 8px; text-align: left; }}
  td   {{ padding: 6px; }}
  tr:nth-child(even) {{ background: #f4f4f4; }}
  .badge {{ display:inline-block; padding:3px 8px; border-radius:4px; font-size:0.85em; }}
  .baja {{ background:#fdd; color:#900; }}
  .reap {{ background:#dfd; color:#060; }}
</style>
</head>
<body>
<h1>MLU Monitor</h1>
<p>Generado: <strong>{now}</strong></p>
<p>
  <span class="badge baja">Bajas detectadas: {len(bajas)}</span>
  &nbsp;
  <span class="badge reap">Reaparecidos: {len(reaparecidos)}</span>
</p>

<h2>🔴 Bajas detectadas ({len(bajas)})</h2>
{bajas_html}

<h2>🟢 Reaparecidos ({len(reaparecidos)})</h2>
{reap_html}
</body>
</html>"""

    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Escrito: {path}')

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('=== Detector Bajas inicio ===')
    print(datetime.now(timezone.utc).isoformat())

    monitor_ok = os.path.exists('output/.monitor_ok')

    by_seller = load_snapshots()

    if not by_seller:
        print('WARN: no hay datos en snapshots — generando outputs vacíos')
        bajas, reaparecidos = [], []
    else:
        print(f'\nComparando snapshots por seller:')
        bajas, reaparecidos = detect(by_seller)

    print(f'\nResultados: {len(bajas)} bajas | {len(reaparecidos)} reaparecidos')

    print('\nEscribiendo output/:')
    write_csv('output/bajas_detectadas.csv', bajas,
              ['detected_at','seller_id','item_id','title_last_seen','price_last_seen','event_type'])
    write_csv('output/reaparecidos.csv', reaparecidos,
              ['detected_at','seller_id','item_id','title','price','event_type'])
    write_resumen('output/resumen.txt', by_seller, bajas, reaparecidos, monitor_ok)
    write_html('output/reporte.html', bajas, reaparecidos)

    print('\n=== Detector Bajas fin ===')

main()
