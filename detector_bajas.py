"""
detector_bajas.py

Fuente de verdad para exclusiones: output/monitor_status.csv
- solo sellers con status=ok participan en detección
- sellers con status=failed → SKIP explícito, nunca baja_detectada

Lógica de comparación:
- agrupa snapshots por seller + run_id (no por timestamp fila a fila)
- penúltimo run válido vs último run válido POR seller
- válido = run donde ese seller tuvo status=ok en monitor_status.csv
"""

import os, sys, csv, json, urllib.request
from datetime import datetime, timezone
from collections import defaultdict

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
OUTPUT = 'output'

if not SUPABASE_URL or not SUPABASE_KEY:
    print('FATAL: faltan SUPABASE_URL / SUPABASE_KEY')
    sys.exit(1)

os.makedirs(OUTPUT, exist_ok=True)

# ─── Leer monitor_status.csv ──────────────────────────────────────────────────

def load_monitor_status():
    path = os.path.join(OUTPUT, 'monitor_status.csv')
    if not os.path.exists(path):
        print('FATAL: output/monitor_status.csv no encontrado — monitor.js no generó trazas')
        sys.exit(1)

    sellers_ok     = set()
    sellers_failed = {}   # seller_id → error_message
    run_id         = None

    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            sid = row['seller_id']
            run_id = row['run_id']
            if row['status'] == 'ok':
                sellers_ok.add(sid)
            else:
                sellers_failed[sid] = row['error_message'] or 'error desconocido'

    print(f'monitor_status.csv → run_id={run_id}')
    print(f'  OK:     {len(sellers_ok)} sellers')
    print(f'  Fallidos: {len(sellers_failed)} sellers')
    if sellers_failed:
        for sid, msg in sellers_failed.items():
            print(f'    SKIP seller {sid}: {msg}')

    return run_id, sellers_ok, sellers_failed

# ─── Supabase REST ────────────────────────────────────────────────────────────

def supabase_get(params):
    url = f'{SUPABASE_URL}/rest/v1/snapshots?{params}'
    req = urllib.request.Request(url, headers={
        'apikey':         SUPABASE_KEY,
        'Authorization':  f'Bearer {SUPABASE_KEY}',
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

def load_snapshots(sellers_ok):
    """
    Carga snapshots solo de sellers OK.
    Agrupa por seller_id → run_id → { item_id: {title, price} }
    Usa run_id como identificador de snapshot (no timestamp fila a fila).
    Fallback: si run_id es NULL (datos viejos), usa checked_at truncado a segundos.
    """
    print('\nCargando snapshots desde Supabase...')

    # Construir filtro por seller_id
    seller_filter = ','.join(sellers_ok)
    try:
        rows = supabase_get(
            f'select=seller_id,meli_item_id,title,price,run_id,checked_at,timestamp'
            f'&seller_id=in.({seller_filter})'
            f'&order=checked_at.asc'
        )
    except Exception as e:
        print(f'ERROR cargando snapshots: {e}')
        return {}

    print(f'  Rows recibidas: {len(rows)}')

    # { seller_id: { run_key: { item_id: {title, price} } } }
    by_seller = defaultdict(lambda: defaultdict(dict))

    for row in rows:
        sid = str(row['seller_id'])
        iid = row['meli_item_id']

        # Identificador de snapshot: run_id si existe, sino checked_at, sino timestamp
        run_key = (
            row.get('run_id') or
            (row.get('checked_at') or '')[:19] or
            (row.get('timestamp') or '')[:19]
        )
        if not run_key:
            continue

        by_seller[sid][run_key][iid] = {
            'title': row.get('title') or '(sin título)',
            'price': row.get('price'),
        }

    for sid, runs in by_seller.items():
        print(f'  Seller {sid}: {len(runs)} snapshots válidos')

    return by_seller

# ─── Comparar penúltimo vs último snapshot válido ─────────────────────────────

def detect(by_seller, sellers_ok):
    bajas        = []
    reaparecidos = []
    now          = datetime.now(timezone.utc).isoformat()

    for sid in sellers_ok:
        snapshots = by_seller.get(sid, {})
        sorted_keys = sorted(snapshots.keys())

        if len(sorted_keys) < 2:
            n = len(sorted_keys)
            print(f'  Seller {sid}: {n} snapshot(s) válido(s) — necesita ≥2 para comparar')
            continue

        key_prev = sorted_keys[-2]
        key_curr = sorted_keys[-1]
        prev     = snapshots[key_prev]
        curr     = snapshots[key_curr]

        print(f'  Seller {sid}: comparando [{key_prev}] → [{key_curr}]')
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
        for k in sorted_keys[:-1]:
            ever_seen.update(snapshots[k].keys())

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

# ─── Outputs ──────────────────────────────────────────────────────────────────

def write_csv(path, rows, fieldnames):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f'  {path} ({len(rows)} filas)')

def write_resumen(path, run_id, sellers_ok, sellers_failed, by_seller, bajas, reaparecidos):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    total_items = 0
    sellers_con_un_solo_snap = []
    for sid in sellers_ok:
        snaps = by_seller.get(sid, {})
        if snaps:
            last_key = sorted(snaps.keys())[-1]
            total_items += len(snaps[last_key])
        if len(snaps) < 2:
            sellers_con_un_solo_snap.append(sid)

    lines = [
        'MLU Monitor — Resumen de ejecución',
        f'Fecha/hora:  {now}',
        f'Run ID:      {run_id}',
        '',
        f'Sellers procesados OK:          {len(sellers_ok)}',
        f'Sellers fallidos (excluidos):   {len(sellers_failed)}',
        f'Sellers con solo 1 snapshot:    {len(sellers_con_un_solo_snap)} (sin comparación aún)',
        f'Total items actuales:           {total_items}',
        '',
        f'Bajas nuevas detectadas:        {len(bajas)}',
        f'Reaparecidos:                   {len(reaparecidos)}',
    ]

    if sellers_failed:
        lines += ['', 'SELLERS FALLIDOS — excluidos de detección de bajas:']
        for sid, msg in sellers_failed.items():
            lines.append(f'  SKIP  seller {sid}  motivo: {msg}')

    if sellers_con_un_solo_snap:
        lines += ['', 'SELLERS CON 1 SOLO SNAPSHOT — esperando próximo run:']
        for sid in sellers_con_un_solo_snap:
            lines.append(f'  WAIT  seller {sid}')

    lines += [
        '',
        'Estado monitor.js:        OK (monitor_status.csv presente)',
        'Estado detector_bajas.py: OK',
    ]

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'  {path}')

def write_html(path, bajas, reaparecidos, run_id):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    def make_table(rows, cols, keys):
        if not rows:
            return '<p class="empty">Sin registros en este run.</p>'
        th  = ''.join(f'<th>{c}</th>' for c in cols)
        trs = ''.join(
            '<tr>' + ''.join(f'<td>{r.get(k,"")}</td>' for k in keys) + '</tr>'
            for r in rows
        )
        return f'<table><tr>{th}</tr>{trs}</table>'

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>MLU Monitor — {now}</title>
<style>
  body  {{ font-family: sans-serif; margin: 24px; color: #222; }}
  h1   {{ margin-bottom: 2px; }}
  .meta {{ color: #666; font-size: .9em; margin-bottom: 20px; }}
  h2   {{ margin-top: 32px; border-bottom: 2px solid #ddd; padding-bottom: 4px; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: .92em; }}
  th   {{ background: #333; color: #fff; padding: 8px 10px; text-align: left; }}
  td   {{ padding: 6px 10px; border-bottom: 1px solid #eee; }}
  tr:hover td {{ background: #f9f9f9; }}
  .br  {{ background:#fdd; color:#900; padding:3px 10px; border-radius:12px; font-weight:bold; }}
  .bg  {{ background:#dfd; color:#060; padding:3px 10px; border-radius:12px; font-weight:bold; }}
  .empty {{ color:#888; }}
</style>
</head>
<body>
<h1>MLU Monitor</h1>
<div class="meta">
  Generado: <strong>{now}</strong> &nbsp;|&nbsp; Run ID: <code>{run_id}</code>
</div>
<p>
  <span class="br">🔴 Bajas detectadas: {len(bajas)}</span>
  &nbsp;&nbsp;
  <span class="bg">🟢 Reaparecidos: {len(reaparecidos)}</span>
</p>

<h2>🔴 Bajas detectadas ({len(bajas)})</h2>
{make_table(bajas,
    ['Detectado','Seller','Item ID','Título','Precio','Evento'],
    ['detected_at','seller_id','item_id','title_last_seen','price_last_seen','event_type'])}

<h2>🟢 Reaparecidos ({len(reaparecidos)})</h2>
{make_table(reaparecidos,
    ['Detectado','Seller','Item ID','Título','Precio','Evento'],
    ['detected_at','seller_id','item_id','title','price','event_type'])}
</body>
</html>"""

    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  {path}')

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('=== Detector Bajas inicio ===')
    print(datetime.now(timezone.utc).isoformat())

    run_id, sellers_ok, sellers_failed = load_monitor_status()

    by_seller = load_snapshots(sellers_ok)

    print('\nComparando snapshots:')
    bajas, reaparecidos = detect(by_seller, sellers_ok)
    print(f'\nResultados: {len(bajas)} bajas | {len(reaparecidos)} reaparecidos')

    print('\nEscribiendo output/:')
    write_csv(f'{OUTPUT}/bajas_detectadas.csv', bajas,
              ['detected_at','seller_id','item_id','title_last_seen','price_last_seen','event_type'])
    write_csv(f'{OUTPUT}/reaparecidos.csv', reaparecidos,
              ['detected_at','seller_id','item_id','title','price','event_type'])
    write_resumen(f'{OUTPUT}/resumen.txt',
                  run_id, sellers_ok, sellers_failed, by_seller, bajas, reaparecidos)
    write_html(f'{OUTPUT}/reporte.html', bajas, reaparecidos, run_id)

    print('\n=== Detector Bajas fin ===')

main()
