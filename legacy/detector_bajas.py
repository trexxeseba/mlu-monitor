"""
detector_bajas.py

Compara el último run válido contra el run válido anterior.
Solo trabaja con item_ids extraídos del listado — sin dependencia de price,
stock ni sold_quantity.

Clasificaciones de cambio:
  nuevo                   → item aparece en run actual pero no en el anterior
  desaparecido_no_confirmado → item estaba y ya no aparece
  reaparecido             → item vuelve a aparecer tras haber sido registrado
                             como desaparecido_no_confirmado en bajas_detectadas

Salida:
  0 → detector completó (aunque no haya cambios)
  1 → error técnico o insuficientes runs válidos
"""

import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('❌ FATAL: Faltan SUPABASE_URL o SUPABASE_KEY')
    sys.exit(1)

REST_URL = f'{SUPABASE_URL}/rest/v1'
HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Prefer': 'return=representation',
}

SEP = '═' * 70


# ─── HTTP helpers ─────────────────────────────────────────────────────────────
def _get(path, params=None):
    url = f'{REST_URL}/{path}'
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={**HEADERS, 'Prefer': 'count=exact'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f'{REST_URL}/{path}',
        data=data,
        headers={**HEADERS},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _select(table, params):
    """Build a GET request with PostgREST query params."""
    url = f'{REST_URL}/{table}?' + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


# ─── Obtener los 2 últimos runs válidos ────────────────────────────────────────
def get_last_two_valid_runs():
    """Intenta monitor_runs; fallback a los 2 run_id más recientes de snapshots."""
    print('📊 Consultando últimos 2 runs válidos...')

    # Intento 1: monitor_runs
    try:
        data = _select('monitor_runs', {
            'status': 'eq.valid',
            'order': 'finished_at.desc',
            'limit': 2,
        })
        if len(data) >= 2:
            current, previous = data[0], data[1]
            print(f'  [monitor_runs] Run actual:   {current["run_id"]}')
            print(f'  [monitor_runs] Run anterior: {previous["run_id"]}')
            return current, previous
        elif len(data) == 1:
            print('⚠️  Solo 1 run válido en monitor_runs — insuficiente para comparar')
    except Exception as e:
        print(f'  ⚠️  monitor_runs no disponible: {e} → fallback a snapshots')

    # Intento 2: execution_logs (status = 'success')
    try:
        data2 = _select('execution_logs', {
            'status': 'eq.success',
            'order': 'executed_at.desc',
            'limit': 2,
        })
        if len(data2) >= 2:
            def to_run(row):
                return {'run_id': row['run_id'], 'total_items': row.get('items_processed', '?'), 'finished_at': row.get('executed_at', row['run_id'])}
            current, previous = to_run(data2[0]), to_run(data2[1])
            print(f'  [execution_logs] Run actual:   {current["run_id"]}')
            print(f'  [execution_logs] Run anterior: {previous["run_id"]}')
            return current, previous
    except Exception as e:
        print(f'  ⚠️  execution_logs no disponible: {e} → fallback a snapshots')

    # Intento 3 (fallback): los 2 run_id más recientes de snapshots
    print('  Fallback: buscando 2 run_ids distintos en snapshots...')
    try:
        data3 = _select('snapshots', {
            'select': 'run_id,checked_at',
            'order': 'checked_at.desc',
            'limit': 5000,
        })
        seen = []
        for row in data3:
            rid = row.get('run_id')
            if rid and rid not in seen:
                seen.append(rid)
            if len(seen) >= 2:
                break
        if len(seen) < 2:
            print(f'⚠️  Solo {len(seen)} run_id(s) en snapshots — insuficiente para comparar')
            return None, None

        def run_obj(run_id):
            return {'run_id': run_id, 'total_items': '?', 'finished_at': run_id}

        current  = run_obj(seen[0])
        previous = run_obj(seen[1])
        print(f'  [snapshots fallback] Run actual:   {current["run_id"]}')
        print(f'  [snapshots fallback] Run anterior: {previous["run_id"]}')
        return current, previous
    except Exception as e2:
        print(f'❌ No se pudo obtener runs de snapshots: {e2}')
        return None, None


# ─── Obtener set de item_ids para un run ──────────────────────────────────────
def get_item_ids_for_run(run_id):
    """Retorna {item_id: seller_id} para todos los items del run dado."""
    all_data = []
    page_size = 1000
    offset = 0
    while True:
        batch = _select('snapshots', {
            'select': 'item_id,meli_item_id,seller_id',
            'run_id': f'eq.{run_id}',
            'limit': page_size,
            'offset': offset,
        })
        all_data.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    items = {}
    for snap in all_data:
        iid = snap.get('item_id') or snap.get('meli_item_id')
        if iid and iid not in items:
            items[iid] = snap.get('seller_id')
    return items


# ─── Obtener item_ids que fueron desaparecidos (para detectar reaparecidos) ───
def get_previously_disappeared(item_ids):
    if not item_ids:
        return set()
    try:
        ids_str = ','.join(str(i) for i in item_ids)
        data = _select('bajas_detectadas', {
            'select': 'item_id',
            'tipo': 'eq.desaparecido_no_confirmado',
            'item_id': f'in.({ids_str})',
        })
        return {row['item_id'] for row in data}
    except Exception as e:
        print(f'  ⚠️  No se pudo consultar reaparecidos: {e}')
        return set()


# ─── Obtener item_ids que ya fueron registrados en bajas_detectadas ───────────
def get_previously_registered(item_ids):
    """Items que ya aparecieron en algún ciclo anterior (cualquier tipo)."""
    if not item_ids:
        return set()
    try:
        ids_str = ','.join(str(i) for i in item_ids)
        data = _select('bajas_detectadas', {
            'select': 'item_id',
            'item_id': f'in.({ids_str})',
        })
        return {row['item_id'] for row in data}
    except Exception as e:
        print(f'  ⚠️  No se pudo consultar items registrados: {e}')
        return set()


# ─── Verificar si ya se detectó para este run (evitar duplicados) ──────────────
def already_detected(current_run_id):
    try:
        data = _select('bajas_detectadas', {
            'select': 'id',
            'run_id': f'eq.{current_run_id}',
            'limit': 1,
        })
        return len(data) > 0
    except Exception:
        return False


# ─── Comparar dos sets de item_ids y clasificar cambios ──────────────────────
def detect_changes(current_items, previous_items, current_run_id):
    cambios = []
    now = datetime.now().isoformat()

    current_ids  = set(current_items.keys())
    previous_ids = set(previous_items.keys())

    # Items nuevos o reaparecidos
    nuevos_ids = current_ids - previous_ids
    prev_disappeared = get_previously_disappeared(nuevos_ids)

    for iid in nuevos_ids:
        seller_id = current_items[iid]
        tipo = 'reaparecido' if iid in prev_disappeared else 'nuevo'
        cambios.append({
            'tipo':            tipo,
            'item_id':         iid,
            'meli_item_id':    iid,
            'seller_id':       seller_id,
            'run_id':          current_run_id,
            'fecha_deteccion': now,
        })

    # Items desaparecidos
    for iid in previous_ids - current_ids:
        seller_id = previous_items[iid]
        cambios.append({
            'tipo':            'desaparecido_no_confirmado',
            'item_id':         iid,
            'meli_item_id':    iid,
            'seller_id':       seller_id,
            'run_id':          current_run_id,
            'fecha_deteccion': now,
        })

    return cambios


# ─── Guardar cambios en bajas_detectadas ──────────────────────────────────────
def guardar_cambios(cambios):
    if not cambios:
        print('ℹ️  Sin cambios para guardar')
        return 0

    print(f'\n💾 Insertando {len(cambios)} cambios en bajas_detectadas...')

    rows = []
    for c in cambios:
        rows.append({
            'seller_id':       c.get('seller_id'),
            'item_id':         c.get('item_id'),
            'meli_item_id':    c.get('meli_item_id'),
            'tipo':            c.get('tipo'),
            'run_id':          c.get('run_id'),
            'fecha_deteccion': c.get('fecha_deteccion'),
        })

    # Bulk insert
    data = json.dumps(rows).encode()
    req = urllib.request.Request(
        f'{REST_URL}/bajas_detectadas',
        data=data,
        headers={**HEADERS, 'Prefer': 'return=minimal'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            pass
        print(f'✅ {len(rows)} cambios guardados')
        return len(rows)
    except Exception as e:
        print(f'⚠️  Bulk insert falló ({e}), intentando de a uno...')
        ok = 0
        for row in rows:
            data2 = json.dumps([row]).encode()
            req2 = urllib.request.Request(
                f'{REST_URL}/bajas_detectadas',
                data=data2,
                headers={**HEADERS, 'Prefer': 'return=minimal'},
                method='POST',
            )
            try:
                with urllib.request.urlopen(req2, timeout=30) as _:
                    ok += 1
            except Exception as e2:
                print(f'  ❌ {row.get("item_id")} [{row.get("tipo")}]: {e2}')
        return ok


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f'\n{SEP}')
    print('DETECTOR DE CAMBIOS MLU — INICIO')
    print(f'Ejecutado: {datetime.now().isoformat()}')
    print(f'{SEP}\n')

    current_run, previous_run = get_last_two_valid_runs()

    if not current_run or not previous_run:
        print('ℹ️  Sin suficientes runs válidos para comparar — saliendo sin error')
        sys.exit(0)

    current_run_id  = current_run['run_id']
    previous_run_id = previous_run['run_id']

    # Evitar duplicados por misma corrida
    if already_detected(current_run_id):
        print(f'⚠️  Ya se detectaron cambios para {current_run_id} — saltando')
        sys.exit(0)

    # Cargar item_ids
    print(f'\n📥 Item IDs del run actual ({current_run_id})...')
    current_items = get_item_ids_for_run(current_run_id)
    print(f'   {len(current_items)} items únicos')

    print(f'\n📥 Item IDs del run anterior ({previous_run_id})...')
    previous_items = get_item_ids_for_run(previous_run_id)
    print(f'   {len(previous_items)} items únicos')

    # Comparar
    print('\n🔍 Comparando...\n')
    cambios = detect_changes(current_items, previous_items, current_run_id)

    # Resumen
    print(f'\n{SEP}')
    print('RESUMEN DE CAMBIOS DETECTADOS')
    print(SEP)
    tipos = {}
    for c in cambios:
        tipos[c['tipo']] = tipos.get(c['tipo'], 0) + 1

    if tipos:
        for tipo in sorted(tipos):
            print(f'  {tipo:35}: {tipos[tipo]:4}')
        print(f'  {"TOTAL":35}: {len(cambios):4}')
    else:
        print('  Sin cambios detectados entre los dos runs')

    print(SEP)

    # Detalle (primeros 30)
    if cambios:
        print('\nDetalle (primeros 30):')
        for c in cambios[:30]:
            print(f'  [{c["tipo"]}] {c["item_id"]} (seller: {c.get("seller_id")})')
        if len(cambios) > 30:
            print(f'  ... y {len(cambios) - 30} más')

    # Guardar
    insertados = guardar_cambios(cambios)

    print(f'\n{SEP}')
    print(f'✅ DETECTOR COMPLETADO — {insertados} cambios guardados')
    print(f'{SEP}\n')


if __name__ == '__main__':
    main()
