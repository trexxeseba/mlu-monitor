"""
detector_bajas.py

Compara el último run válido contra el run válido anterior.
NO mezcla snapshots de corridas distintas ni de runs inválidos.

Clasificaciones de cambio:
  nuevo                   → item aparece en run actual pero no en el anterior
  desaparecido_no_confirmado → item estaba y ya no aparece
  vendido_confirmado      → sold_quantity subió (señal más fuerte)
  vendido_probable        → available_quantity bajó y el item sigue activo
  status_cambio           → cambió el campo status
  precio_cambio           → cambió el precio más de un 5%
  stock_cambio            → cambió available_quantity sin venta confirmada

Salida:
  0 → detector completó (aunque no haya cambios)
  1 → error técnico o insuficientes runs válidos
"""

import os
import sys
from datetime import datetime
from supabase import create_client

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('❌ FATAL: Faltan SUPABASE_URL o SUPABASE_KEY')
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SEP = '═' * 70
PRECIO_MIN_PCT = 5.0      # mínimo % de cambio de precio para reportar


# ─── Obtener los 2 últimos runs válidos ────────────────────────────────────────
def get_last_two_valid_runs():
    print('📊 Consultando últimos 2 runs válidos en monitor_runs...')
    resp = (
        supabase.table('monitor_runs')
        .select('*')
        .eq('status', 'valid')
        .order('finished_at', desc=True)
        .limit(2)
        .execute()
    )
    data = resp.data or []
    if len(data) < 2:
        print(f'⚠️  Solo {len(data)} run(s) válido(s). Se necesitan al menos 2 para comparar.')
        return None, None
    current  = data[0]   # el más reciente
    previous = data[1]   # el inmediatamente anterior válido
    print(f'  Run actual:   {current["run_id"]}  ({current["total_items"]} items,'
          f' {current["finished_at"][:19]})')
    print(f'  Run anterior: {previous["run_id"]} ({previous["total_items"]} items,'
          f' {previous["finished_at"][:19]})')
    return current, previous


# ─── Obtener todos los snapshots de un run (indexados por item_id) ─────────────
def get_snapshots_for_run(run_id):
    resp = (
        supabase.table('snapshots')
        .select('*')
        .eq('run_id', run_id)
        .execute()
    )
    data = resp.data or []
    by_item = {}
    for snap in data:
        iid = snap.get('item_id') or snap.get('meli_item_id')
        if iid and iid not in by_item:
            by_item[iid] = snap
    return by_item


# ─── Verificar si ya se detectó para este run (evitar duplicados) ──────────────
def already_detected(current_run_id):
    try:
        resp = (
            supabase.table('bajas_detectadas')
            .select('id')
            .eq('detection_run_id', current_run_id)
            .limit(1)
            .execute()
        )
        return len(resp.data or []) > 0
    except Exception:
        # Si la columna no existe todavía, proceder igual
        return False


# ─── Comparar dos sets de snapshots y clasificar cambios ──────────────────────
def detect_changes(current_snaps, previous_snaps, current_run_id):
    cambios = []
    now = datetime.now().isoformat()

    current_ids  = set(current_snaps.keys())
    previous_ids = set(previous_snaps.keys())

    # ── NUEVOS ────────────────────────────────────────────────────────────────
    for iid in current_ids - previous_ids:
        snap = current_snaps[iid]
        cambios.append({
            'tipo':             'nuevo',
            'item_id':          iid,
            'meli_item_id':     snap.get('meli_item_id', iid),
            'seller_id':        snap.get('seller_id'),
            'title':            snap.get('title'),
            'precio_nuevo':     snap.get('price'),
            'status_nuevo':     snap.get('status'),
            'detection_run_id': current_run_id,
            'fecha_deteccion':  now,
        })

    # ── DESAPARECIDOS ─────────────────────────────────────────────────────────
    for iid in previous_ids - current_ids:
        snap = previous_snaps[iid]
        cambios.append({
            'tipo':             'desaparecido_no_confirmado',
            'item_id':          iid,
            'meli_item_id':     snap.get('meli_item_id', iid),
            'seller_id':        snap.get('seller_id'),
            'title':            snap.get('title'),
            'precio_anterior':  snap.get('price'),
            'status_anterior':  snap.get('status'),
            'detection_run_id': current_run_id,
            'fecha_deteccion':  now,
        })

    # ── ITEMS EN AMBOS RUNS ───────────────────────────────────────────────────
    for iid in current_ids & previous_ids:
        curr = current_snaps[iid]
        prev = previous_snaps[iid]

        sold_curr  = curr.get('sold_quantity')  or 0
        sold_prev  = prev.get('sold_quantity')  or 0
        avail_curr = curr.get('available_quantity') or 0
        avail_prev = prev.get('available_quantity') or 0
        price_curr = curr.get('price') or 0
        price_prev = prev.get('price') or 0
        status_curr = curr.get('status') or ''
        status_prev = prev.get('status') or ''

        base = {
            'item_id':          iid,
            'meli_item_id':     curr.get('meli_item_id', iid),
            'seller_id':        curr.get('seller_id'),
            'title':            curr.get('title'),
            'precio_anterior':  price_prev,
            'precio_nuevo':     price_curr,
            'detection_run_id': current_run_id,
            'fecha_deteccion':  now,
        }

        # 1. vendido_confirmado: sold_quantity subió (señal más fuerte, cortar análisis)
        if sold_curr > sold_prev and sold_curr > 0:
            cambios.append({**base,
                'tipo':             'vendido_confirmado',
                'unidades_vendidas': sold_curr - sold_prev,
                'stock_anterior':   avail_prev,
                'stock_nuevo':      avail_curr,
            })
            continue  # señal más fuerte para este item

        # 2. vendido_probable: available_quantity bajó y el item sigue activo
        vendido_probable = avail_curr < avail_prev and status_curr == 'active'
        if vendido_probable:
            cambios.append({**base,
                'tipo':           'vendido_probable',
                'stock_anterior': avail_prev,
                'stock_nuevo':    avail_curr,
            })
            # no cortamos: podría haber también status o precio cambiado

        # 3. status_cambio
        if status_curr != status_prev:
            cambios.append({**base,
                'tipo':            'status_cambio',
                'status_anterior': status_prev,
                'status_nuevo':    status_curr,
            })

        # 4. precio_cambio (>= 5%)
        if price_prev > 0 and price_curr > 0:
            pct = (price_curr - price_prev) / price_prev * 100
            if abs(pct) >= PRECIO_MIN_PCT:
                cambios.append({**base,
                    'tipo':              'precio_cambio',
                    'cambio_porcentaje': round(pct, 2),
                })

        # 5. stock_cambio (sin venta confirmada ni probable)
        elif avail_curr != avail_prev and not vendido_probable:
            cambios.append({**base,
                'tipo':           'stock_cambio',
                'stock_anterior': avail_prev,
                'stock_nuevo':    avail_curr,
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
            'seller_id':        c.get('seller_id'),
            'item_id':          c.get('item_id'),
            'meli_item_id':     c.get('meli_item_id'),
            'title':            c.get('title'),
            'tipo':             c.get('tipo'),
            'precio_anterior':  c.get('precio_anterior'),
            'precio_nuevo':     c.get('precio_nuevo'),
            'cambio_porcentaje':c.get('cambio_porcentaje'),
            'status_anterior':  c.get('status_anterior'),
            'status_nuevo':     c.get('status_nuevo'),
            'unidades_vendidas':c.get('unidades_vendidas'),
            'stock_anterior':   c.get('stock_anterior'),
            'stock_nuevo':      c.get('stock_nuevo'),
            'detection_run_id': c.get('detection_run_id'),
            'fecha_deteccion':  c.get('fecha_deteccion'),
        })

    try:
        supabase.table('bajas_detectadas').insert(rows).execute()
        print(f'✅ {len(rows)} cambios guardados')
        return len(rows)
    except Exception as e:
        print(f'⚠️  Bulk insert falló ({e}), intentando de a uno...')
        ok = 0
        for row in rows:
            try:
                supabase.table('bajas_detectadas').insert([row]).execute()
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

    # Cargar snapshots
    print(f'\n📥 Snapshots del run actual ({current_run_id})...')
    current_snaps = get_snapshots_for_run(current_run_id)
    print(f'   {len(current_snaps)} items únicos')

    print(f'\n📥 Snapshots del run anterior ({previous_run_id})...')
    previous_snaps = get_snapshots_for_run(previous_run_id)
    print(f'   {len(previous_snaps)} items únicos')

    # Comparar
    print('\n🔍 Comparando...\n')
    cambios = detect_changes(current_snaps, previous_snaps, current_run_id)

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

    # Detalle
    if cambios:
        print('\nDetalle:')
        for c in cambios:
            title = (c.get('title') or '')[:45]
            extra = ''
            if c['tipo'] == 'vendido_confirmado':
                extra = f' | +{c.get("unidades_vendidas", "?")} unid'
            elif c['tipo'] == 'precio_cambio':
                extra = f' | {c.get("cambio_porcentaje", 0):+.1f}%'
            elif c['tipo'] in ('vendido_probable', 'stock_cambio'):
                extra = f' | stock {c.get("stock_anterior")}→{c.get("stock_nuevo")}'
            elif c['tipo'] in ('status_cambio',):
                extra = f' | {c.get("status_anterior")}→{c.get("status_nuevo")}'
            print(f'  [{c["tipo"]}] {title}{extra}')

    # Guardar
    insertados = guardar_cambios(cambios)

    print(f'\n{SEP}')
    print(f'✅ DETECTOR COMPLETADO — {insertados} cambios guardados')
    print(f'{SEP}\n')


if __name__ == '__main__':
    main()
