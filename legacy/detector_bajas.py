import os
import json
from datetime import datetime
from supabase import create_client

# ─── Configuración ─────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ FATAL: Faltan SUPABASE_URL o SUPABASE_KEY")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("\n" + "═"*70)
print("🔍 DETECTOR DE BAJAS - MEJORADO")
print("═"*70 + "\n")

# ─── Obtener últimos 2 snapshots de cada item ──────────────────────────────
def get_snapshots_for_comparison():
    """
    Obtiene últimos 2 snapshots de cada item para comparar
    """
    print("📊 Obteniendo snapshots para comparación...")
    
    response = supabase.table('snapshots').select('*').order('checked_at', ascending=False).limit(2000).execute()
    
    if not response.data:
        print("⚠️  No hay snapshots para procesar")
        return {}
    
    # Agrupar por item_id y obtener últimos 2
    items_snapshots = {}
    for snapshot in response.data:
        item_id = snapshot['item_id']
        if item_id not in items_snapshots:
            items_snapshots[item_id] = []
        if len(items_snapshots[item_id]) < 2:
            items_snapshots[item_id].append(snapshot)
    
    print(f"✅ {len(items_snapshots)} items con múltiples snapshots\n")
    return items_snapshots

# ─── Detectar cambios ──────────────────────────────────────────────────────
def es_falso_positivo(cambio, snapshot_anterior, snapshot_nuevo):
    """
    Filtra cambios que probablemente NO son reales
    """
    
    # Falso positivo: cambio de URL (edición del vendedor)
    if snapshot_anterior and snapshot_nuevo:
        if snapshot_anterior.get('url') != snapshot_nuevo.get('url'):
            return True
    
    # Falso positivo: status = paused (vendedor pausó, no vendió)
    if snapshot_nuevo and snapshot_nuevo.get('status') == 'paused':
        return True
    
    # Falso positivo: cambio de precio < 5% (ruido)
    if cambio['tipo'] == 'precio_cambio':
        pct = cambio.get('cambio_porcentaje', 0)
        if abs(pct) < 5:
            return True
    
    # Falso positivo: sold_quantity = 0 (Mercado Libre no lo expone)
    if cambio['tipo'] == 'vendido':
        if snapshot_nuevo.get('sold_quantity', 0) == 0:
            return True
    
    return False

def detectar_cambios(snapshot_anterior, snapshot_nuevo):
    """
    Detecta 3 tipos de cambios: NUEVO, DESAPARECIDO, PRECIO_CAMBIO, VENDIDO
    """
    cambios = []
    
    # TIPO 1: ITEM NUEVO
    if snapshot_anterior is None and snapshot_nuevo:
        cambios.append({
            'tipo': 'nuevo',
            'item_id': snapshot_nuevo['item_id'],
            'meli_item_id': snapshot_nuevo['meli_item_id'],
            'seller_id': snapshot_nuevo['seller_id'],
            'title': snapshot_nuevo['title'],
            'precio': snapshot_nuevo['price'],
            'status_nuevo': snapshot_nuevo.get('status'),
            'timestamp': datetime.now().isoformat()
        })
    
    # TIPO 2: ITEM DESAPARECIDO
    if snapshot_anterior and snapshot_nuevo is None:
        cambios.append({
            'tipo': 'desaparecido',  # podría ser venta, pausado, o eliminado
            'item_id': snapshot_anterior['item_id'],
            'meli_item_id': snapshot_anterior['meli_item_id'],
            'seller_id': snapshot_anterior['seller_id'],
            'title': snapshot_anterior['title'],
            'precio_ultimo': snapshot_anterior['price'],
            'status_anterior': snapshot_anterior.get('status'),
            'timestamp': datetime.now().isoformat()
        })
    
    # TIPO 3: CAMBIO DE PRECIO
    if snapshot_anterior and snapshot_nuevo:
        precio_ant = snapshot_anterior.get('price', 0)
        precio_nuevo = snapshot_nuevo.get('price', 0)
        
        if precio_ant > 0 and abs(precio_nuevo - precio_ant) > 100:
            cambio_pct = ((precio_nuevo / precio_ant) - 1) * 100
            cambios.append({
                'tipo': 'precio_cambio',
                'item_id': snapshot_nuevo['item_id'],
                'meli_item_id': snapshot_nuevo['meli_item_id'],
                'seller_id': snapshot_nuevo['seller_id'],
                'title': snapshot_nuevo['title'],
                'precio_anterior': precio_ant,
                'precio_nuevo': precio_nuevo,
                'cambio_porcentaje': round(cambio_pct, 2),
                'timestamp': datetime.now().isoformat()
            })
    
    # TIPO 4: ITEM VENDIDO
    if snapshot_anterior and snapshot_nuevo:
        sold_ant = snapshot_anterior.get('sold_quantity', 0)
        sold_nuevo = snapshot_nuevo.get('sold_quantity', 0)
        
        if sold_nuevo > sold_ant and sold_nuevo > 0:
            cambios.append({
                'tipo': 'vendido',
                'item_id': snapshot_nuevo['item_id'],
                'meli_item_id': snapshot_nuevo['meli_item_id'],
                'seller_id': snapshot_nuevo['seller_id'],
                'title': snapshot_nuevo['title'],
                'unidades_vendidas': sold_nuevo - sold_ant,
                'precio': snapshot_nuevo['price'],
                'timestamp': datetime.now().isoformat()
            })
    
    # Filtrar falsos positivos
    cambios_validos = []
    for cambio in cambios:
        if not es_falso_positivo(cambio, snapshot_anterior, snapshot_nuevo):
            cambios_validos.append(cambio)
        else:
            print(f"  🚫 Falso positivo filtrado: {cambio['tipo']} - {cambio['title'][:40]}")
    
    return cambios_validos

# ─── Guardar cambios en bajas_detectadas ───────────────────────────────────
def guardar_cambios(cambios):
    """
    Guarda cambios en tabla bajas_detectadas
    """
    if not cambios:
        print("⚠️  No hay cambios válidos para guardar")
        return 0
    
    print(f"\n💾 Guardando {len(cambios)} cambios...")
    
    rows_a_insertar = []
    for cambio in cambios:
        row = {
            'seller_id': cambio['seller_id'],
            'item_id': cambio['item_id'],
            'meli_item_id': cambio.get('meli_item_id'),
            'title': cambio['title'],
            'tipo': cambio['tipo'],
            'precio_anterior': cambio.get('precio_anterior'),
            'precio_nuevo': cambio.get('precio_nuevo'),
            'cambio_porcentaje': cambio.get('cambio_porcentaje'),
            'status_anterior': cambio.get('status_anterior'),
            'status_nuevo': cambio.get('status_nuevo'),
            'unidades_vendidas': cambio.get('unidades_vendidas'),
            'fecha_deteccion': cambio['timestamp'],
        }
        rows_a_insertar.append(row)
    
    # Intentar insertar
    try:
        response = supabase.table('bajas_detectadas').insert(rows_a_insertar).execute()
        print(f"✅ {len(rows_a_insertar)} cambios guardados")
        return len(rows_a_insertar)
    except Exception as e:
        print(f"❌ Error insertando: {str(e)}")
        return 0

# ─── Main ──────────────────────────────────────────────────────────────────
def main():
    items_snapshots = get_snapshots_for_comparison()
    
    if not items_snapshots:
        print("❌ No hay datos para procesar")
        return
    
    cambios_totales = []
    cambios_por_tipo = {}
    
    for item_id, snapshots in items_snapshots.items():
        if len(snapshots) >= 2:
            # Comparar último vs penúltimo
            snapshot_nuevo = snapshots[0]
            snapshot_anterior = snapshots[1]
            
            cambios = detectar_cambios(snapshot_anterior, snapshot_nuevo)
            
            for cambio in cambios:
                cambios_totales.append(cambio)
                tipo = cambio['tipo']
                cambios_por_tipo[tipo] = cambios_por_tipo.get(tipo, 0) + 1
                print(f"  ✓ {tipo}: {cambio['title'][:40]}")
    
    # Guardar todos los cambios
    print(f"\n{'═'*70}")
    print("📈 RESUMEN DE CAMBIOS DETECTADOS")
    print(f"{'═'*70}\n")
    
    for tipo, count in cambios_por_tipo.items():
        print(f"  {tipo:20}: {count:3} cambios")
    
    print(f"\n  TOTAL: {len(cambios_totales)} cambios\n")
    
    insertados = guardar_cambios(cambios_totales)
    
    print(f"\n{'═'*70}")
    if insertados > 0:
        print(f"✅ DETECTOR COMPLETADO: {insertados} cambios guardados")
    else:
        print(f"⚠️  DETECTOR COMPLETADO: Sin cambios nuevos")
    print(f"{'═'*70}\n")

if __name__ == '__main__':
    main()
