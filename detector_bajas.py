#!/usr/bin/env python3
"""
Detector de Bajas - Detecta cambios en publicaciones entre snapshots
Genera: bajas_detectadas.csv, reaparecidos.csv, reporte.html, resumen.txt
"""

import csv
import os
import sys
from datetime import datetime
from pathlib import Path

# Directorios
OUTPUT_DIR = "output"
SNAPSHOT_ANTERIOR = f"{OUTPUT_DIR}/snapshot_anterior.csv"
SNAPSHOT_ACTUAL = f"{OUTPUT_DIR}/snapshot_actual.csv"
BAJAS_DETECTADAS = f"{OUTPUT_DIR}/bajas_detectadas.csv"
REAPARICIONES = f"{OUTPUT_DIR}/reapariciones.csv"
REPORTE_HTML = f"{OUTPUT_DIR}/reporte.html"
RESUMEN_TXT = f"{OUTPUT_DIR}/resumen.txt"

# Columnas
SNAPSHOT_COLS = ["seller_id", "meli_item_id", "title", "price", "status", "timestamp"]
BAJAS_COLS = ["detected_at", "seller_id", "meli_item_id", "title_last_seen", "price_last_seen", "event_type", "note"]
REAPARICIONES_COLS = ["detected_at", "seller_id", "meli_item_id", "title", "price", "event_type", "note"]


def crear_output_dir():
    """Crea directorio output si no existe"""
    Path(OUTPUT_DIR).mkdir(exist_ok=True)


def cargar_csv(ruta):
    """Carga CSV y retorna dict {meli_item_id: row}"""
    if not os.path.exists(ruta):
        return {}
    
    items = {}
    try:
        with open(ruta, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row and row.get('meli_item_id'):
                    items[row['meli_item_id']] = row
    except Exception as e:
        print(f"⚠️  Error leyendo {ruta}: {e}")
    
    return items


def guardar_csv(ruta, datos, columnas):
    """Guarda datos a CSV (append si existe, crear si no)"""
    existe = os.path.exists(ruta)
    
    try:
        with open(ruta, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columnas)
            if not existe:
                writer.writeheader()
            for row in datos:
                writer.writerow(row)
    except Exception as e:
        print(f"❌ Error guardando {ruta}: {e}")
        raise


def detectar_bajas():
    """Compara snapshots y registra bajas"""
    
    print("\n🔍 Comparando snapshots...")
    
    anterior = cargar_csv(SNAPSHOT_ANTERIOR)
    actual = cargar_csv(SNAPSHOT_ACTUAL)
    
    print(f"  Anterior: {len(anterior)} items")
    print(f"  Actual: {len(actual)} items")
    
    bajas = []
    reapariciones = []
    
    ahora = datetime.now().isoformat()
    
    # Detectar bajas (estaban antes, ahora no)
    for item_id, row_anterior in anterior.items():
        if item_id not in actual:
            baja = {
                "detected_at": ahora,
                "seller_id": row_anterior.get('seller_id', ''),
                "meli_item_id": item_id,
                "title_last_seen": row_anterior.get('title', ''),
                "price_last_seen": row_anterior.get('price', ''),
                "event_type": "baja_detectada",
                "note": ""
            }
            bajas.append(baja)
            print(f"  ⬇️  BAJA: {item_id} - {row_anterior.get('title')} (${row_anterior.get('price')})")
    
    # Detectar reapariciones (estaban en bajas, ahora reaparecen)
    if os.path.exists(BAJAS_DETECTADAS):
        bajas_hist = cargar_csv(BAJAS_DETECTADAS)
        for item_id, row_actual in actual.items():
            if item_id in bajas_hist:
                reaparicion = {
                    "detected_at": ahora,
                    "seller_id": row_actual.get('seller_id', ''),
                    "meli_item_id": item_id,
                    "title": row_actual.get('title', ''),
                    "price": row_actual.get('price', ''),
                    "event_type": "reaparecio",
                    "note": ""
                }
                reapariciones.append(reaparicion)
                print(f"  ⬆️  REAPARICIÓN: {item_id} - {row_actual.get('title')}")
    
    # Guardar resultados
    if bajas:
        guardar_csv(BAJAS_DETECTADAS, bajas, BAJAS_COLS)
        print(f"\n✓ Registradas {len(bajas)} baja(s) en {BAJAS_DETECTADAS}")
    
    if reapariciones:
        guardar_csv(REAPARICIONES, reapariciones, REAPARICIONES_COLS)
        print(f"✓ Registradas {len(reapariciones)} reaparición(es) en {REAPARICIONES}")
    
    if not bajas and not reapariciones:
        print("\n✓ Sin cambios")
    
    return bajas, reapariciones, anterior, actual


def rotar_snapshots():
    """Renombra snapshot_actual → snapshot_anterior para próximo chequeo"""
    print("\n🔄 Rotando snapshots para próximo chequeo...")
    
    if os.path.exists(SNAPSHOT_ACTUAL):
        try:
            os.replace(SNAPSHOT_ACTUAL, SNAPSHOT_ANTERIOR)
            print(f"  ✓ {SNAPSHOT_ACTUAL} → {SNAPSHOT_ANTERIOR}")
        except Exception as e:
            print(f"  ❌ Error rotando: {e}")
            raise
    else:
        print(f"  ⚠️  {SNAPSHOT_ACTUAL} no existe (primer chequeo?)")


def generar_reporte_html(bajas, reapariciones):
    """Genera reporte HTML"""
    print(f"\n📄 Generando {REPORTE_HTML}...")
    
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    filas_bajas = "".join([
        f"<tr><td>{b['detected_at']}</td><td>{b['seller_id']}</td><td>{b['meli_item_id']}</td>"
        f"<td>{b['title_last_seen']}</td><td>${b['price_last_seen']}</td></tr>"
        for b in bajas
    ])
    
    filas_reapariciones = "".join([
        f"<tr><td>{r['detected_at']}</td><td>{r['seller_id']}</td><td>{r['meli_item_id']}</td>"
        f"<td>{r['title']}</td><td>${r['price']}</td></tr>"
        for r in reapariciones
    ])
    
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MLU Monitor - Bajas Detectadas</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #0066cc; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #0066cc; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f9f9f9; }}
        .summary {{ background: #f0f0f0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .stat {{ display: inline-block; margin-right: 30px; font-weight: bold; }}
        .stat-num {{ color: #0066cc; font-size: 24px; }}
        .alert {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin-bottom: 10px; }}
        .empty {{ color: #999; text-align: center; padding: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🎯 MLU Monitor - Detector de Bajas</h1>
        <p>Generado: <strong>{ahora}</strong></p>
        
        <div class="summary">
            <div class="stat">Bajas Detectadas: <span class="stat-num">{len(bajas)}</span></div>
            <div class="stat">Reapariciones: <span class="stat-num">{len(reapariciones)}</span></div>
        </div>
        
        <h2>⬇️ Bajas Detectadas</h2>
        {f'<table><tr><th>Fecha</th><th>Seller ID</th><th>Item ID</th><th>Título</th><th>Precio</th></tr>{filas_bajas}</table>' if bajas else '<div class="empty">Sin bajas detectadas</div>'}
        
        <h2>⬆️ Reapariciones</h2>
        {f'<table><tr><th>Fecha</th><th>Seller ID</th><th>Item ID</th><th>Título</th><th>Precio</th></tr>{filas_reapariciones}</table>' if reapariciones else '<div class="empty">Sin reapariciones</div>'}
    </div>
</body>
</html>"""
    
    with open(REPORTE_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"  ✓ {REPORTE_HTML} generado")


def generar_resumen_txt(bajas, reapariciones):
    """Genera resumen en texto"""
    print(f"\n📝 Generando {RESUMEN_TXT}...")
    
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    resumen = f"""MLU Monitor - Resumen de Ejecución
Generado: {ahora}

=== ESTADÍSTICAS ===
Bajas detectadas: {len(bajas)}
Reapariciones: {len(reapariciones)}

"""
    
    if bajas:
        resumen += "=== BAJAS DETECTADAS ===\n"
        for b in bajas:
            resumen += f"  • {b['meli_item_id']} - {b['title_last_seen']} (${b['price_last_seen']})\n"
        resumen += "\n"
    
    if reapariciones:
        resumen += "=== REAPARICIONES ===\n"
        for r in reapariciones:
            resumen += f"  • {r['meli_item_id']} - {r['title']} (${r['price']})\n"
        resumen += "\n"
    
    resumen += "Próximo chequeo: dentro de 2 horas\n"
    
    with open(RESUMEN_TXT, 'w', encoding='utf-8') as f:
        f.write(resumen)
    
    print(f"  ✓ {RESUMEN_TXT} generado")


def main():
    print("🎯 Detector de Bajas - MLU Monitor")
    print("=" * 80)
    
    crear_output_dir()
    
    # Detectar cambios
    bajas, reapariciones, anterior, actual = detectar_bajas()
    
    # Rotar para próximo chequeo
    rotar_snapshots()
    
    # Generar reportes
    generar_reporte_html(bajas, reapariciones)
    generar_resumen_txt(bajas, reapariciones)
    
    print("\n" + "=" * 80)
    print(f"✅ Chequeo completado | Bajas: {len(bajas)} | Reapariciones: {len(reapariciones)}")
    print(f"📁 Resultados en: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
