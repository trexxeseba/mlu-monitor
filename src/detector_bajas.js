'use strict';

/**
 * detector_bajas.js
 *
 * Compara el último run válido contra el run válido anterior.
 * Solo trabaja con item_ids extraídos del listado — sin dependencia de price,
 * stock ni sold_quantity.
 *
 * Clasificaciones de cambio:
 *   nuevo                   → item aparece en run actual pero no en el anterior
 *                             y nunca fue registrado en bajas_detectadas
 *   desaparecido_no_confirmado → item estaba y ya no aparece
 *   reaparecido             → item vuelve a aparecer tras haber sido registrado
 *                             como desaparecido_no_confirmado
 *
 * Salida:
 *   0 → detector completó (aunque no haya cambios)
 *   1 → error técnico o insuficientes runs válidos
 */

['SUPABASE_URL', 'SUPABASE_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta variable ${k}`); process.exit(1); }
});

const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const SEP = '═'.repeat(70);

// ─── Obtener los 2 últimos runs válidos ────────────────────────────────────────
async function getLastTwoValidRuns() {
  console.log('📊 Consultando últimos 2 runs válidos...');

  // Intento 1: monitor_runs (tabla ideal)
  {
    const { data, error } = await supabase
      .from('monitor_runs')
      .select('*')
      .eq('status', 'valid')
      .order('finished_at', { ascending: false })
      .limit(2);

    if (!error && data && data.length >= 2) {
      console.log(`  [monitor_runs] Run actual:   ${data[0].run_id}`);
      console.log(`  [monitor_runs] Run anterior: ${data[1].run_id}`);
      return [data[0], data[1]];
    }
    if (!error && data && data.length === 1) {
      console.warn('⚠️  Solo 1 run válido en monitor_runs — insuficiente para comparar');
    }
    if (error && !error.message?.includes('schema cache') && !error.message?.includes('does not exist') && error.code !== '42P01') {
      console.error(`❌ Error consultando monitor_runs: ${error.message}`);
      // continuar al fallback igualmente
    }
  }

  // Intento 2: execution_logs (fallback cuando monitor_runs no existe)
  console.log('  ⚠️  Fallback a execution_logs...');
  {
    const { data, error } = await supabase
      .from('execution_logs')
      .select('*')
      .eq('status', 'success')
      .order('executed_at', { ascending: false })
      .limit(2);

    if (!error && data && data.length >= 2) {
      const toRun = row => ({
        run_id:       row.run_id,
        total_items:  row.items_processed,
        finished_at:  row.executed_at,
        _source:      'execution_logs',
      });
      const [current, previous] = [toRun(data[0]), toRun(data[1])];
      console.log(`  [execution_logs] Run actual:   ${current.run_id}`);
      console.log(`  [execution_logs] Run anterior: ${previous.run_id}`);
      return [current, previous];
    }
    if (!error && data && data.length === 1) {
      console.warn('⚠️  Solo 1 run exitoso en execution_logs — insuficiente para comparar');
    }
  }

  // Intento 3: 2 run_ids más recientes de snapshots
  console.log('  Fallback: buscando 2 run_ids distintos en snapshots...');
  {
    const { data, error } = await supabase
      .from('snapshots')
      .select('run_id, checked_at')
      .order('checked_at', { ascending: false })
      .limit(5000);

    if (error) {
      console.error(`❌ No se pudo obtener runs de snapshots: ${error.message}`);
      return [null, null];
    }
    const seen = [];
    for (const row of (data || [])) {
      if (row.run_id && !seen.includes(row.run_id)) seen.push(row.run_id);
      if (seen.length >= 2) break;
    }
    if (seen.length < 2) {
      console.warn(`⚠️  Solo ${seen.length} run_id(s) en snapshots — insuficiente para comparar`);
      return [null, null];
    }
    const toRun = id => ({ run_id: id, total_items: '?', finished_at: id });
    console.log(`  [snapshots fallback] Run actual:   ${seen[0]}`);
    console.log(`  [snapshots fallback] Run anterior: ${seen[1]}`);
    return [toRun(seen[0]), toRun(seen[1])];
  }
}

// ─── Obtener {item_id → seller_id} para un run ────────────────────────────────
async function getItemsForRun(runId) {
  const items = {};
  const pageSize = 1000;
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('snapshots')
      .select('item_id, meli_item_id, seller_id')
      .eq('run_id', runId)
      .range(offset, offset + pageSize - 1);

    if (error) throw new Error(`getItemsForRun(${runId}): ${error.message}`);
    const batch = data || [];
    for (const snap of batch) {
      const iid = snap.item_id || snap.meli_item_id;
      if (iid && !(iid in items)) items[iid] = snap.seller_id;
    }
    if (batch.length < pageSize) break;
    offset += pageSize;
  }
  return items;
}

// ─── Verificar si ya se detectó para este run ─────────────────────────────────
async function alreadyDetected(runId) {
  try {
    const { data } = await supabase
      .from('bajas_detectadas')
      .select('id')
      .eq('run_id', runId)
      .limit(1);
    return (data || []).length > 0;
  } catch (_) {
    return false;
  }
}

// ─── Helper: query in() por lotes para evitar URLs largas ────────────────────
async function queryInBatches(table, select, filters, idField, ids) {
  const CHUNK = 100;
  const results = [];
  for (let i = 0; i < ids.length; i += CHUNK) {
    const chunk = ids.slice(i, i + CHUNK);
    let q = supabase.from(table).select(select).in(idField, chunk);
    for (const [col, val] of Object.entries(filters)) q = q.eq(col, val);
    const { data, error } = await q;
    if (error) throw error;
    results.push(...(data || []));
  }
  return results;
}

// ─── Items que ya fueron registrados en bajas_detectadas (cualquier tipo) ─────
async function getPreviouslyRegistered(itemIds) {
  if (!itemIds.length) return new Set();
  try {
    const data = await queryInBatches('bajas_detectadas', 'item_id', {}, 'item_id', itemIds);
    return new Set(data.map(r => r.item_id));
  } catch (e) {
    console.warn(`  ⚠️  No se pudo consultar registros previos: ${e.message}`);
    return new Set();
  }
}

// ─── Items cuyo ÚLTIMO estado en bajas_detectadas es 'desaparecido_no_confirmado'
// No basta con que alguna vez hayan desaparecido: si ya fueron registrados como
// 'reaparecido' o 'nuevo' en un run posterior, se consideran estables.
async function getPreviouslyDisappeared(itemIds) {
  if (!itemIds.length) return new Set();
  try {
    // Traer item_id, tipo y fecha de todas las entradas para estos items
    const data = await queryInBatches(
      'bajas_detectadas', 'item_id, tipo, fecha_deteccion', {}, 'item_id', itemIds
    );

    // Para cada item, quedarse solo con la entrada más reciente
    const latestByItem = {};
    for (const r of data) {
      if (!latestByItem[r.item_id] || r.fecha_deteccion > latestByItem[r.item_id].fecha_deteccion) {
        latestByItem[r.item_id] = r;
      }
    }

    // Solo items cuyo último estado es 'desaparecido_no_confirmado'
    return new Set(
      Object.values(latestByItem)
        .filter(r => r.tipo === 'desaparecido_no_confirmado')
        .map(r => r.item_id)
    );
  } catch (e) {
    console.warn(`  ⚠️  No se pudo consultar reaparecidos: ${e.message}`);
    return new Set();
  }
}

// ─── Comparar y clasificar cambios ────────────────────────────────────────────
async function detectChanges(currentItems, previousItems, currentRunId) {
  /**
   * Con el diseño UPSERT:
   * - currentItems  = todos los items visibles en el run actual (run_id = current)
   * - previousItems = items con run_id = previous = items que DESAPARECIERON
   *                   (no fueron vistos en el run actual → no se actualizaron)
   *
   * Para "nuevo" y "reaparecido":
   *   - Un item en currentItems que NO está en bajas_detectadas (nunca visto) → nuevo
   *   - Un item en currentItems que fue desaparecido_no_confirmado → reaparecido
   *   - Un item en currentItems ya registrado como "nuevo" en algún ciclo → estable → ignorar
   */
  const cambios = [];
  const now = new Date().toISOString();

  const currentIds  = new Set(Object.keys(currentItems));
  const previousIds = new Set(Object.keys(previousItems));

  // ── DESAPARECIDOS: items con run_id = previous (no actualizados al run actual)
  for (const iid of previousIds) {
    cambios.push({
      tipo:            'desaparecido_no_confirmado',
      item_id:         iid,
      meli_item_id:    iid,
      seller_id:       previousItems[iid],
      run_id:          currentRunId,
      fecha_deteccion: now,
    });
  }

  // ── NUEVOS / REAPARECIDOS: items en current que no están en previous
  //    (con UPSERT, casi todos los current items no están en previous, porque
  //    los que siguen activos se upsertaron al run actual; solo los desaparecidos
  //    quedaron con el run anterior)
  const candidatosNuevos = [...currentIds].filter(iid => !previousIds.has(iid));
  const [registrados, desaparecidos] = await Promise.all([
    getPreviouslyRegistered(candidatosNuevos),
    getPreviouslyDisappeared(candidatosNuevos),
  ]);

  for (const iid of candidatosNuevos) {
    let tipo;
    if (desaparecidos.has(iid)) {
      tipo = 'reaparecido';
    } else if (!registrados.has(iid)) {
      tipo = 'nuevo';
    } else {
      continue; // estable — ya fue registrado, no es nuevo ni reaparecido
    }
    cambios.push({
      tipo,
      item_id:         iid,
      meli_item_id:    iid,
      seller_id:       currentItems[iid],
      run_id:          currentRunId,
      fecha_deteccion: now,
    });
  }

  return cambios;
}

// ─── Guardar cambios en bajas_detectadas ──────────────────────────────────────
async function guardarCambios(cambios) {
  if (!cambios.length) {
    console.log('ℹ️  Sin cambios para guardar');
    return 0;
  }

  console.log(`\n💾 Insertando ${cambios.length} cambios en bajas_detectadas...`);

  const rows = cambios.map(c => ({
    seller_id:       c.seller_id,
    item_id:         c.item_id,
    meli_item_id:    c.meli_item_id,
    tipo:            c.tipo,
    run_id:          c.run_id,
    fecha_deteccion: c.fecha_deteccion,
  }));

  // Bulk insert
  const { error } = await supabase.from('bajas_detectadas').insert(rows);
  if (!error) {
    console.log(`✅ ${rows.length} cambios guardados`);
    return rows.length;
  }

  // Fallback: de a uno
  console.warn(`⚠️  Bulk insert falló (${error.message}), intentando de a uno...`);
  let ok = 0;
  for (const row of rows) {
    const { error: e2 } = await supabase.from('bajas_detectadas').insert([row]);
    if (!e2) ok++;
    else console.warn(`  ❌ ${row.item_id} [${row.tipo}]: ${e2.message}`);
  }
  return ok;
}

// ─── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  console.log(`\n${SEP}`);
  console.log('DETECTOR DE CAMBIOS MLU — INICIO');
  console.log(`Ejecutado: ${new Date().toISOString()}`);
  console.log(`${SEP}\n`);

  const [currentRun, previousRun] = await getLastTwoValidRuns();

  if (!currentRun || !previousRun) {
    console.log('ℹ️  Sin suficientes runs válidos para comparar — saliendo sin error');
    process.exit(0);
  }

  const currentRunId  = currentRun.run_id;
  const previousRunId = previousRun.run_id;

  // Evitar duplicados por mismo run
  if (await alreadyDetected(currentRunId)) {
    console.warn(`⚠️  Ya se detectaron cambios para ${currentRunId} — saltando`);
    process.exit(0);
  }

  // Cargar items
  console.log(`\n📥 Items del run actual (${currentRunId})...`);
  let currentItems;
  try {
    currentItems = await getItemsForRun(currentRunId);
  } catch (e) {
    console.error(`❌ ${e.message}`);
    process.exit(1);
  }
  console.log(`   ${Object.keys(currentItems).length} items únicos`);

  console.log(`\n📥 Items del run anterior (${previousRunId})...`);
  let previousItems;
  try {
    previousItems = await getItemsForRun(previousRunId);
  } catch (e) {
    console.error(`❌ ${e.message}`);
    process.exit(1);
  }
  console.log(`   ${Object.keys(previousItems).length} items únicos`);

  // Comparar
  console.log('\n🔍 Comparando...\n');
  const cambios = await detectChanges(currentItems, previousItems, currentRunId);

  // Resumen
  console.log(`\n${SEP}`);
  console.log('RESUMEN DE CAMBIOS DETECTADOS');
  console.log(SEP);
  const tipos = {};
  for (const c of cambios) tipos[c.tipo] = (tipos[c.tipo] || 0) + 1;

  if (Object.keys(tipos).length) {
    for (const tipo of Object.keys(tipos).sort()) {
      console.log(`  ${tipo.padEnd(35)}: ${String(tipos[tipo]).padStart(4)}`);
    }
    console.log(`  ${'TOTAL'.padEnd(35)}: ${String(cambios.length).padStart(4)}`);
  } else {
    console.log('  Sin cambios detectados entre los dos runs');
  }
  console.log(SEP);

  // Detalle (primeros 30)
  if (cambios.length) {
    console.log('\nDetalle (primeros 30):');
    cambios.slice(0, 30).forEach(c =>
      console.log(`  [${c.tipo}] ${c.item_id} (seller: ${c.seller_id})`)
    );
    if (cambios.length > 30) console.log(`  ... y ${cambios.length - 30} más`);
  }

  // Guardar
  const insertados = await guardarCambios(cambios);

  console.log(`\n${SEP}`);
  console.log(`✅ DETECTOR COMPLETADO — ${insertados} cambios guardados`);
  console.log(`${SEP}\n`);

  process.exit(0);
})();
