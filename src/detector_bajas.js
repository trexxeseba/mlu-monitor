'use strict';

const fs = require('fs');

/**
 * detector_bajas.js — v5, mínimo y auditable
 *
 * Por cada seller activo, de forma independiente:
 *   1. Encuentra los 2 run_ids más recientes con datos en snapshots
 *   2. Construye sets de item_ids para cada run
 *   3. Calcula desaparecidos (set_anterior por diseño UPSERT)
 *   4. Loguea todo lo suficiente para verificar a mano
 *   5. Graba en bajas_detectadas (con chequeo de idempotencia por seller)
 *
 * NOTA sobre el diseño UPSERT de snapshots:
 *   Cada item tiene UNA fila con el run_id del último scrape donde apareció.
 *   Por tanto, para seller X:
 *     set_actual   = WHERE seller_id=X AND run_id=run_actual   → activos ahora
 *     set_anterior = WHERE seller_id=X AND run_id=run_anterior → desaparecidos
 *   Los sets son DISJUNTOS por construcción — no hace falta restar.
 *
 * Detección de "nuevos" desactivada en esta fase.
 *
 * Salida:
 *   0 → completó (aunque sin cambios)
 *   1 → error técnico
 */

['SUPABASE_URL', 'SUPABASE_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta variable ${k}`); process.exit(1); }
});

const https    = require('https');
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const OXYLABS_USER = process.env.OXYLABS_USER || '';
const OXYLABS_PASS = process.env.OXYLABS_PASS || '';

// ─── Enriquecer items: título + precio via Oxylabs → ML API ──────────────────
async function enrichItems(itemIds) {
  const details = {};
  if (!itemIds.length || !OXYLABS_USER) return details;

  const CHUNK = 20;
  for (let i = 0; i < itemIds.length; i += CHUNK) {
    const chunk   = itemIds.slice(i, i + CHUNK);
    const mlUrl   = `https://api.mercadolibre.com/items?ids=${chunk.join(',')}`;
    const payload = JSON.stringify({ source: 'universal', url: mlUrl });
    const auth    = Buffer.from(`${OXYLABS_USER}:${OXYLABS_PASS}`).toString('base64');

    try {
      const raw = await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: 'realtime.oxylabs.io', path: '/v1/queries', method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload), 'Authorization': 'Basic ' + auth },
        }, res => { const c = []; res.on('data', x => c.push(x)); res.on('end', () => resolve(Buffer.concat(c).toString())); });
        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout')); });
        req.write(payload); req.end();
      });

      const oxy  = JSON.parse(raw);
      let content = oxy.results?.[0]?.content;
      if (typeof content === 'string') content = JSON.parse(content);
      if (!Array.isArray(content)) continue;

      for (const row of content) {
        const b = row.body || row;
        if (b?.id) {
          details[b.id] = {
            title:     b.title     || null,
            price:     b.price     ?? null,
            currency:  b.currency_id || 'UYU',
            thumbnail: b.thumbnail || b.pictures?.[0]?.url || null,
          };
        }
      }
    } catch (e) {
      console.warn(`  ⚠️  enrichItems chunk error: ${e.message}`);
    }
  }
  console.log(`  🔍 enriquecidos: ${Object.keys(details).length}/${itemIds.length}`);
  return details;
}

const SEP  = '═'.repeat(70);
const SEP2 = '─'.repeat(70);

// ─── Sellers activos ──────────────────────────────────────────────────────────
async function getActiveSellers() {
  const { data, error } = await supabase
    .from('sellers')
    .select('seller_id, nombre_real, nickname')
    .eq('activo', true);
  if (error) throw new Error(`getActiveSellers: ${error.message}`);
  return data || [];
}

// ─── 2 run_ids válidos más recientes para un seller ──────────────────────────
// Usa execution_logs (o monitor_runs) para obtener los run_ids en orden
// cronológico correcto — no confiamos en checked_at de snapshots porque el
// UPSERT actualiza checked_at en cada run, rompiendo el orden.
async function getLastTwoRunIds(sellerId) {
  // Intentar execution_logs primero (siempre existe)
  const { data: logData, error: logError } = await supabase
    .from('execution_logs')
    .select('run_id, executed_at')
    .eq('status', 'success')
    .order('executed_at', { ascending: false })
    .limit(20);

  let orderedRunIds = [];
  if (!logError && logData?.length) {
    orderedRunIds = logData.map(r => r.run_id).filter(Boolean);
  } else {
    // Fallback: monitor_runs
    const { data: mrData, error: mrError } = await supabase
      .from('monitor_runs')
      .select('run_id, finished_at')
      .in('status', ['valid'])
      .order('finished_at', { ascending: false })
      .limit(20);
    if (!mrError && mrData?.length) {
      orderedRunIds = mrData.map(r => r.run_id).filter(Boolean);
    }
  }

  // De los run_ids globales ordenados, buscar cuáles tienen datos para este seller
  const ts = {};
  const seen = [];

  for (const runId of orderedRunIds) {
    if (seen.length >= 2) break;
    // Verificar si hay snapshots de este seller en este run
    const { data: snap, error: snapErr } = await supabase
      .from('snapshots')
      .select('run_id, checked_at')
      .eq('seller_id', String(sellerId))
      .eq('run_id', runId)
      .limit(1);
    if (!snapErr && snap?.length) {
      seen.push(runId);
      ts[runId] = snap[0].checked_at;
    }
  }

  // Si no hay suficientes runs en logs, fallback a distinct run_ids en snapshots
  // ordenados por el run_id (que contiene timestamp: run_XXXXXXX)
  if (seen.length < 2) {
    const { data: snapData, error: snapError } = await supabase
      .from('snapshots')
      .select('run_id, checked_at')
      .eq('seller_id', String(sellerId))
      .order('checked_at', { ascending: false })
      .limit(2000);

    if (!snapError) {
      for (const row of (snapData || [])) {
        if (row.run_id && !seen.includes(row.run_id)) {
          seen.push(row.run_id);
          ts[row.run_id] = row.checked_at;
        }
        if (seen.length >= 2) break;
      }
    }
  }

  return { runIds: seen, ts };
}

// ─── Items de un seller en un run concreto (paginado, deduplicado) ────────────
async function getItemSet(sellerId, runId) {
  const items    = new Set();
  const pageSize = 1000;
  let   offset   = 0;

  while (true) {
    const { data, error } = await supabase
      .from('snapshots')
      .select('item_id, meli_item_id')
      .eq('seller_id', String(sellerId))
      .eq('run_id', runId)
      .range(offset, offset + pageSize - 1);

    if (error) throw new Error(`getItemSet(${sellerId}, ${runId}): ${error.message}`);
    const batch = data || [];
    for (const row of batch) {
      const id = row.item_id || row.meli_item_id;
      if (id) items.add(id);
    }
    if (batch.length < pageSize) break;
    offset += pageSize;
  }
  return items;
}

// ─── Idempotencia: ¿ya corrimos detección para este par (seller, run_anterior)? ──
// Chequeamos por run_anterior en bajas_detectadas para evitar doble-guardado si
// el detector corre dos veces en el mismo día (workflow_dispatch manual, etc.).
// Ya NO muestreamos items individuales — eso causaba falsos positivos porque
// items que desaparecieron hace semanas bloqueaban nuevas detecciones.
async function alreadyDetectedSeller(sellerId, runAnterior) {
  const { data, error } = await supabase
    .from('bajas_detectadas')
    .select('item_id')
    .eq('seller_id', String(sellerId))
    .eq('tipo', 'desaparecido_no_confirmado')
    // El run_id de bajas_detectadas es el det_run_id del detector, no el run del scraper.
    // Usamos fecha_deteccion del día de hoy como proxy de idempotencia.
    .gte('fecha_deteccion', new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString()) // últimas 6h
    .limit(1);
  if (error) return false; // si falla la consulta, dejamos pasar (mejor duplicar que perder)
  return (data?.length ?? 0) > 0;
}

// ─── Procesar un seller ────────────────────────────────────────────────────────
async function processSeller(seller) {
  const sellerId = String(seller.seller_id);
  const name     = seller.nombre_real || seller.nickname || sellerId;

  console.log(`\n${SEP2}`);
  console.log(`Seller: ${name}  (id: ${sellerId})`);

  // 1. Runs
  const { runIds, ts } = await getLastTwoRunIds(sellerId);

  if (runIds.length < 2) {
    console.log(`  ⏭️  Solo ${runIds.length} run(s) — se necesitan 2 para comparar. Salteando.`);
    return null;
  }

  const [runActual, runAnterior] = runIds;
  console.log(`  run_actual:   ${runActual}  (${(ts[runActual]  || '').slice(0, 19)})`);
  console.log(`  run_anterior: ${runAnterior}  (${(ts[runAnterior] || '').slice(0, 19)})`);

  // 2. Sets de items
  const setActual   = await getItemSet(sellerId, runActual);
  const setAnterior = await getItemSet(sellerId, runAnterior);

  console.log(`  items run_actual:   ${setActual.size}`);
  console.log(`  items run_anterior: ${setAnterior.size}  ← estos son los desaparecidos (UPSERT)`);

  // 3. Desaparecidos (BAJADAS) y Nuevos (SUBIDAS)
  // Con el UPSERT corregido por (seller_id, meli_item_id):
  //   setAnterior = items cuyo run_id es el anterior → desaparecieron en el run actual
  //   setActual   = items cuyo run_id es el actual   → son nuevos (no estaban en anterior)
  // Los sets son DISJUNTOS por construcción UPSERT — cada item tiene solo un run_id.
  const desaparecidos = [...setAnterior]; // items en run anterior pero no en actual
  const nuevos        = [...setActual].filter(id => !setAnterior.has(id)); // items solo en actual

  // Nota: con UPSERT, setActual contiene SOLO items nuevos de este run
  // (los que ya existían fueron actualizados y ya no aparecen en setAnterior).
  // Por lo tanto setActual y setAnterior son naturalmente disjuntos → nuevos = setActual.
  // El filter(...has) es solo seguridad por si se corre sin migración.

  console.log(`  desaparecidos: ${desaparecidos.length}  nuevos: ${nuevos.length}`);

  // 4. Muestra auditable
  if (desaparecidos.length > 0) {
    const muestra = desaparecidos.slice(0, 8).join(', ');
    console.log(`  muestra desaparecidos: ${muestra}${desaparecidos.length > 8 ? ` ... (${desaparecidos.length - 8} más)` : ''}`);
  }
  if (nuevos.length > 0) {
    const muestra = nuevos.slice(0, 8).join(', ');
    console.log(`  muestra nuevos:        ${muestra}${nuevos.length > 8 ? ` ... (${nuevos.length - 8} más)` : ''}`);
  }

  return { sellerId, name, runActual, runAnterior, setActual, setAnterior, desaparecidos, nuevos };
}

// ─── Guardar en bajas_detectadas ──────────────────────────────────────────────
async function saveChanges(rows) {
  if (!rows.length) return 0;

  const { error } = await supabase.from('bajas_detectadas').insert(rows);
  if (!error) return rows.length;

  console.warn(`  ⚠️  Bulk insert falló (${error.message}), insertando de a uno...`);
  let ok = 0;
  for (const row of rows) {
    const { error: e2 } = await supabase.from('bajas_detectadas').insert([row]);
    if (!e2) ok++;
    else console.warn(`    ❌ ${row.item_id} [${row.tipo}]: ${e2.message}`);
  }
  return ok;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
(async () => {
  const DET_RUN_ID = `det_${Date.now()}`;
  const NOW        = new Date().toISOString();

  console.log(`\n${SEP}`);
  console.log('DETECTOR BAJAS v4 — INICIO');
  console.log(`det_run_id: ${DET_RUN_ID}`);
  console.log(`timestamp:  ${NOW}`);
  console.log(`${SEP}`);

  let sellers;
  try {
    sellers = await getActiveSellers();
    console.log(`\nSellers activos: ${sellers.length}`);
  } catch (e) {
    console.error(`❌ ${e.message}`);
    process.exit(1);
  }

  const results = [];

  for (const seller of sellers) {
    try {
      const r = await processSeller(seller);
      if (r) results.push(r);
    } catch (e) {
      console.error(`  ❌ Error en seller ${seller.seller_id}: ${e.message}`);
    }
  }

  // ── Resumen tabular ──────────────────────────────────────────────────────────
  console.log(`\n${SEP}`);
  console.log('RESUMEN POR SELLER');
  console.log(SEP);
  console.log(
    'Seller'.padEnd(38) + ' | ' +
    'ant'.padStart(4)   + ' | ' +
    'act'.padStart(4)   + ' | ' +
    'desap'.padStart(5) + ' | ' +
    'nuevo'.padStart(5)
  );
  console.log(SEP2);

  let totDes = 0;
  let totNuevos = 0;
  for (const r of results) {
    console.log(
      r.name.slice(0, 37).padEnd(38) + ' | ' +
      String(r.setAnterior.size).padStart(4) + ' | ' +
      String(r.setActual.size).padStart(4)   + ' | ' +
      String(r.desaparecidos.length).padStart(5) + ' | ' +
      String(r.nuevos.length).padStart(5)
    );
    totDes    += r.desaparecidos.length;
    totNuevos += r.nuevos.length;
  }
  console.log(SEP2);
  console.log(
    'TOTAL'.padEnd(38) + ' | ' +
    '    '             + ' | ' +
    '    '             + ' | ' +
    String(totDes).padStart(5) + ' | ' +
    String(totNuevos).padStart(5)
  );
  console.log(SEP);

  // ── Guardar (con idempotencia por seller por ventana 6h) ─────────────────────
  let totalSaved   = 0;
  let totalSkipped = 0;

  for (const r of results) {
    const hayBajas  = r.desaparecidos.length > 0;
    const haySubidas = r.nuevos.length > 0;
    if (!hayBajas && !haySubidas) continue;

    // Idempotencia: si ya hay detecciones para este seller en las últimas 6h → saltar
    const alreadyDone = await alreadyDetectedSeller(r.sellerId, r.runAnterior);
    if (alreadyDone) {
      console.log(`  ⏭️  ${r.name}: detección ya guardada (últimas 6h), salteando.`);
      totalSkipped++;
      continue;
    }

    // Enriquecer items con título y precio antes de guardar
    const allItemIds = [...r.desaparecidos, ...r.nuevos];
    const enriched   = await enrichItems(allItemIds);

    const rows = [
      ...r.desaparecidos.map(id => ({
        seller_id:      r.sellerId,
        item_id:        id,
        meli_item_id:   id,
        tipo:           'desaparecido_no_confirmado',
        run_id:         DET_RUN_ID,
        fecha_deteccion: NOW,
        title:          enriched[id]?.title  ?? null,
        price_anterior: enriched[id]?.price  ?? null,
      })),
      ...r.nuevos.map(id => ({
        seller_id:      r.sellerId,
        item_id:        id,
        meli_item_id:   id,
        tipo:           'nuevo',
        run_id:         DET_RUN_ID,
        fecha_deteccion: NOW,
        title:          enriched[id]?.title  ?? null,
        price_anterior: enriched[id]?.price  ?? null,
      })),
    ];

    const label = [
      hayBajas  ? `${r.desaparecidos.length} bajas`  : '',
      haySubidas ? `${r.nuevos.length} subidas` : '',
    ].filter(Boolean).join(' + ');

    console.log(`\nGuardando ${label} para ${r.name}...`);
    const saved = await saveChanges(rows);
    console.log(`  ✅ ${saved}/${rows.length} guardados`);
    totalSaved += saved;
  }

  // ── Escribir summary JSON ────────────────────────────────────────────────────
  const summary = {
    det_run_id: DET_RUN_ID,
    timestamp:  NOW,
    sellers: results.map(r => ({
      seller_id:      r.sellerId,
      name:           r.name,
      run_actual:     r.runActual,
      run_anterior:   r.runAnterior,
      items_actuales: r.setActual.size,
      desaparecidos:  r.desaparecidos.length,
      nuevos:         r.nuevos.length,
    })),
  };
  fs.mkdirSync('output', { recursive: true });
  fs.writeFileSync('output/detector_summary.json', JSON.stringify(summary, null, 2));
  console.log('\n📄 Summary escrito en output/detector_summary.json');

  // ── Cierre ───────────────────────────────────────────────────────────────────
  console.log(`\n${SEP}`);
  console.log('DETECTOR v5 — COMPLETADO');
  console.log(`  Sellers procesados: ${results.length}/${sellers.length}`);
  console.log(`  Desaparecidos:      ${totDes}`);
  console.log(`  Nuevos:             ${totNuevos}`);
  console.log(`  Guardados:          ${totalSaved}`);
  console.log(`  Salteados:          ${totalSkipped}`);
  console.log(`${SEP}\n`);

  process.exit(0);
})();
