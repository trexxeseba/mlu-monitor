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

// ─── 2 run_ids más recientes para un seller, con timestamps ──────────────────
async function getLastTwoRunIds(sellerId) {
  const { data, error } = await supabase
    .from('snapshots')
    .select('run_id, checked_at')
    .eq('seller_id', String(sellerId))
    .order('checked_at', { ascending: false })
    .limit(5000);

  if (error) throw new Error(`getLastTwoRunIds(${sellerId}): ${error.message}`);

  const seen = [];
  const ts   = {};
  for (const row of (data || [])) {
    if (row.run_id && !seen.includes(row.run_id)) {
      seen.push(row.run_id);
      ts[row.run_id] = row.checked_at;
    }
    if (seen.length >= 2) break;
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

// ─── Idempotencia: ¿ya guardamos desaparecidos para este seller en este par de runs? ──
// Verifica si alguno de los items desaparecidos ya existe en bajas_detectadas
// para este seller con tipo='desaparecido_no_confirmado'.
async function alreadyDetectedSeller(sellerId, desaparecidos) {
  if (!desaparecidos.length) return false;
  const sample = desaparecidos.slice(0, 5);
  const { data, error } = await supabase
    .from('bajas_detectadas')
    .select('item_id')
    .eq('seller_id', String(sellerId))
    .eq('tipo', 'desaparecido_no_confirmado')
    .in('item_id', sample)
    .limit(1);
  if (error) return false; // si no se puede verificar, dejamos pasar
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

  // 3. Desaparecidos (BAJADAS)
  // Por diseño UPSERT: setAnterior = items que ya no están en setActual → son las bajas.
  // "nuevos" desactivado: con UPSERT los sets son disjuntos → falsos positivos masivos.
  const desaparecidos = [...setAnterior];

  console.log(`  desaparecidos: ${desaparecidos.length}`);

  // 4. Muestra auditable
  if (desaparecidos.length > 0) {
    const muestra = desaparecidos.slice(0, 8).join(', ');
    console.log(`  muestra desaparecidos: ${muestra}${desaparecidos.length > 8 ? ` ... (${desaparecidos.length - 8} más)` : ''}`);
  }

  return { sellerId, name, runActual, runAnterior, setActual, setAnterior, desaparecidos, nuevos: [] };
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
    'desap'.padStart(5)
  );
  console.log(SEP2);

  let totDes = 0;
  let totNuevos = 0;
  for (const r of results) {
    console.log(
      r.name.slice(0, 37).padEnd(38) + ' | ' +
      String(r.setAnterior.size).padStart(4) + ' | ' +
      String(r.setActual.size).padStart(4)   + ' | ' +
      String(r.desaparecidos.length).padStart(5)
    );
    totDes += r.desaparecidos.length;
    totNuevos += r.nuevos.length;
  }
  console.log(SEP2);
  console.log(
    'TOTAL'.padEnd(38) + ' | ' +
    '    '             + ' | ' +
    '    '             + ' | ' +
    String(totDes).padStart(5)
  );
  console.log(SEP);

  // ── Guardar (con idempotencia por seller) ────────────────────────────────────
  let totalSaved   = 0;
  let totalSkipped = 0;

  for (const r of results) {
    if (!r.desaparecidos.length) continue;

    // Chequeo de idempotencia: si ya existen entradas para este seller+items → saltar
    const alreadyDone = await alreadyDetectedSeller(r.sellerId, r.desaparecidos);
    if (alreadyDone) {
      console.log(`  ⏭️  ${r.name}: detección ya guardada, salteando.`);
      totalSkipped++;
      continue;
    }

    // Enriquecer con título y precio antes de guardar
    const enriched = await enrichItems(r.desaparecidos);

    const rows = r.desaparecidos.map(id => ({
      seller_id:     r.sellerId,
      item_id:       id,
      meli_item_id:  id,
      tipo:          'desaparecido_no_confirmado',
      run_id:        DET_RUN_ID,
      fecha_deteccion: NOW,
      title:         enriched[id]?.title     ?? null,
      price_anterior: enriched[id]?.price    ?? null,
    }));

    console.log(`\nGuardando ${r.desaparecidos.length} bajas para ${r.name}...`);
    const saved = await saveChanges(rows);
    console.log(`  ✅ ${saved}/${rows.length} guardados`);
    totalSaved += saved;
  }

  // ── Escribir summary JSON para write_sheets.js ───────────────────────────────
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
    })),
  };
  fs.mkdirSync('output', { recursive: true });
  fs.writeFileSync('output/detector_summary.json', JSON.stringify(summary, null, 2));
  console.log('\n📄 Summary escrito en output/detector_summary.json');

  // ── Cierre ───────────────────────────────────────────────────────────────────
  console.log(`\n${SEP}`);
  console.log('DETECTOR COMPLETADO');
  console.log(`  Sellers procesados: ${results.length}/${sellers.length}`);
  console.log(`  Desaparecidos:      ${totDes}`);
  console.log(`  Guardados:          ${totalSaved}`);
  console.log(`  Salteados (ya guardados): ${totalSkipped}`);
  console.log(`${SEP}\n`);

  process.exit(0);
})();
