'use strict';

const https = require('https');
const { createClient } = require('@supabase/supabase-js');

// ─── Verificar secrets ────────────────────────────────────────────────────────
['SUPABASE_URL', 'SUPABASE_KEY', 'SCRAPFLY_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase     = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const SCRAPFLY_KEY = process.env.SCRAPFLY_KEY;

const RUN_ID     = `run_${Date.now()}`;
const STARTED_AT = new Date().toISOString();
const SEP = '═'.repeat(70);

// ─── Umbrales de validez ──────────────────────────────────────────────────────
const MAX_SELLER_FAIL_RATIO  = 0.30;  // >30% sellers fallidos → run inválido
const MAX_ITEMS_DROP_RATIO   = 0.40;  // >40% caída de items vs run válido previo → inválido
const MIN_SELLER_ITEMS_RATIO = 0.50;  // seller devuelve <50% de su baseline → fallo de scraping

console.log(`\n${SEP}`);
console.log('MONITOR MLU — INICIO');
console.log(`RUN_ID:     ${RUN_ID}`);
console.log(`STARTED_AT: ${STARTED_AT}`);
console.log(`${SEP}\n`);

// ─── HTTP helper ──────────────────────────────────────────────────────────────
function httpGet(hostname, path) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'GET' }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout 30s')); });
    req.end();
  });
}

// ─── Scrapfly: obtener IDs de items del listado ───────────────────────────────
// Intenta con country=uy (proxy local, menos bot detection).
// Si falla con 422 (pool agotado), reintenta con país alternativo tras delay.
async function scrapeSellerIds(sellerId) {
  const targetUrl = `https://listado.mercadolibre.com.uy/_CustId_${sellerId}`;
  // Sin country= : Scrapfly elige cualquier proxy disponible del pool global.
  // country=uy fue removido porque el pool residencial UY del plan DISCOVERY
  // se agota con 3 requests simultáneos (HTTP 422).
  const path = `/scrape?key=${SCRAPFLY_KEY}&url=${encodeURIComponent(targetUrl)}&asp=false&render_js=false`;

  console.log(`  📡 Scrapfly → seller ${sellerId}...`);

  const res = await httpGet('api.scrapfly.io', path);
  if (res.status !== 200) throw new Error(`Scrapfly HTTP ${res.status}`);

  let parsed;
  try { parsed = JSON.parse(res.body); } catch (e) {
    throw new Error(`Scrapfly: JSON inválido — ${e.message}`);
  }

  if (!parsed.result?.success) {
    throw new Error(`Scrapfly fallo: ${parsed.result?.reason || 'sin razón'} (HTTP ${parsed.result?.status_code})`);
  }

  // Loggear país del proxy efectivamente usado
  const proxyCountry = parsed.result?.context?.proxy?.country
    || parsed.context?.proxy?.country
    || 'desconocido';
  console.log(`  🌐 proxy country=${proxyCountry}`);

  const html = parsed.result.content || '';

  if (html.includes('account-verification') || html.length < 5000)
    throw new Error('Scrapfly: página bloqueada o vacía');

  console.log(`  ✅ HTML recibido: ${html.length} bytes`);

  const itemIds = [...new Set((html.match(/MLU\d{9,12}/g) || []))];
  if (!itemIds.length) throw new Error('0 IDs MLU encontrados en el HTML');

  console.log(`  📋 IDs únicos extraídos: ${itemIds.length}`);
  return itemIds;
}

// ─── Obtener sellers activos (con soporte de batches) ─────────────────────────
// SELLER_BATCH = 0..4 → scrape solo 3 sellers del batch
// Sin SELLER_BATCH (o valor inválido) → scrape todos los sellers
async function getActiveSellers() {
  const { data, error } = await supabase
    .from('sellers')
    .select('*')
    .eq('activo', true)
    .order('seller_id', { ascending: true });
  if (error) throw new Error(`getSellers: ${error.message}`);
  if (!data?.length) throw new Error('No hay sellers activos en la BD');

  const batchEnv = process.env.SELLER_BATCH;
  if (batchEnv !== undefined && batchEnv !== '') {
    const batchNum  = parseInt(batchEnv, 10);
    const batchSize = 3;
    const start     = batchNum * batchSize;
    const slice     = data.slice(start, start + batchSize);
    console.log(`📦 Batch ${batchNum}: sellers ${start + 1}–${start + slice.length} de ${data.length} total`);
    if (!slice.length) throw new Error(`Batch ${batchNum} vacío (solo hay ${data.length} sellers)`);
    return slice;
  }

  return data;
}

// ─── Obtener último run válido ────────────────────────────────────────────────
async function getLastValidRun() {
  const { data, error } = await supabase
    .from('monitor_runs')
    .select('run_id, total_items, finished_at')
    .eq('status', 'valid')
    .order('finished_at', { ascending: false })
    .limit(1);
  if (error) {
    console.warn(`  ⚠️  No se pudo consultar monitor_runs: ${error.message}`);
    return null;
  }
  return data?.[0] || null;
}

// ─── Baseline de items de un seller en el último run válido ──────────────────
async function getSellerBaseline(sellerId, validRunId) {
  if (!validRunId) return null;
  const { data, error } = await supabase
    .from('snapshots')
    .select('meli_item_id')
    .eq('seller_id', String(sellerId))
    .eq('run_id', validRunId);
  if (error) return null;
  return data?.length ?? null;
}

// ─── Guardar snapshots de un seller (solo IDs del listado) ───────────────────
// Usa UPSERT por meli_item_id para que cada item muestre el run_id del último
// run en que fue visto. Items no vistos en este run conservan el run_id anterior.
async function saveSnapshots(sellerId, itemIds) {
  const checkedAt = new Date().toISOString();
  const rows = itemIds.map(itemId => ({
    seller_id:    String(sellerId),
    meli_item_id: itemId,
    item_id:      itemId,
    run_id:       RUN_ID,
    checked_at:   checkedAt,
    timestamp:    checkedAt,
  }));

  const { error } = await supabase.from('snapshots').upsert(rows, { onConflict: 'meli_item_id' });
  if (error) throw new Error(`saveSnapshots: ${error.message}`);
  return rows.length;
}

// ─── Registrar estado del run ─────────────────────────────────────────────────
// Intenta en monitor_runs; si no existe, fallback a execution_logs.
async function upsertRun(fields) {
  const row = { run_id: RUN_ID, ...fields };
  const { error } = await supabase
    .from('monitor_runs')
    .upsert([row], { onConflict: 'run_id' });
  if (!error) return;

  // Fallback: execution_logs (tabla que ya existe)
  const logRow = {
    run_id:          RUN_ID,
    executed_at:     fields.finished_at || fields.started_at || STARTED_AT,
    items_processed: fields.total_items || 0,
    sellers_total:   fields.sellers_total || 0,
    sellers_success: fields.sellers_ok || 0,
    sellers_failed:  fields.sellers_failed || 0,
    status:          fields.status === 'valid' ? 'success'
                   : fields.status === 'invalid' ? 'partial'
                   : fields.status || 'running',
    message:         fields.invalid_reason
                     || `items:${fields.total_items || 0} sellers:${fields.sellers_total || 0}`,
  };
  const { error: logErr } = await supabase.from('execution_logs').upsert(
    [logRow], { onConflict: 'run_id', ignoreDuplicates: false }
  );
  if (logErr) {
    await supabase.from('execution_logs').insert([logRow]);
  }
}

// ─── Procesar un seller ────────────────────────────────────────────────────────
async function processSeller(seller, lastValidRunId) {
  const sellerId = seller.seller_id;
  const name     = seller.nombre_real || seller.nickname || String(sellerId);
  console.log(`\n${'─'.repeat(70)}\n🏪 ${name} (ID: ${sellerId})`);

  let status = 'failed', errorMessage = '', itemsFound = 0;

  try {
    // 1. Obtener IDs desde el listado via Scrapfly
    const itemIds = await scrapeSellerIds(sellerId);

    // 2. Validar contra baseline del último run válido
    const baseline = await getSellerBaseline(sellerId, lastValidRunId);
    if (baseline !== null && itemIds.length < baseline * MIN_SELLER_ITEMS_RATIO) {
      throw new Error(
        `Solo ${itemIds.length} items vs baseline ${baseline} — probable fallo de scraping (< ${MIN_SELLER_ITEMS_RATIO * 100}%)`
      );
    }

    itemsFound = itemIds.length;

    // 3. Guardar snapshots (solo item_ids del listado)
    const saved = await saveSnapshots(sellerId, itemIds);
    console.log(`  ✅ ${saved}/${itemsFound} item IDs guardados`);
    status = 'ok';

  } catch (err) {
    errorMessage = err.message;
    console.error(`  ❌ FALLO: ${errorMessage}`);
  }

  return { sellerId, name, status, errorMessage, itemsFound };
}

// ─── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  // Registrar inicio
  await upsertRun({ status: 'running', started_at: STARTED_AT });

  // Obtener sellers
  let sellers = [];
  try {
    sellers = await getActiveSellers();
    console.log(`📋 ${sellers.length} sellers activos\n`);
  } catch (err) {
    console.error(`\n❌ FATAL al obtener sellers: ${err.message}`);
    await upsertRun({
      status: 'invalid',
      invalid_reason: `getSellers fatal: ${err.message}`,
      finished_at: new Date().toISOString(),
    });
    process.exit(1);
  }

  // Obtener referencia del último run válido para comparación de baseline
  const lastValidRun      = await getLastValidRun();
  const lastValidRunId    = lastValidRun?.run_id    || null;
  const lastValidRunItems = lastValidRun?.total_items ?? null;

  if (lastValidRun) {
    console.log(`📌 Último run válido: ${lastValidRunId} (${lastValidRunItems} items)\n`);
  } else {
    console.log(`📌 Sin run válido previo — sin baseline de comparación\n`);
  }

  // Procesar cada seller
  const results   = [];
  let totalItems  = 0;
  let totalFailed = 0;

  for (const seller of sellers) {
    const result = await processSeller(seller, lastValidRunId);
    results.push(result);
    totalItems  += result.itemsFound;
    if (result.status === 'failed') totalFailed++;
  }

  // ─── Validación del run ───────────────────────────────────────────────────
  const failRatio      = sellers.length > 0 ? totalFailed / sellers.length : 1;
  const itemsDeltaRatio = (lastValidRunItems !== null && lastValidRunItems > 0)
    ? (lastValidRunItems - totalItems) / lastValidRunItems
    : null;

  let runStatus     = 'valid';
  let invalidReason = null;

  if (failRatio > MAX_SELLER_FAIL_RATIO) {
    runStatus     = 'invalid';
    invalidReason = `${(failRatio * 100).toFixed(1)}% sellers fallaron` +
      ` (umbral: ${MAX_SELLER_FAIL_RATIO * 100}%)`;
  } else if (itemsDeltaRatio !== null && itemsDeltaRatio > MAX_ITEMS_DROP_RATIO) {
    runStatus     = 'invalid';
    invalidReason = `Items cayeron ${(itemsDeltaRatio * 100).toFixed(1)}%` +
      ` vs run válido previo (umbral: ${MAX_ITEMS_DROP_RATIO * 100}%)`;
  }

  // ─── Resumen ──────────────────────────────────────────────────────────────
  console.log(`\n${SEP}`);
  console.log('RESUMEN FINAL');
  console.log(SEP);
  results.forEach(r =>
    console.log(
      `  ${r.status === 'ok' ? '✅' : '❌'} ${r.name}: ${r.itemsFound} items` +
      (r.errorMessage ? ` — ${r.errorMessage}` : '')
    )
  );
  console.log(`\n  Sellers total:    ${sellers.length}`);
  console.log(`  Sellers OK:       ${sellers.length - totalFailed}`);
  console.log(`  Sellers fallidos: ${totalFailed} (${(failRatio * 100).toFixed(1)}%)`);
  console.log(`  Items totales:    ${totalItems}`);
  if (itemsDeltaRatio !== null) {
    const sign = itemsDeltaRatio >= 0 ? '↓' : '↑';
    console.log(`  Items vs previo:  ${sign}${Math.abs(itemsDeltaRatio * 100).toFixed(1)}%`);
  }
  console.log(`  Run status:       ${runStatus.toUpperCase()}`);
  if (invalidReason) console.log(`  Motivo inválido:  ${invalidReason}`);
  console.log(`${SEP}\n`);

  // ─── Guardar run final ────────────────────────────────────────────────────
  await upsertRun({
    status:              runStatus,
    invalid_reason:      invalidReason,
    sellers_total:       sellers.length,
    sellers_ok:          sellers.length - totalFailed,
    sellers_failed:      totalFailed,
    total_items:         totalItems,
    prev_valid_run_id:   lastValidRunId,
    prev_total_items:    lastValidRunItems,
    items_delta_pct:     itemsDeltaRatio !== null
      ? parseFloat((itemsDeltaRatio * 100).toFixed(2))
      : null,
    finished_at: new Date().toISOString(),
  });

  if (runStatus === 'invalid') {
    console.error(`⚠️  RUN MARCADO INVÁLIDO: ${invalidReason}`);
    console.error('   El paso validate_run abortará el pipeline de detección.\n');
    process.exit(0);
  }

  console.log(`✅ RUN VÁLIDO — listo para validate_run y detector\n`);
  process.exit(0);
})();
