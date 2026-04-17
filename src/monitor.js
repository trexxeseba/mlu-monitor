'use strict';

const https = require('https');
const { createClient } = require('@supabase/supabase-js');

// ─── Verificar secrets ────────────────────────────────────────────────────────
['SUPABASE_URL', 'SUPABASE_KEY', 'OXYLABS_USER', 'OXYLABS_PASS'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase      = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const OXYLABS_USER  = process.env.OXYLABS_USER;
const OXYLABS_PASS  = process.env.OXYLABS_PASS;

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

// ─── HTTP helpers ─────────────────────────────────────────────────────────────
function httpGet(hostname, path, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'GET' }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error(`timeout ${timeoutMs}ms`)); });
    req.end();
  });
}

function httpPost(hostname, path, body, headers = {}, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const bodyBuf = Buffer.from(body, 'utf8');
    const opts = {
      hostname, path, method: 'POST',
      headers: { 'Content-Length': bodyBuf.length, ...headers },
    };
    const req = https.request(opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error(`timeout ${timeoutMs}ms`)); });
    req.write(bodyBuf);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Oxylabs: scrape una URL y devolver HTML + IDs ───────────────────────────
async function fetchPage(url) {
  const payload = JSON.stringify({ source: 'universal', url });
  const auth    = Buffer.from(`${OXYLABS_USER}:${OXYLABS_PASS}`).toString('base64');
  const res = await httpPost('realtime.oxylabs.io', '/v1/queries', payload, {
    'Content-Type':  'application/json',
    'Authorization': `Basic ${auth}`,
  }, 60000);

  if (res.status !== 200)
    throw new Error(`oxylabs HTTP ${res.status}: ${res.body.slice(0, 200)}`);

  let html;
  try { html = JSON.parse(res.body).results?.[0]?.content ?? ''; }
  catch (e) { throw new Error(`oxylabs JSON inválido: ${e.message}`); }

  if (!html || html.length < 5000)
    throw new Error(`HTML corto (${html?.length ?? 0} bytes)`);
  if (html.includes('account-verification'))
    throw new Error('bloqueado (account-verification)');
  if (html.includes('suspicious_traffic'))
    throw new Error('bloqueado (suspicious_traffic)');

  const ids = [...new Set((html.match(/MLU\d{9,12}/g) || []))];
  return { html, ids };
}

// ─── scrapeSellerIds: paginación automática ───────────────────────────────────
// ML pagina en grupos de 48. URL: /_CustId_ID_Desde_49_NoIndex_True, _Desde_97, ...
async function scrapeSellerIds(sellerId) {
  const base = `https://listado.mercadolibre.com.uy/_CustId_${sellerId}`;
  console.log(`  📡 scraping seller ${sellerId}...`);

  const allIds = new Set();
  let   page   = 1;
  const PAGE_SIZE = 48;
  const MAX_PAGES = 25; // tope de seguridad (1200 items max)

  while (page <= MAX_PAGES) {
    const url = page === 1
      ? base
      : `${base}_Desde_${1 + PAGE_SIZE * (page - 1)}_NoIndex_True`;

    let result;
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        console.log(`    página=${page} attempt=${attempt} url=...Desde_${page === 1 ? 1 : 1 + PAGE_SIZE * (page - 1)}`);
        result = await fetchPage(url);
        break;
      } catch (err) {
        console.log(`    página=${page} attempt=${attempt} error=${err.message}`);
        if (attempt === 1) await sleep(3000);
        else if (page === 1) throw new Error(`Oxylabs falló en página 1: ${err.message}`);
        else { console.log(`    página=${page} — skip (no es crítico)`); result = null; break; }
      }
    }

    if (!result) break;

    const antes = allIds.size;
    result.ids.forEach(id => allIds.add(id));
    const nuevos = allIds.size - antes;
    console.log(`    página=${page} ids_nuevos=${nuevos} total_acum=${allIds.size} bytes=${result.html.length}`);

    // Si no hay más páginas de paginación o no llegaron IDs nuevos → fin
    const hasNextPage = result.html.includes(`_Desde_${1 + PAGE_SIZE * page}`);
    if (!hasNextPage || nuevos === 0) break;

    page++;
    await sleep(1000); // pausa cortés entre páginas
  }

  if (!allIds.size) throw new Error('0 IDs MLU encontrados en todas las páginas');
  console.log(`  ✅ seller=${sellerId} total_ids=${allIds.size} páginas=${page}`);
  return [...allIds];
}

// ─── Obtener sellers activos ──────────────────────────────────────────────────
async function getActiveSellers() {
  const { data, error } = await supabase
    .from('sellers')
    .select('*')
    .eq('activo', true)
    .order('seller_id', { ascending: true });
  if (error) throw new Error(`getSellers: ${error.message}`);
  if (!data?.length) throw new Error('No hay sellers activos en la BD');
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

// ─── Guardar snapshots de un seller ──────────────────────────────────────────
// UPSERT por meli_item_id (constraint único que ya existe en la tabla).
// Cada item guarda el run_id del último run en que fue visto.
// Items no vistos en este run conservan su run_id anterior → el detector
// los detecta como desaparecidos comparando run_id=actual vs run_id=anterior.
// Chunks de 500 para no superar límites de payload de Supabase.
const UPSERT_CHUNK = 500;

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

  let saved = 0;
  for (let i = 0; i < rows.length; i += UPSERT_CHUNK) {
    const chunk = rows.slice(i, i + UPSERT_CHUNK);
    const { error } = await supabase
      .from('snapshots')
      .upsert(chunk, { onConflict: 'meli_item_id' });
    if (error) throw new Error(`saveSnapshots chunk ${i}-${i + chunk.length}: ${error.message}`);
    saved += chunk.length;
  }
  return saved;
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
    // 1. Obtener IDs desde el listado via Oxylabs
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
