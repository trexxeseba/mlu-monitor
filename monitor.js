const https = require('https');
const fs    = require('fs');
const { createClient } = require('@supabase/supabase-js');

// ─── Fail fast ────────────────────────────────────────────────────────────────
['SUPABASE_URL','SUPABASE_KEY','SCRAPFLY_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase     = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const SCRAPFLY_KEY = process.env.SCRAPFLY_KEY;

const RUN_ID     = `run_${Date.now()}`;
const CHECKED_AT = new Date().toISOString();

console.log(`RUN_ID:     ${RUN_ID}`);
console.log(`CHECKED_AT: ${CHECKED_AT}`);

// ─── HTTP helper ──────────────────────────────────────────────────────────────
function httpGet(hostname, path) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'GET' }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.end();
  });
}

// ─── Scrapfly ─────────────────────────────────────────────────────────────────
async function scrapeSeller(sellerId) {
  const targetUrl = encodeURIComponent(`https://listado.mercadolibre.com.uy/_CustId_${sellerId}`);
  const path = `/scrape?key=${SCRAPFLY_KEY}&url=${targetUrl}&asp=true&render_js=true&rendering_wait=5000&country=uy`;

  const res = await httpGet('api.scrapfly.io', path);
  if (res.status !== 200) throw new Error(`Scrapfly HTTP ${res.status}`);

  const raw = res.body;

  // Extraer HTML unicode-escaped del JSON de Scrapfly
  const contentIdx = raw.indexOf('"content"');
  if (contentIdx < 0) throw new Error('Scrapfly: no hay campo content');
  const contentStart = contentIdx + '"content":"'.length;
  const endMarker = raw.indexOf('","format":', contentStart);
  if (endMarker < 0) throw new Error('Scrapfly: no encontre cierre de content');

  const html = raw.slice(contentStart, endMarker)
    .replace(/\\u([\dA-Fa-f]{4})/g, (_, c) => String.fromCharCode(parseInt(c, 16)))
    .replace(/\\"/g, '"').replace(/\\\\/g, '\\').replace(/\\n/g, '\n').replace(/\\r/g, '');

  if (html.includes('account-verification') || html.length < 10000) {
    throw new Error('Scrapfly: pagina bloqueada o vacia');
  }

  // Extraer JSON de resultados del script _n.ctx.r
  const scriptMatch = html.match(/_n\.ctx\.r=(\{[\s\S]+?);\s*_n\.ctx\.r\.assets/);
  if (!scriptMatch) throw new Error('Scrapfly: no encontre _n.ctx.r en HTML');

  let ctxData;
  try { ctxData = JSON.parse(scriptMatch[1]); }
  catch (e) { throw new Error(`JSON parse: ${e.message}`); }

  const results = ctxData?.appProps?.pageProps?.initialState?.results ?? [];
  if (!results.length) throw new Error('0 resultados en initialState');

  const items = [];
  for (const r of results) {
    const meta = r?.polycard?.metadata ?? {};
    const itemId = meta.id;
    if (!itemId || !itemId.startsWith('MLU')) continue;

    let title = null, price = null;
    for (const comp of r?.polycard?.components ?? []) {
      if (comp.type === 'title') title = comp?.title?.text ?? null;
      if (comp.type === 'price') price = comp?.price?.current_price?.value ?? null;
    }
    items.push({ id: itemId, title, price, currency: 'UYU' });
  }
  return items;
}

// ─── Supabase ─────────────────────────────────────────────────────────────────
async function getActiveSellers() {
  const { data, error } = await supabase.from('sellers').select('*').eq('activo', true);
  if (error) throw new Error(`getSellers: ${error.message}`);
  return data;
}

async function saveSnapshot(sellerId, items) {
  const rows = items.map(item => ({
    seller_id: String(sellerId), meli_item_id: item.id, item_id: item.id,
    title: item.title, price: item.price, sold_quantity: 0, available_quantity: 1,
    status: 'active', run_id: RUN_ID, checked_at: CHECKED_AT, timestamp: CHECKED_AT,
  }));
  const { error } = await supabase.from('snapshots').upsert(rows, { onConflict: 'meli_item_id', ignoreDuplicates: false });
  if (error) throw new Error(`saveSnapshot: ${error.message}`);
  return rows.length;
}

// ─── Procesar seller ──────────────────────────────────────────────────────────
async function processSeller(seller) {
  const sellerId = seller.seller_id;
  const name = seller.nombre_real ?? seller.nickname ?? String(sellerId);
  console.log(`\n${'─'.repeat(60)}\nSeller: ${sellerId} — ${name}`);

  let status = 'failed', errorMessage = '', itemsFound = 0;
  try {
    const items = await scrapeSeller(sellerId);
    itemsFound = items.length;
    console.log(`  Items: ${items.length}`);
    if (!items.length) throw new Error('0_items');
    items.slice(0, 3).forEach(i =>
      console.log(`    [${i.id}] ${(i.title ?? '').substring(0, 50)} — UYU ${i.price}`)
    );
    await saveSnapshot(sellerId, items);
    console.log(`  OK: ${items.length} guardados`);
    status = 'ok';
  } catch (err) {
    errorMessage = err.message;
    console.error(`  ERROR: ${errorMessage}`);
  }
  return { seller_id: String(sellerId), run_id: RUN_ID, status, error_message: errorMessage, items_found: itemsFound };
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== MLU Monitor (Scrapfly) ===\n' + new Date().toISOString());
  fs.mkdirSync('output', { recursive: true });

  let sellers;
  try {
    sellers = await getActiveSellers();
    console.log(`Sellers activos: ${sellers.length}`);
  } catch (err) {
    console.warn(`WARN: ${err.message} — fallback TIOPACO`);
    sellers = [{ seller_id: 42794274, nickname: 'TIOPACO', activo: true }];
  }

  // TEST MODE: limitar a 5 sellers para verificar funcionamiento
  const testSellers = sellers.slice(0, 5);
  console.log(`TEST MODE: procesando ${testSellers.length} de ${sellers.length} sellers`);
  const statusRows = [];
  for (const seller of testSellers) statusRows.push(await processSeller(seller));

  const csv = [
    'seller_id,run_id,status,error_message,items_found',
    ...statusRows.map(r => `${r.seller_id},${r.run_id},${r.status},"${r.error_message}",${r.items_found}`)
  ].join('\n');
  fs.writeFileSync('output/monitor_status.csv', csv);

  const ok = statusRows.filter(r => r.status === 'ok').length;
  const fail = statusRows.filter(r => r.status === 'failed').length;
  console.log(`\n=== Fin: ${ok} OK / ${fail} fallidos ===`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
