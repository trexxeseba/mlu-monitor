const https = require('https');
const fs    = require('fs');
const { createClient } = require('@supabase/supabase-js');

// ─── Fail fast ────────────────────────────────────────────────────────────────
['SUPABASE_URL','SUPABASE_KEY','BD_API_KEY','BD_ZONE'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase   = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const BD_API_KEY = process.env.BD_API_KEY;
const BD_ZONE    = process.env.BD_ZONE;

// ─── Identidad del run — generados UNA sola vez, compartidos por todo ─────────
const RUN_ID    = `run_${Date.now()}`;          // ej: run_1741528800000
const CHECKED_AT = new Date().toISOString();     // ej: 2026-03-09T14:00:00.000Z

console.log(`RUN_ID:     ${RUN_ID}`);
console.log(`CHECKED_AT: ${CHECKED_AT}`);

// ─── Bright Data ──────────────────────────────────────────────────────────────
function scrape(url) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({ zone: BD_ZONE, url, format: 'raw' });
    const options = {
      hostname: 'api.brightdata.com',
      path:     '/request',
      method:   'POST',
      headers:  {
        'Content-Type':   'application/json',
        'Authorization':  `Bearer ${BD_API_KEY}`,
        'Content-Length': Buffer.byteLength(payload),
      },
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, html: data }));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ─── Parseo polycard ──────────────────────────────────────────────────────────
function extractBalancedJson(str, start) {
  let depth = 0, inString = false, escape = false;
  for (let i = start; i < str.length; i++) {
    const ch = str[i];
    if (escape)                  { escape = false; continue; }
    if (ch === '\\' && inString) { escape = true;  continue; }
    if (ch === '"')              { inString = !inString; continue; }
    if (inString) continue;
    if (ch === '{') depth++;
    else if (ch === '}') { depth--; if (depth === 0) return str.substring(start, i + 1); }
  }
  return null;
}

function parsePolycard(pc) {
  const meta = pc.metadata;
  if (!meta?.id) return null;
  const comps     = pc.components ?? [];
  const titleComp = comps.find(c => c.type === 'title');
  const priceComp = comps.find(c => c.type === 'price');
  return {
    id:       meta.id,
    title:    titleComp?.title?.text ?? null,
    price:    priceComp?.price?.current_price?.value ?? null,
    currency: priceComp?.price?.current_price?.currency ?? null,
  };
}

function extractItems(html) {
  const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
  const big = scripts.find(m => m[1].includes('"polycard"') && m[1].includes('"components"'));
  if (!big) return [];

  const content = big[1];
  const seen    = new Set();
  const items   = [];
  let from      = 0;
  const KEY     = '"polycard":';

  while (true) {
    const ki = content.indexOf(KEY, from);
    if (ki === -1) break;
    const os  = content.indexOf('{', ki + KEY.length);
    if (os === -1) break;
    const obj = extractBalancedJson(content, os);
    if (!obj) { from = ki + 1; continue; }
    try {
      const item = parsePolycard(JSON.parse(obj));
      if (item && !seen.has(item.id)) { seen.add(item.id); items.push(item); }
    } catch { /* skip malformed */ }
    from = os + obj.length;
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
    seller_id:          String(sellerId),
    meli_item_id:       item.id,
    item_id:            item.id,
    title:              item.title,
    price:              item.price,
    sold_quantity:      0,
    available_quantity: 0,
    status:             'active',
    run_id:             RUN_ID,       // ← identificador del run
    checked_at:         CHECKED_AT,   // ← timestamp único del run
    timestamp:          CHECKED_AT,   // ← mantener columna vieja en sync
  }));

  const { error } = await supabase.from('snapshots').insert(rows);
  if (error) throw new Error(`saveSnapshot: ${error.message}`);
  return rows.length;
}

// ─── Procesar seller ──────────────────────────────────────────────────────────
async function processSeller(seller) {
  const sellerId = seller.seller_id;
  const name     = seller.nombre_real ?? seller.nickname ?? String(sellerId);
  const url      = `https://listado.mercadolibre.com.uy/_CustId_${sellerId}`;

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Seller: ${sellerId} — ${name}`);

  let status = 'failed';
  let errorMessage = '';
  let itemsFound = 0;

  try {
    const { status: httpStatus, html } = await scrape(url);
    console.log(`  HTTP ${httpStatus} | ${html.length.toLocaleString()} chars`);

    if (httpStatus !== 200) {
      throw new Error(`HTTP ${httpStatus}`);
    }

    const items = extractItems(html);
    console.log(`  Items extraídos: ${items.length}`);
    itemsFound = items.length;

    if (items.length === 0) {
      throw new Error('0_items_extraidos');
    }

    items.slice(0, 3).forEach(i =>
      console.log(`    [${i.id}] ${(i.title ?? '').substring(0, 50)} — ${i.currency} ${i.price}`)
    );

    await saveSnapshot(sellerId, items);
    console.log(`  OK: ${items.length} items guardados`);
    status = 'ok';

  } catch (err) {
    errorMessage = err.message;
    console.error(`  ERROR: ${errorMessage}`);
  }

  return {
    seller_id:     String(sellerId),
    run_id:        RUN_ID,
    status,                          // 'ok' | 'failed'
    error_message: errorMessage,
    items_found:   itemsFound,
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== MLU Monitor inicio ===');
  console.log(new Date().toISOString());

  fs.mkdirSync('output', { recursive: true });

  let sellers;
  try {
    sellers = await getActiveSellers();
    console.log(`Sellers activos: ${sellers.length}`);
  } catch (err) {
    console.warn(`WARN getSellers: ${err.message} — usando fallback`);
    sellers = [{ seller_id: 42794274, nickname: 'TIOPACO', activo: true }];
  }

  const statusRows = [];
  for (const seller of sellers) {
    const result = await processSeller(seller);
    statusRows.push(result);
  }

  // Escribir monitor_status.csv — una fila por seller, evidencia explícita
  const csvLines = [
    'seller_id,run_id,status,error_message,items_found',
    ...statusRows.map(r =>
      `${r.seller_id},${r.run_id},${r.status},"${r.error_message}",${r.items_found}`
    ),
  ];
  fs.writeFileSync('output/monitor_status.csv', csvLines.join('\n'));
  console.log(`\nmonitor_status.csv escrito (${statusRows.length} sellers)`);

  const ok   = statusRows.filter(r => r.status === 'ok').length;
  const fail = statusRows.filter(r => r.status === 'failed').length;
  console.log(`=== Monitor fin: ${ok} OK / ${fail} fallidos ===`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
