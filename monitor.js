const https = require('https');
const { createClient } = require('@supabase/supabase-js');

// Fail fast si faltan secrets
['SUPABASE_URL','SUPABASE_KEY','BD_API_KEY','BD_ZONE'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const BD_API_KEY = process.env.BD_API_KEY;
const BD_ZONE    = process.env.BD_ZONE;

// ─── Bright Data ──────────────────────────────────────────────────────────────

function scrape(url) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({ zone: BD_ZONE, url, format: 'raw' });
    const options = {
      hostname: 'api.brightdata.com',
      path: '/request',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${BD_API_KEY}`,
        'Content-Length': Buffer.byteLength(payload),
      },
    };
    const req = https.request(options, (res) => {
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
    if (escape)              { escape = false; continue; }
    if (ch === '\\' && inString) { escape = true; continue; }
    if (ch === '"')          { inString = !inString; continue; }
    if (inString)            continue;
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
  const seen = new Set();
  const items = [];
  let from = 0;
  const KEY = '"polycard":';

  while (true) {
    const ki = content.indexOf(KEY, from);
    if (ki === -1) break;
    const os = content.indexOf('{', ki + KEY.length);
    if (os === -1) break;
    const obj = extractBalancedJson(content, os);
    if (!obj) { from = ki + 1; continue; }
    try {
      const item = parsePolycard(JSON.parse(obj));
      if (item && !seen.has(item.id)) { seen.add(item.id); items.push(item); }
    } catch { /* skip */ }
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

// INSERT siempre — nunca upsert, para preservar historial de snapshots
async function saveSnapshot(sellerId, items) {
  const now = new Date().toISOString();
  const rows = items.map(item => ({
    seller_id:          String(sellerId),
    meli_item_id:       item.id,
    item_id:            item.id,
    title:              item.title,
    price:              item.price,
    sold_quantity:      0,
    available_quantity: 0,
    status:             'active',
    timestamp:          now,
  }));

  const { error } = await supabase.from('snapshots').insert(rows);
  if (error) throw new Error(`saveSnapshot: ${error.message}`);
  return { count: rows.length, timestamp: now };
}

// ─── Procesar seller ──────────────────────────────────────────────────────────

async function processSeller(seller) {
  const sellerId = seller.seller_id;
  const name     = seller.nombre_real ?? seller.nickname ?? String(sellerId);
  const url      = `https://listado.mercadolibre.com.uy/_CustId_${sellerId}`;

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Seller: ${sellerId} — ${name}`);

  const { status, html } = await scrape(url);
  console.log(`  HTTP ${status} | ${html.length.toLocaleString()} chars`);

  const items = extractItems(html);
  console.log(`  Items extraídos: ${items.length}`);

  if (items.length === 0) {
    console.warn('  WARN: 0 items extraídos — snapshot omitido');
    return { sellerId, name, count: 0, ok: false };
  }

  items.slice(0, 3).forEach(i =>
    console.log(`    [${i.id}] ${(i.title ?? '').substring(0,50)} — ${i.currency} ${i.price}`)
  );

  const saved = await saveSnapshot(sellerId, items);
  console.log(`  OK: ${saved.count} items guardados @ ${saved.timestamp}`);
  return { sellerId, name, count: saved.count, ok: true };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('=== MLU Monitor inicio ===');
  console.log(new Date().toISOString());

  let sellers;
  try {
    sellers = await getActiveSellers();
    console.log(`Sellers activos: ${sellers.length}`);
  } catch (err) {
    console.warn(`WARN getSellers: ${err.message} — usando fallback`);
    sellers = [{ seller_id: 42794274, nickname: 'TIOPACO', activo: true }];
  }

  const results = [];
  for (const seller of sellers) {
    try {
      const r = await processSeller(seller);
      results.push(r);
    } catch (err) {
      console.error(`  ERROR seller ${seller.seller_id}: ${err.message}`);
      results.push({ sellerId: seller.seller_id, name: seller.nickname, count: 0, ok: false });
    }
  }

  const ok  = results.filter(r => r.ok).length;
  const bad = results.filter(r => !r.ok).length;
  console.log(`\n=== Monitor fin: ${ok} OK / ${bad} con problemas ===`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
