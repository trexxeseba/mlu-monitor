const https = require('https');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const BD_API_KEY  = process.env.BD_API_KEY  || '0701164e-6bc2-4f78-af4d-4910090ac9e7';
const BD_ZONE     = process.env.BD_ZONE     || 'web_unlocker1mlu_monitor';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

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
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve({ status: res.statusCode, html: data }));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ─── Extraer items del HTML (polycard) ────────────────────────────────────────

function extractItems(html) {
  const scriptMatches = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
  const bigScript = scriptMatches.find(
    (m) => m[1].includes('"polycard"') && m[1].includes('"components"')
  );
  if (!bigScript) return [];

  const content = bigScript[1];
  const seen = new Set();
  const items = [];
  let searchFrom = 0;

  while (true) {
    const polycardKey = '"polycard":';
    const keyIdx = content.indexOf(polycardKey, searchFrom);
    if (keyIdx === -1) break;

    const objStart = content.indexOf('{', keyIdx + polycardKey.length);
    if (objStart === -1) break;

    const objStr = extractBalancedJson(content, objStart);
    if (!objStr) { searchFrom = keyIdx + 1; continue; }

    try {
      const polycard = JSON.parse(objStr);
      const item = parsePolycard(polycard);
      if (item && !seen.has(item.id)) {
        seen.add(item.id);
        items.push(item);
      }
    } catch { /* skip */ }

    searchFrom = objStart + objStr.length;
  }

  return items;
}

function extractBalancedJson(str, start) {
  let depth = 0, inString = false, escape = false;
  for (let i = start; i < str.length; i++) {
    const ch = str[i];
    if (escape) { escape = false; continue; }
    if (ch === '\\' && inString) { escape = true; continue; }
    if (ch === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (ch === '{') depth++;
    else if (ch === '}') { depth--; if (depth === 0) return str.substring(start, i + 1); }
  }
  return null;
}

function parsePolycard(polycard) {
  const meta = polycard.metadata;
  if (!meta?.id) return null;
  const components = polycard.components ?? [];
  const titleComp = components.find((c) => c.type === 'title');
  const priceComp = components.find((c) => c.type === 'price');
  return {
    id: meta.id,
    title: titleComp?.title?.text ?? null,
    price: priceComp?.price?.current_price?.value ?? null,
    currency: priceComp?.price?.current_price?.currency ?? null,
  };
}

// ─── Supabase ─────────────────────────────────────────────────────────────────

async function getActiveSellers() {
  const { data, error } = await supabase.from('sellers').select('*').eq('activo', true);
  if (error) throw new Error(`getSellers: ${error.message}`);
  return data;
}

async function saveSnapshot(sellerId, items) {
  const now = new Date().toISOString();
  const rows = items.map((item) => ({
    seller_id: String(sellerId),
    meli_item_id: item.id,
    item_id: item.id,
    title: item.title,
    price: item.price,
    sold_quantity: 0,
    available_quantity: 0,
    status: 'active',
    timestamp: now,
  }));

  const { error } = await supabase
    .from('snapshots')
    .upsert(rows, { onConflict: 'meli_item_id' });
  if (error) throw new Error(`saveSnapshot: ${error.message}`);
  return { count: rows.length, timestamp: now };
}

// ─── Procesar seller ──────────────────────────────────────────────────────────

async function processSeller(seller) {
  const sellerId = seller.seller_id;
  const name = seller.nombre_real ?? seller.nickname ?? sellerId;
  const url = `https://listado.mercadolibre.com.uy/_CustId_${sellerId}`;

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Seller: ${sellerId} — ${name}`);
  console.log(`URL: ${url}`);

  console.log('  Scraping...');
  const { status, html } = await scrape(url);
  console.log(`  HTTP status: ${status}  |  HTML: ${html.length.toLocaleString()} chars`);

  const items = extractItems(html);
  console.log(`  Items extraídos: ${items.length}`);

  if (items.length === 0) {
    console.warn('  ⚠ No se extrajeron items — se omite guardado.');
    return;
  }

  console.log('  Muestra (primeros 3):');
  items.slice(0, 3).forEach((i) =>
    console.log(`    [${i.id}] ${i.title ?? '(sin título)'} — ${i.currency} ${i.price}`)
  );

  const saved = await saveSnapshot(sellerId, items);
  console.log(`  ✅ Snapshot guardado: ${saved.count} items @ ${saved.timestamp}`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('MLU Monitor — inicio');
  console.log(new Date().toISOString());

  let sellers;
  try {
    sellers = await getActiveSellers();
    console.log(`\nSellers activos en BD: ${sellers.length}`);
  } catch (err) {
    console.warn(`No se pudo leer tabla sellers: ${err.message}`);
    sellers = [];
  }

  if (!sellers || sellers.length === 0) {
    console.log('Usando seller de prueba: 42794274');
    sellers = [{ seller_id: 42794274, nickname: 'Test seller', activo: true }];
  }

  for (const seller of sellers) {
    try {
      await processSeller(seller);
    } catch (err) {
      console.error(`  ❌ Error procesando seller ${seller.seller_id}: ${err.message}`);
    }
  }

  console.log('\nMLU Monitor — fin');
}

main().catch(console.error);
