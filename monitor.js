const https = require('https');
const { createClient } = require('@supabase/supabase-js');
const { google } = require('googleapis');
const path = require('path');

const SUPABASE_URL = 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const BD_API_KEY = '0701164e-6bc2-4f78-af4d-4910090ac9e7';
const BD_ZONE = 'web_unlocker1mlu_monitor';
const SHEET_ID = '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';
const CREDENTIALS_PATH = path.join(__dirname, 'clauditaaa-0b93ce3a496b.json');

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ─── Bright Data scraper ──────────────────────────────────────────────────────

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

// ─── Extract items from polycard structure ────────────────────────────────────

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
    } catch {
      // skip malformed
    }

    searchFrom = objStart + objStr.length;
  }

  return items;
}

function extractBalancedJson(str, start) {
  let depth = 0;
  let inString = false;
  let escape = false;

  for (let i = start; i < str.length; i++) {
    const ch = str[i];
    if (escape) { escape = false; continue; }
    if (ch === '\\' && inString) { escape = true; continue; }
    if (ch === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) return str.substring(start, i + 1);
    }
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

// ─── Supabase helpers ─────────────────────────────────────────────────────────

async function getActiveSellers() {
  const { data, error } = await supabase
    .from('sellers')
    .select('*')
    .eq('activo', true);
  if (error) throw new Error(`getSellers: ${error.message}`);
  return data;
}

/**
 * Returns all current items for a seller stored in the DB.
 * Since the table uses upsert (one row per item), this is the "last known state".
 */
async function getCurrentDbItems(sellerId) {
  const { data, error } = await supabase
    .from('snapshots')
    .select('*')
    .eq('seller_id', String(sellerId));

  if (error) throw new Error(`getCurrentDbItems: ${error.message}`);
  return data ?? [];
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

  // Upsert: unique constraint on meli_item_id → update on conflict
  const { error } = await supabase
    .from('snapshots')
    .upsert(rows, { onConflict: 'meli_item_id' });
  if (error) throw new Error(`saveSnapshot: ${error.message}`);
  return { count: rows.length, timestamp: now };
}

// ─── Google Sheets ────────────────────────────────────────────────────────────

let _sheets = null;

async function getSheetsClient() {
  if (_sheets) return _sheets;
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDENTIALS_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  _sheets = google.sheets({ version: 'v4', auth });
  return _sheets;
}

async function ensureSheetHeader(sheets) {
  const header = [['Fecha', 'Vendedor ID', 'Nickname', 'Item ID', 'Título', 'Precio', 'Estado']];
  // Check if A1 already has a value
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'A1',
  });
  if (!res.data.values || res.data.values[0]?.[0] !== 'Fecha') {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'A1',
      valueInputOption: 'RAW',
      requestBody: { values: header },
    });
    console.log('  [Sheets] Header escrito.');
  }
}

async function appendDisappearedToSheet(sheets, seller, disappearedItems) {
  if (disappearedItems.length === 0) return;

  const fecha = new Date().toLocaleString('es-UY', { timeZone: 'America/Montevideo' });
  const sellerId = String(seller.seller_id);
  const nickname = seller.nickname ?? seller.nombre_real ?? sellerId;

  const rows = disappearedItems.map((item) => [
    fecha,
    sellerId,
    nickname,
    item.meli_item_id ?? item.id,
    item.title ?? '(sin título)',
    item.price ?? '',
    'desaparecido (venta probable)',
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: 'A1',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows },
  });

  console.log(`  [Sheets] ${rows.length} fila(s) escritas para items desaparecidos.`);
}

// ─── Diff logic ───────────────────────────────────────────────────────────────

function detectChanges(prevItems, currItems) {
  const prevMap = new Map(prevItems.map((i) => [i.meli_item_id ?? i.id, i]));
  const currMap = new Map(currItems.map((i) => [i.id, i]));

  const disappeared = prevItems.filter((i) => !currMap.has(i.meli_item_id ?? i.id));
  const appeared    = currItems.filter((i) => !prevMap.has(i.id));
  const priceChanged = currItems.filter((i) => {
    const prev = prevMap.get(i.id);
    return prev && prev.price !== i.price;
  });

  return { disappeared, appeared, priceChanged };
}

// ─── Process one seller ───────────────────────────────────────────────────────

async function processSeller(seller, sheets) {
  const sellerId = seller.seller_id;
  const name = seller.nombre_real ?? seller.nickname ?? sellerId;
  const url = `https://listado.mercadolibre.com.uy/_CustId_${sellerId}`;

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Seller: ${sellerId} — ${name}`);
  console.log(`URL: ${url}`);

  // Scrape
  console.log('  Scraping...');
  const { status, html } = await scrape(url);
  console.log(`  HTTP status: ${status}  |  HTML: ${html.length.toLocaleString()} chars`);

  const items = extractItems(html);
  console.log(`  Items extraídos: ${items.length}`);

  if (items.length === 0) {
    console.warn('  ⚠ No se extrajeron items.');
    return;
  }

  console.log('  Muestra (primeros 3):');
  items.slice(0, 3).forEach((i) =>
    console.log(`    [${i.id}] ${i.title ?? '(sin título)'} — ${i.currency} ${i.price}`)
  );

  // Previous state from DB (before this run's upsert)
  const prevItems = await getCurrentDbItems(sellerId);
  const prevTs = prevItems.length > 0
    ? prevItems.reduce((max, i) => i.timestamp > max ? i.timestamp : max, prevItems[0].timestamp)
    : null;
  console.log(`  Estado anterior en BD: ${prevTs ? `${prevItems.length} items (último: ${prevTs})` : 'ninguno'}`);

  // Diff
  const { disappeared, appeared, priceChanged } = detectChanges(prevItems, items);

  if (disappeared.length) {
    console.log(`  🔴 Items DESAPARECIDOS (${disappeared.length}):`);
    disappeared.forEach((i) =>
      console.log(`     - [${i.meli_item_id}] ${i.title} — ${i.price}`)
    );
    // Write to Google Sheets
    try {
      await appendDisappearedToSheet(sheets, seller, disappeared);
    } catch (err) {
      console.error(`  [Sheets] Error escribiendo: ${err.message}`);
    }
  }
  if (appeared.length) {
    console.log(`  🟢 Items NUEVOS (${appeared.length}):`);
    appeared.forEach((i) =>
      console.log(`     + [${i.id}] ${i.title} — ${i.currency} ${i.price}`)
    );
  }
  if (priceChanged.length) {
    console.log(`  🟡 Cambios de PRECIO (${priceChanged.length}):`);
    priceChanged.forEach((i) => {
      const p = prevItems.find((x) => (x.meli_item_id ?? x.id) === i.id);
      console.log(`     ~ [${i.id}] ${i.title}: ${p?.price} → ${i.price} ${i.currency}`);
    });
  }
  if (!disappeared.length && !appeared.length && !priceChanged.length && prevTs) {
    console.log('  ✔ Sin cambios respecto al snapshot anterior.');
  }

  // Save
  const saved = await saveSnapshot(sellerId, items);
  console.log(`  Snapshot guardado: ${saved.count} items @ ${saved.timestamp}`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('MLU Monitor — inicio');
  console.log(new Date().toISOString());

  // Init Google Sheets
  let sheets;
  try {
    sheets = await getSheetsClient();
    await ensureSheetHeader(sheets);
    console.log('Google Sheets: conectado ✓');
  } catch (err) {
    console.error(`Google Sheets: error de conexión — ${err.message}`);
    sheets = null;
  }

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
      await processSeller(seller, sheets);
    } catch (err) {
      console.error(`  Error procesando seller ${seller.seller_id}: ${err.message}`);
    }
  }

  console.log('\nMLU Monitor — fin');
}

main().catch(console.error);
