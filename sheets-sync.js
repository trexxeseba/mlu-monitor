const { createClient } = require('@supabase/supabase-js');
const { google } = require('googleapis');
const path = require('path');
const fs = require('fs');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const SHEET_ID = process.env.SHEET_ID || '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';

// Credenciales de Google
if (process.env.GOOGLE_CREDENTIALS) {
  const tmpPath = '/tmp/credentials.json';
  fs.writeFileSync(tmpPath, process.env.GOOGLE_CREDENTIALS);
}
const CREDENTIALS_PATH = process.env.GOOGLE_CREDENTIALS 
  ? '/tmp/credentials.json'
  : path.join(__dirname, 'clauditaaa-dbcde137b8d8.json');

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

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

async function ensureSheets(sheets) {
  try {
    const res = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const existing = res.data.sheets.map(s => s.properties.title);

    const needed = ['RESUMEN', 'PRODUCTOS', 'TIMELINE'];
    for (const name of needed) {
      if (!existing.includes(name)) {
        await sheets.spreadsheets.batchUpdate({
          spreadsheetId: SHEET_ID,
          requestBody: {
            requests: [{
              addSheet: { properties: { title: name } }
            }]
          }
        });
        console.log(`  [+] Sheet creado: ${name}`);
      }
    }
  } catch (err) {
    console.error(`  [!] Error en ensureSheets: ${err.message}`);
  }
}

async function clearSheets(sheets) {
  const sheetNames = ['RESUMEN', 'PRODUCTOS', 'TIMELINE'];
  for (const name of sheetNames) {
    try {
      await sheets.spreadsheets.values.clear({
        spreadsheetId: SHEET_ID,
        range: `${name}!A:Z`,
      });
    } catch (e) {
      // ignorar si no existe
    }
  }
}

async function writeResumen(sheets, sellers, allSnapshots) {
  const header = [['VENDEDOR ID', 'NICKNAME', 'TOTAL ITEMS', 'ITEMS VENDIDOS', '% VENTA', 'ÚLTIMO UPDATE']];
  
  const rows = sellers.map(seller => {
    const sellerSnapshots = allSnapshots.filter(s => s.seller_id === seller.id);
    const totalItems = sellerSnapshots.length;
    const soldItems = sellerSnapshots.filter(s => s.status === 'sold').length;
    const percentage = totalItems > 0 ? ((soldItems / totalItems) * 100).toFixed(1) : 0;
    const lastUpdate = sellerSnapshots.length > 0
      ? new Date(Math.max(...sellerSnapshots.map(s => new Date(s.timestamp)))).toLocaleString('es-UY', { timeZone: 'America/Montevideo' })
      : '—';
    
    return [
      seller.seller_id,
      seller.nombre_real || seller.nickname || '—',
      totalItems,
      soldItems,
      `${percentage}%`,
      lastUpdate
    ];
  });

  const data = [{ range: 'RESUMEN!A1', values: header }];
  if (rows.length > 0) {
    data.push({ range: 'RESUMEN!A2', values: rows });
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    valueInputOption: 'RAW',
    requestBody: { data }
  });
  console.log(`  [✓] RESUMEN: ${rows.length} vendedores`);
}

async function writeProductos(sheets, products) {
  const header = [['SELLER ID', 'ITEM ID', 'TÍTULO', 'PRECIO', 'ESTADO', 'LAST SEEN']];
  
  const rows = products.slice(0, 1000).map(p => [
    p.seller_id,
    p.meli_item_id || p.item_id || '—',
    (p.title || '—').substring(0, 60),
    p.price || '—',
    p.status || 'active',
    p.timestamp ? new Date(p.timestamp).toLocaleDateString('es-UY') : '—'
  ]);

  const data = [{ range: 'PRODUCTOS!A1', values: header }];
  if (rows.length > 0) {
    data.push({ range: 'PRODUCTOS!A2', values: rows });
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    valueInputOption: 'RAW',
    requestBody: { data }
  });
  console.log(`  [✓] PRODUCTOS: ${rows.length} items`);
}

async function writeTimeline(sheets, changes) {
  const header = [['FECHA', 'SELLER ID', 'TIPO', 'ITEM ID', 'TÍTULO', 'PRECIO']];
  
  const rows = changes.slice(0, 500).map(c => [
    c.timestamp ? new Date(c.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' }) : '—',
    c.seller_id,
    c.change_type || 'unknown',
    c.meli_item_id || c.item_id || '—',
    (c.title || '—').substring(0, 60),
    c.price || '—'
  ]);

  const data = [{ range: 'TIMELINE!A1', values: header }];
  if (rows.length > 0) {
    data.push({ range: 'TIMELINE!A2', values: rows });
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    valueInputOption: 'RAW',
    requestBody: { data }
  });
  console.log(`  [✓] TIMELINE: ${rows.length} cambios recientes`);
}

async function main() {
  console.log('MLU Sheets Sync — inicio');
  console.log(new Date().toISOString());

  try {
    // Get Sheets client
    const sheets = await getSheetsClient();
    console.log('[✓] Google Sheets conectado');

    // Ensure sheets exist
    await ensureSheets(sheets);
    
    // Clear existing
    await clearSheets(sheets);

    // Fetch data from Supabase - SIN RELACIONES
    console.log('\n[→] Leyendo datos de Supabase...');
    
    // Sellers
    const { data: sellers, error: sellersErr } = await supabase
      .from('sellers')
      .select('id, seller_id, nombre_real, nickname, activo');
    
    if (sellersErr) throw new Error(`sellers query failed: ${sellersErr.message}`);
    console.log(`  [✓] ${sellers?.length || 0} vendedores`);

    // Snapshots (todos, sin relación)
    const { data: allSnapshots, error: snapshotsErr } = await supabase
      .from('snapshots')
      .select('*')
      .order('timestamp', { ascending: false });
    
    if (snapshotsErr) throw new Error(`snapshots query failed: ${snapshotsErr.message}`);
    console.log(`  [✓] ${allSnapshots?.length || 0} snapshots totales`);

    // Write to Sheets
    console.log('\n[→] Escribiendo a Google Sheets...');
    await writeResumen(sheets, sellers || [], allSnapshots || []);
    await writeProductos(sheets, allSnapshots || []);
    await writeTimeline(sheets, allSnapshots || []);

    console.log('\n[✓] MLU Sheets Sync — completado');
    process.exit(0);
  } catch (err) {
    console.error(`[✗] Error: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  }
}

main();
