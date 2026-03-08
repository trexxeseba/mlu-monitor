const { createClient } = require('@supabase/supabase-js');
const { google } = require('googleapis');
const path = require('path');
const fs = require('fs');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const SHEET_ID = process.env.SHEET_ID || '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';

// Si viene desde GitHub Actions, escribir credenciales a archivo temporal
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

async function clearSheets(sheets) {
  const sheetNames = ['RESUMEN', 'PRODUCTOS', 'TIMELINE'];
  for (const name of sheetNames) {
    try {
      await sheets.spreadsheets.values.clear({
        spreadsheetId: SHEET_ID,
        range: `${name}!A:Z`,
      });
    } catch (e) {
      console.log(`  [${name}] Sheet creada o no existía (first time)`);
    }
  }
}

async function ensureSheets(sheets) {
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
}

async function writeResumen(sheets, sellers) {
  const header = [['VENDEDOR ID', 'NICKNAME', 'TOTAL ITEMS', 'ITEMS VENDIDOS', '% VENTA', 'ÚLTIMO UPDATE']];
  
  const rows = sellers.map(seller => {
    const soldItems = seller.sold_count || 0;
    const totalItems = seller.total_count || 0;
    const percentage = totalItems > 0 ? ((soldItems / totalItems) * 100).toFixed(1) : 0;
    const lastUpdate = seller.last_snapshot_at 
      ? new Date(seller.last_snapshot_at).toLocaleString('es-UY', { timeZone: 'America/Montevideo' })
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

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      data: [
        { range: 'RESUMEN!A1', values: header },
        { range: 'RESUMEN!A2', values: rows }
      ]
    }
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

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      data: [
        { range: 'PRODUCTOS!A1', values: header },
        { range: 'PRODUCTOS!A2', values: rows }
      ]
    }
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

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      data: [
        { range: 'TIMELINE!A1', values: header },
        { range: 'TIMELINE!A2', values: rows }
      ]
    }
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

    // Fetch data from Supabase
    console.log('\n[→] Leyendo datos de Supabase...');
    
    const { data: sellers, error: sellersErr } = await supabase
      .from('sellers')
      .select('*, snapshots(count)', { count: 'exact' })
      .eq('activo', true);
    
    if (sellersErr) throw sellersErr;
    console.log(`  [✓] ${sellers.length} vendedores activos`);

    // Enrich sellers with stats
    const enrichedSellers = await Promise.all(sellers.map(async (seller) => {
      const { data: snapshots } = await supabase
        .from('snapshots')
        .select('*')
        .eq('seller_id', String(seller.id))
        .order('timestamp', { ascending: false })
        .limit(500);

      const total = snapshots?.length || 0;
      const sold = snapshots?.filter(s => s.status === 'sold').length || 0;
      const lastSnapshot = snapshots?.[0]?.timestamp;

      return {
        ...seller,
        total_count: total,
        sold_count: sold,
        last_snapshot_at: lastSnapshot
      };
    }));

    // Get all products
    const { data: products } = await supabase
      .from('snapshots')
      .select('*')
      .order('timestamp', { ascending: false });
    
    console.log(`  [✓] ${products?.length || 0} productos totales`);

    // Write to Sheets
    console.log('\n[→] Escribiendo a Google Sheets...');
    await writeResumen(sheets, enrichedSellers);
    await writeProductos(sheets, products || []);
    await writeTimeline(sheets, products || []);

    console.log('\n[✓] MLU Sheets Sync — completado');
  } catch (err) {
    console.error(`[✗] Error: ${err.message}`);
    process.exit(1);
  }
}

main().catch(console.error);
