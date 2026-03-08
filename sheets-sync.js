const { createClient } = require('@supabase/supabase-js');
const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const SHEET_ID = process.env.SHEET_ID || '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';

let sheetDoc = null;

// Validación estricta de datos
function assert2D(values, name = 'values') {
  if (!Array.isArray(values)) {
    throw new Error(`${name} debe ser un array`);
  }
  if (values.length === 0) {
    throw new Error(`${name} no puede estar vacío`);
  }
  if (!values.every(row => Array.isArray(row))) {
    throw new Error(`${name} debe ser array bidimensional [[...], [...]]`);
  }
}

// Retry con backoff exponencial
async function withRetry(fn, maxRetries = 4, label = 'Operation') {
  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      const status = err?.code || err?.response?.status || err?.status;
      const retriable = [429, 500, 502, 503, 504].includes(status);

      console.log(`  [!] ${label} - Attempt ${attempt + 1}/${maxRetries + 1} failed: ${err.message}`);

      if (!retriable || attempt === maxRetries) {
        throw err;
      }

      const delayMs = Math.min(1000 * Math.pow(2, attempt), 8000);
      console.log(`      Retrying in ${delayMs}ms...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }

  throw lastError;
}

// Inicializar documento
async function getSheetDoc() {
  if (sheetDoc) return sheetDoc;

  const credsJson = process.env.GOOGLE_CREDENTIALS;
  if (!credsJson) {
    throw new Error('GOOGLE_CREDENTIALS env var not set');
  }

  const creds = JSON.parse(credsJson);
  
  const serviceAccountAuth = new JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  sheetDoc = new GoogleSpreadsheet(SHEET_ID, serviceAccountAuth);
  await withRetry(() => sheetDoc.loadInfo(), 3, 'Load Sheet Info');
  
  return sheetDoc;
}

// Encapsulación única de escritura
const SheetWriter = {
  async ensureSheets(sheetNames) {
    const doc = await getSheetDoc();
    const existingSheets = doc.sheetsByTitle;

    for (const name of sheetNames) {
      if (!existingSheets[name]) {
        await withRetry(
          () => doc.addSheet({ title: name }),
          3,
          `Create sheet "${name}"`
        );
        console.log(`  [+] Sheet creado: ${name}`);
      }
    }
  },

  async updateBlock(sheetName, values) {
    assert2D(values);
    
    const doc = await getSheetDoc();
    const sheet = doc.sheetsByTitle[sheetName];
    
    if (!sheet) {
      throw new Error(`Sheet "${sheetName}" no existe`);
    }

    return withRetry(async () => {
      await sheet.clear();
      await sheet.addRows(values);
      return { updatedCells: values.length * values[0].length };
    }, 3, `Update ${sheetName}`);
  },

  async appendRows(sheetName, values) {
    assert2D(values);
    
    const doc = await getSheetDoc();
    const sheet = doc.sheetsByTitle[sheetName];
    
    if (!sheet) {
      throw new Error(`Sheet "${sheetName}" no existe`);
    }

    return withRetry(async () => {
      await sheet.addRows(values);
      return { appendedRows: values.length };
    }, 3, `Append to ${sheetName}`);
  }
};

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function syncData() {
  console.log('📊 MLU Sheets Sync (google-spreadsheet) — inicio');
  console.log(new Date().toISOString());

  try {
    // Fetch from Supabase
    console.log('\n[→] Leyendo datos de Supabase...');
    
    const { data: sellers, error: sellersErr } = await supabase
      .from('sellers')
      .select('id, seller_id, nombre_real, nickname, activo');
    
    if (sellersErr) throw new Error(`sellers query failed: ${sellersErr.message}`);
    console.log(`  [✓] ${sellers?.length || 0} vendedores`);

    const { data: allSnapshots, error: snapshotsErr } = await supabase
      .from('snapshots')
      .select('*')
      .order('timestamp', { ascending: false });
    
    if (snapshotsErr) throw new Error(`snapshots query failed: ${snapshotsErr.message}`);
    console.log(`  [✓] ${allSnapshots?.length || 0} snapshots totales`);

    // Ensure sheets exist
    console.log('\n[→] Verificando hojas en Google Sheets...');
    await SheetWriter.ensureSheets(['RESUMEN', 'PRODUCTOS', 'TIMELINE']);

    // Write to Sheets using SheetWriter (single encapsulation point)
    console.log('\n[→] Escribiendo a Google Sheets...');

    // RESUMEN
    const resumenData = [['VENDEDOR ID', 'NICKNAME', 'TOTAL ITEMS', 'ITEMS VENDIDOS', '% VENTA', 'ÚLTIMO UPDATE']];
    sellers.forEach(seller => {
      const sellerSnapshots = allSnapshots.filter(s => s.seller_id === seller.id);
      const totalItems = sellerSnapshots.length;
      const soldItems = sellerSnapshots.filter(s => s.status === 'sold').length;
      const percentage = totalItems > 0 ? ((soldItems / totalItems) * 100).toFixed(1) : 0;
      const lastUpdate = sellerSnapshots.length > 0
        ? new Date(Math.max(...sellerSnapshots.map(s => new Date(s.timestamp)))).toLocaleString('es-UY', { timeZone: 'America/Montevideo' })
        : '—';
      
      resumenData.push([seller.seller_id, seller.nombre_real || seller.nickname || '—', totalItems, soldItems, `${percentage}%`, lastUpdate]);
    });

    await SheetWriter.updateBlock('RESUMEN', resumenData);
    console.log(`  [✓] RESUMEN: ${resumenData.length - 1} vendedores`);

    // PRODUCTOS
    const productosData = [['SELLER ID', 'ITEM ID', 'TÍTULO', 'PRECIO', 'ESTADO', 'LAST SEEN']];
    allSnapshots.slice(0, 1000).forEach(p => {
      productosData.push([
        p.seller_id,
        p.meli_item_id || p.item_id || '—',
        (p.title || '—').substring(0, 60),
        p.price || '—',
        p.status || 'active',
        p.timestamp ? new Date(p.timestamp).toLocaleDateString('es-UY') : '—'
      ]);
    });

    await SheetWriter.updateBlock('PRODUCTOS', productosData);
    console.log(`  [✓] PRODUCTOS: ${productosData.length - 1} items`);

    // TIMELINE
    const timelineData = [['FECHA', 'SELLER ID', 'TIPO', 'ITEM ID', 'TÍTULO', 'PRECIO']];
    allSnapshots.slice(0, 500).forEach(c => {
      timelineData.push([
        c.timestamp ? new Date(c.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' }) : '—',
        c.seller_id,
        c.change_type || 'unknown',
        c.meli_item_id || c.item_id || '—',
        (c.title || '—').substring(0, 60),
        c.price || '—'
      ]);
    });

    await SheetWriter.updateBlock('TIMELINE', timelineData);
    console.log(`  [✓] TIMELINE: ${timelineData.length - 1} cambios`);

    console.log('\n[✓] MLU Sheets Sync — completado exitosamente');
    process.exit(0);
  } catch (err) {
    console.error(`\n[✗] Error: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  }
}

syncData();
