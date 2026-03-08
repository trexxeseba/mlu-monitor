const { createClient } = require('@supabase/supabase-js');
const { SheetWriter } = require('./sheetWriter');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const SHEET_ID = process.env.SHEET_ID || '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';

// Parse Google Credentials from env
function getGoogleCreds() {
  const credsJson = process.env.GOOGLE_CREDENTIALS;
  if (!credsJson) {
    throw new Error('GOOGLE_CREDENTIALS environment variable is required');
  }
  
  try {
    return JSON.parse(credsJson);
  } catch (e) {
    throw new Error(`Failed to parse GOOGLE_CREDENTIALS: ${e.message}`);
  }
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function syncData() {
  console.log('📊 MLU Sheets Sync — inicio');
  console.log(new Date().toISOString());

  try {
    // Initialize SheetWriter
    console.log('\n[→] Inicializando Google Sheets...');
    const creds = getGoogleCreds();
    const writer = new SheetWriter({
      spreadsheetId: SHEET_ID,
      clientEmail: creds.client_email,
      privateKey: creds.private_key
    });
    await writer.init();

    // Fetch data from Supabase
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

    // Build RESUMEN data
    console.log('\n[→] Escribiendo a Google Sheets...');
    
    const resumenHeader = ['VENDEDOR ID', 'NICKNAME', 'TOTAL ITEMS', 'ITEMS VENDIDOS', '% VENTA', 'ÚLTIMO UPDATE'];
    const resumenRows = sellers.map(seller => {
      const sellerSnapshots = allSnapshots.filter(s => s.seller_id === seller.id);
      const totalItems = sellerSnapshots.length;
      const soldItems = sellerSnapshots.filter(s => s.status === 'sold').length;
      const percentage = totalItems > 0 ? ((soldItems / totalItems) * 100).toFixed(1) : 0;
      const lastUpdate = sellerSnapshots.length > 0
        ? new Date(Math.max(...sellerSnapshots.map(s => new Date(s.timestamp)))).toLocaleString('es-UY', { timeZone: 'America/Montevideo' })
        : '—';
      
      return [seller.seller_id, seller.nombre_real || seller.nickname || '—', totalItems, soldItems, `${percentage}%`, lastUpdate];
    });

    await writer.overwriteSheet('RESUMEN', resumenHeader, resumenRows);

    // Build PRODUCTOS data
    const productosHeader = ['SELLER ID', 'ITEM ID', 'TÍTULO', 'PRECIO', 'ESTADO', 'LAST SEEN'];
    const productosRows = allSnapshots.slice(0, 1000).map(p => [
      p.seller_id,
      p.meli_item_id || p.item_id || '—',
      (p.title || '—').substring(0, 60),
      p.price || '—',
      p.status || 'active',
      p.timestamp ? new Date(p.timestamp).toLocaleDateString('es-UY') : '—'
    ]);

    await writer.overwriteSheet('PRODUCTOS', productosHeader, productosRows);

    // Build TIMELINE data
    const timelineHeader = ['FECHA', 'SELLER ID', 'TIPO', 'ITEM ID', 'TÍTULO', 'PRECIO'];
    const timelineRows = allSnapshots.slice(0, 500).map(c => [
      c.timestamp ? new Date(c.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' }) : '—',
      c.seller_id,
      c.change_type || 'unknown',
      c.meli_item_id || c.item_id || '—',
      (c.title || '—').substring(0, 60),
      c.price || '—'
    ]);

    await writer.overwriteSheet('TIMELINE', timelineHeader, timelineRows);

    console.log('\n[✓] MLU Sheets Sync — completado exitosamente');
    process.exit(0);

  } catch (err) {
    console.error(`\n[✗] Error fatal: ${err.message}`);
    if (err.stack) {
      console.error(err.stack);
    }
    process.exit(1);
  }
}

syncData();
