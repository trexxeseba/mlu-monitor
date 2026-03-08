const { createClient } = require('@supabase/supabase-js');
const https = require('https');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';
const SHEET_ID = process.env.SHEET_ID || '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';

// Parse Google Credentials desde JSON string (variable de entorno)
let ACCESS_TOKEN = null;

async function getGoogleAccessToken() {
  if (ACCESS_TOKEN) return ACCESS_TOKEN;

  const credsJson = process.env.GOOGLE_CREDENTIALS;
  if (!credsJson) {
    throw new Error('GOOGLE_CREDENTIALS env var not set');
  }

  const creds = JSON.parse(credsJson);
  const jwt = Buffer.from(JSON.stringify({
    iss: creds.client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    exp: Math.floor(Date.now() / 1000) + 3600,
    iat: Math.floor(Date.now() / 1000)
  })).toString('base64');

  const header = Buffer.from(JSON.stringify({
    alg: 'RS256',
    typ: 'JWT',
    kid: creds.private_key_id
  })).toString('base64');

  // Sign con private key (simplificado - usar librería crypto en prod)
  const crypto = require('crypto');
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(`${header}.${jwt}`);
  const signature = sign.sign(creds.private_key, 'base64');
  const token = `${header}.${jwt}.${signature}`;

  return new Promise((resolve, reject) => {
    const postData = `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${token}`;
    
    const options = {
      hostname: 'oauth2.googleapis.com',
      port: 443,
      path: '/token',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData)
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          ACCESS_TOKEN = parsed.access_token;
          resolve(ACCESS_TOKEN);
        } catch (e) {
          reject(new Error(`Failed to parse token: ${data}`));
        }
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

// Escritura via REST API puro - 100% confiable
async function writeToSheets(range, values) {
  const token = await getGoogleAccessToken();

  const payload = {
    values: values
  };

  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const options = {
      hostname: 'sheets.googleapis.com',
      port: 443,
      path: `/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`,
      method: 'PUT',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(JSON.parse(data));
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${data}`));
        }
      });
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function syncData() {
  console.log('📊 MLU Sheets Sync (REST API) — inicio');
  console.log(new Date().toISOString());

  try {
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

    // Write to Sheets via REST API
    console.log('\n[→] Escribiendo a Google Sheets (REST API)...');

    // RESUMEN
    const resumenHeader = [['VENDEDOR ID', 'NICKNAME', 'TOTAL ITEMS', 'ITEMS VENDIDOS', '% VENTA', 'ÚLTIMO UPDATE']];
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

    await writeToSheets('RESUMEN!A1', resumenHeader);
    if (resumenRows.length > 0) {
      await writeToSheets('RESUMEN!A2', resumenRows);
    }
    console.log(`  [✓] RESUMEN: ${resumenRows.length} vendedores`);

    // PRODUCTOS
    const productosHeader = [['SELLER ID', 'ITEM ID', 'TÍTULO', 'PRECIO', 'ESTADO', 'LAST SEEN']];
    const productosRows = allSnapshots.slice(0, 1000).map(p => [
      p.seller_id,
      p.meli_item_id || p.item_id || '—',
      (p.title || '—').substring(0, 60),
      p.price || '—',
      p.status || 'active',
      p.timestamp ? new Date(p.timestamp).toLocaleDateString('es-UY') : '—'
    ]);

    await writeToSheets('PRODUCTOS!A1', productosHeader);
    if (productosRows.length > 0) {
      await writeToSheets('PRODUCTOS!A2', productosRows);
    }
    console.log(`  [✓] PRODUCTOS: ${productosRows.length} items`);

    // TIMELINE
    const timelineHeader = [['FECHA', 'SELLER ID', 'TIPO', 'ITEM ID', 'TÍTULO', 'PRECIO']];
    const timelineRows = allSnapshots.slice(0, 500).map(c => [
      c.timestamp ? new Date(c.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' }) : '—',
      c.seller_id,
      c.change_type || 'unknown',
      c.meli_item_id || c.item_id || '—',
      (c.title || '—').substring(0, 60),
      c.price || '—'
    ]);

    await writeToSheets('TIMELINE!A1', timelineHeader);
    if (timelineRows.length > 0) {
      await writeToSheets('TIMELINE!A2', timelineRows);
    }
    console.log(`  [✓] TIMELINE: ${timelineRows.length} cambios`);

    console.log('\n[✓] MLU Sheets Sync — completado exitosamente');
    process.exit(0);
  } catch (err) {
    console.error(`\n[✗] Error: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  }
}

syncData();
