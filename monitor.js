const https = require('https');
const fs    = require('fs');
const { createClient } = require('@supabase/supabase-js');

// ─── Verificar secrets ─────────────────────────────────────────────────────
['SUPABASE_URL','SUPABASE_KEY','SCRAPFLY_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase     = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const SCRAPFLY_KEY = process.env.SCRAPFLY_KEY;

const RUN_ID     = `run_${Date.now()}`;
const CHECKED_AT = new Date().toISOString();

console.log(`\n${'═'.repeat(70)}`);
console.log(`🚀 MONITOR MEJORADO - EJECUTADO`);
console.log(`═'.repeat(70)}`);
console.log(`RUN_ID:     ${RUN_ID}`);
console.log(`CHECKED_AT: ${CHECKED_AT}\n`);

// ─── HTTP helper ───────────────────────────────────────────────────────────
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

// ─── Scrapfly con múltiples estrategias ────────────────────────────────────
async function scrapeSeller(sellerId) {
  const targetUrl = encodeURIComponent(`https://listado.mercadolibre.com.uy/_CustId_${sellerId}`);
  const path = `/scrape?key=${SCRAPFLY_KEY}&url=${targetUrl}&asp=true&render_js=true&rendering_wait=5000&country=uy`;

  console.log(`  📡 Scrapeando Scrapfly para seller ${sellerId}...`);
  
  const res = await httpGet('api.scrapfly.io', path);
  if (res.status !== 200) {
    throw new Error(`Scrapfly HTTP ${res.status}`);
  }

  const raw = res.body;

  // Extraer HTML de Scrapfly
  const contentIdx = raw.indexOf('"content"');
  if (contentIdx < 0) throw new Error('Scrapfly: no hay campo content');
  
  const contentStart = contentIdx + '"content":"'.length;
  const endMarker = raw.indexOf('","format":', contentStart);
  if (endMarker < 0) throw new Error('Scrapfly: no encontre cierre de content');

  let html = raw.slice(contentStart, endMarker)
    .replace(/\\u([\\dA-Fa-f]{4})/g, (_, c) => String.fromCharCode(parseInt(c, 16)))
    .replace(/\\\"/g, '"').replace(/\\\\\\\\/g, '\\').replace(/\\n/g, '\\n').replace(/\\r/g, '');

  if (html.includes('account-verification') || html.length < 5000) {
    throw new Error('Scrapfly: pagina bloqueada o vacia');
  }

  console.log(`  ✅ HTML recibido: ${html.length} bytes`);

  // Extraer IDs de items
  const mlRegex = /MLU\d+/g;
  const matches = html.match(mlRegex) || [];
  const itemIds = [...new Set(matches)];

  if (!itemIds.length) throw new Error('0 items encontrados');

  console.log(`  ✅ Items encontrados: ${itemIds.length}`);

  // Para cada item, obtener datos COMPLETOS de la API de MELI
  const items = [];
  for (const itemId of itemIds.slice(0, 50)) {
    try {
      const itemRes = await httpGet('api.mercadolibre.com.uy', `/items/${itemId}`);
      if (itemRes.status === 200) {
        const itemData = JSON.parse(itemRes.body);
        
        items.push({
          id: itemId,
          title: itemData.title || 'Sin título',
          price: itemData.price || 0,
          currency: itemData.currency_id || 'UYU',
          status: itemData.status || 'active',
          available_quantity: itemData.available_quantity || 0,
          sold_quantity: itemData.sold_quantity || 0,
          url: itemData.permalink || '',
          thumbnail: itemData.thumbnail || ''
        });
      }
    } catch (e) {
      console.warn(`  ⚠️  No se pudo obtener ${itemId}: ${e.message}`);
    }
  }

  return items;
}

// ─── Obtener vendedores activos ────────────────────────────────────────────
async function getActiveSellers() {
  const { data, error } = await supabase.from('sellers').select('*').eq('activo', true);
  if (error) throw new Error(`getSellers: ${error.message}`);
  return data;
}

// ─── Guardar snapshot ──────────────────────────────────────────────────────
async function saveSnapshot(sellerId, items) {
  const rows = items.map(item => ({
    seller_id: String(sellerId),
    meli_item_id: item.id,
    item_id: item.id,
    title: item.title,
    price: item.price,
    currency: item.currency,
    status: item.status,
    available_quantity: item.available_quantity,
    sold_quantity: item.sold_quantity,
    url: item.url,
    thumbnail: item.thumbnail,
    run_id: RUN_ID,
    checked_at: CHECKED_AT,
    timestamp: CHECKED_AT,
  }));

  const { error } = await supabase.from('snapshots').insert(rows);
  if (error) {
    if (error.message.includes('duplicate')) {
      let inserted = 0;
      for (const row of rows) {
        const { error: e2 } = await supabase.from('snapshots').insert([row]);
        if (!e2) inserted++;
      }
      return inserted;
    }
    throw new Error(`saveSnapshot: ${error.message}`);
  }
  
  return rows.length;
}

// ─── Procesar seller ───────────────────────────────────────────────────────
async function processSeller(seller) {
  const sellerId = seller.seller_id;
  const name = seller.nombre_real || seller.nickname || String(sellerId);
  console.log(`\n${'─'.repeat(70)}\n🏪 ${name} (ID: ${sellerId})`);

  let status = 'failed', errorMessage = '', itemsFound = 0;
  try {
    const items = await scrapeSeller(sellerId);
    itemsFound = items.length;
    
    if (!items.length) throw new Error('0 items');
    
    console.log(`\n  📊 DATOS EXTRAÍDOS:`);
    items.slice(0, 3).forEach(i => {
      console.log(`    [${i.id}] ${i.title.substring(0, 50)} | $${i.price} | Stock: ${i.available_quantity} | Vendidos: ${i.sold_quantity}`);
    });
    
    await saveSnapshot(sellerId, items);
    console.log(`  ✅ Guardados ${itemsFound} snapshots`);
    status = 'ok';
    
  } catch (err) {
    errorMessage = err.message;
    console.error(`  ❌ ERROR: ${errorMessage}`);
  }

  return { sellerId, name, status, errorMessage, itemsFound };
}

// ─── Main ─────────────────────────────────────────────────────────────────
(async () => {
  const results = [];
  let totalItems = 0;
  let totalErrors = 0;

  try {
    const sellers = await getActiveSellers();
    console.log(`\n📋 Procesando ${sellers.length} vendedores...\n`);

    for (const seller of sellers) {
      const result = await processSeller(seller);
      results.push(result);
      totalItems += result.itemsFound;
      if (result.status === 'failed') totalErrors++;
    }

    // Guardar resumen de ejecución
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`✅ MONITOR COMPLETADO`);
    console.log(`═'.repeat(70)}`);
    
    const summary = {
      run_id: RUN_ID,
      executed_at: CHECKED_AT,
      items_processed: totalItems,
      sellers_total: sellers.length,
      sellers_success: sellers.length - totalErrors,
      sellers_failed: totalErrors,
      status: totalErrors === 0 ? 'success' : 'partial',
      message: `Procesados ${totalItems} items de ${sellers.length} vendedores`
    };

    console.log(`\n📊 RESUMEN:`);
    console.log(`   Items procesados: ${summary.items_processed}`);
    console.log(`   Vendedores OK: ${summary.sellers_success}/${summary.sellers_total}`);
    console.log(`   Status: ${summary.status}`);

    // Guardar en tabla execution_logs
    const { error: logError } = await supabase
      .from('execution_logs')
      .insert([summary]);
    
    if (logError) {
      console.warn(`⚠️  Error guardando log: ${logError.message}`);
    } else {
      console.log(`\n✅ Ejecución guardada en execution_logs`);
    }

    // Si hay muchos errores, alertar
    if (totalErrors > sellers.length * 0.1) {
      console.warn(`\n⚠️  ALERTA: ${totalErrors} vendedores fallaron (${(totalErrors/sellers.length*100).toFixed(1)}%)`);
    }

    console.log(`\n${'═'.repeat(70)}\n`);

  } catch (err) {
    console.error('\n❌ FATAL:', err.message);
    process.exit(1);
  }
})();
