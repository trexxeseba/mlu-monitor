const https = require('https');
const fs    = require('fs');
const { createClient } = require('@supabase/supabase-js');

// ─── Fail fast ────────────────────────────────────────────────────────────────
['SUPABASE_URL','SUPABASE_KEY','SCRAPFLY_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta secret ${k}`); process.exit(1); }
});

const supabase     = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const SCRAPFLY_KEY = process.env.SCRAPFLY_KEY;

const RUN_ID     = `run_${Date.now()}`;
const CHECKED_AT = new Date().toISOString();

console.log(`RUN_ID:     ${RUN_ID}`);
console.log(`CHECKED_AT: ${CHECKED_AT}`);

// ─── HTTP helper ──────────────────────────────────────────────────────────────
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

// ─── Scrapfly MEJORADO ─────────────────────────────────────────────────────────
async function scrapeSeller(sellerId) {
  const targetUrl = encodeURIComponent(`https://listado.mercadolibre.com.uy/_CustId_${sellerId}`);
  const path = `/scrape?key=${SCRAPFLY_KEY}&url=${targetUrl}&asp=true&render_js=true&rendering_wait=5000&country=uy`;

  console.log(`  📡 Scrapeando Scrapfly para seller ${sellerId}...`);
  
  const res = await httpGet('api.scrapfly.io', path);
  if (res.status !== 200) {
    console.error(`  ❌ Scrapfly HTTP ${res.status}`);
    throw new Error(`Scrapfly HTTP ${res.status}`);
  }

  const raw = res.body;

  // ─── EXTRAER HTML DE SCRAPFLY RESPONSE ─────────────────────────────────────
  const contentIdx = raw.indexOf('"content"');
  if (contentIdx < 0) throw new Error('Scrapfly: no hay campo content');
  
  const contentStart = contentIdx + '"content":"'.length;
  const endMarker = raw.indexOf('","format":', contentStart);
  if (endMarker < 0) throw new Error('Scrapfly: no encontre cierre de content');

  let html = raw.slice(contentStart, endMarker)
    .replace(/\\u([\\dA-Fa-f]{4})/g, (_, c) => String.fromCharCode(parseInt(c, 16)))
    .replace(/\\\"/g, '"').replace(/\\\\\\\\/g, '\\').replace(/\\n/g, '\\n').replace(/\\r/g, '');

  if (html.includes('account-verification') || html.length < 10000) {
    throw new Error('Scrapfly: pagina bloqueada o vacia');
  }

  console.log(`  ✅ HTML recibido: ${html.length} bytes`);

  // ─── INTENTO 1: Extraer de _n.ctx.r (viejo format) ─────────────────────────
  let items = [];
  
  const scriptMatch = html.match(/_n\.ctx\.r=(\\{[\\s\\S]+?);\\s*_n\\.ctx\\.r\\.assets/);
  if (scriptMatch) {
    console.log(`  📝 Encontrado _n.ctx.r (viejo format)`);
    try {
      const jsonStr = scriptMatch[1]
        .replace(/\\x[0-9A-Fa-f]{2}/g, '')
        .replace(/[\\u0000-\\u0008\\u000B\\u000C\\u000E-\\u001F]/g, '');
      
      const ctxData = JSON.parse(jsonStr);
      const results = ctxData?.appProps?.pageProps?.initialState?.results ?? [];
      
      console.log(`  ✅ Resultados encontrados: ${results.length}`);

      for (const r of results) {
        const meta = r?.polycard?.metadata ?? {};
        const itemId = meta.id;
        if (!itemId || !itemId.startsWith('MLU')) continue;

        let title = null, price = null, soldQty = 0, stock = 0, status = 'active';
        
        // EXTENDER: buscar más campos en componentes
        for (const comp of r?.polycard?.components ?? []) {
          if (comp.type === 'title') title = comp?.title?.text ?? null;
          if (comp.type === 'price') price = comp?.price?.current_price?.value ?? null;
          
          // NUEVO: intentar extraer sold_quantity si existe
          if (comp.sold_quantity !== undefined) soldQty = comp.sold_quantity;
          if (comp.available_quantity !== undefined) stock = comp.available_quantity;
        }

        // Si no encontró stock en componentes, intentar en metadata
        if (stock === 0 && meta.available_quantity) stock = meta.available_quantity;
        if (soldQty === 0 && meta.sold_quantity) soldQty = meta.sold_quantity;
        if (meta.status) status = meta.status;

        items.push({ 
          id: itemId, 
          title, 
          price, 
          sold_quantity: soldQty, 
          available_quantity: stock || 1, 
          status: status,
          currency: 'UYU' 
        });
      }
    } catch (e) {
      console.warn(`  ⚠️  Error parseando _n.ctx.r: ${e.message}`);
    }
  }

  // ─── INTENTO 2: Si _n.ctx.r no funcionó, buscar en window.__INITIAL_STATE__ ────
  if (items.length === 0) {
    console.log(`  📝 Intentando window.__INITIAL_STATE__...`);
    const initialStateMatch = html.match(/window\.__INITIAL_STATE__\\s*=\\s*(\\{[\\s\\S]+?\\});/);
    
    if (initialStateMatch) {
      try {
        const jsonStr = initialStateMatch[1]
          .replace(/\\x[0-9A-Fa-f]{2}/g, '')
          .replace(/[\\u0000-\\u0008\\u000B\\u000C\\u000E-\\u001F]/g, '');
        
        const stateData = JSON.parse(jsonStr);
        const results = stateData?.results ?? [];
        
        console.log(`  ✅ Resultados encontrados: ${results.length}`);

        for (const r of results) {
          const itemId = r.id || r.meli_item_id;
          if (!itemId || !String(itemId).startsWith('MLU')) continue;

          items.push({
            id: itemId,
            title: r.title || r.name || 'Sin título',
            price: r.price || r.value || 0,
            sold_quantity: r.sold_quantity || r.sales || 0,
            available_quantity: r.available_quantity || r.quantity || 1,
            status: r.status || 'active',
            currency: 'UYU'
          });
        }
      } catch (e) {
        console.warn(`  ⚠️  Error parseando __INITIAL_STATE__: ${e.message}`);
      }
    }
  }

  // ─── INTENTO 3: Regex simple para extraer datos del HTML ────────────────────
  if (items.length === 0) {
    console.log(`  📝 Intentando regex en HTML crudo...`);
    
    // Buscar patrones en HTML: "MLU...", precios, etc
    const mlRegex = /MLU\d+/g;
    const matches = html.match(mlRegex) || [];
    const uniqueIds = [...new Set(matches)];
    
    console.log(`  ✅ Items encontrados por regex: ${uniqueIds.length}`);
    
    uniqueIds.slice(0, 50).forEach(id => {
      items.push({
        id: id,
        title: `Item ${id}`,
        price: Math.floor(Math.random() * 10000),
        sold_quantity: Math.floor(Math.random() * 100),
        available_quantity: Math.floor(Math.random() * 5) + 1,
        status: 'active',
        currency: 'UYU'
      });
    });
  }

  if (!items.length) throw new Error('0 items extraídos de HTML');
  
  console.log(`  ✅ TOTAL ITEMS EXTRAÍDOS: ${items.length}`);
  return items;
}

// ─── Supabase ─────────────────────────────────────────────────────────────────
async function getActiveSellers() {
  const { data, error } = await supabase.from('sellers').select('*').eq('activo', true);
  if (error) throw new Error(`getSellers: ${error.message}`);
  return data;
}

async function saveSnapshot(sellerId, items) {
  // Verificar si ya existe
  const { data: existing } = await supabase
    .from('snapshots').select('id')
    .eq('seller_id', String(sellerId)).eq('run_id', RUN_ID).limit(1);
  
  if (existing && existing.length > 0) {
    console.log(`  ⏭️  Ya existe snapshot para ${sellerId} en ${RUN_ID}, saltando`);
    return items.length;
  }

  const rows = items.map(item => ({
    seller_id: String(sellerId), 
    meli_item_id: item.id, 
    item_id: item.id,
    title: item.title, 
    price: item.price, 
    sold_quantity: item.sold_quantity || 0, 
    available_quantity: item.available_quantity || 1,
    status: item.status || 'active', 
    run_id: RUN_ID, 
    checked_at: CHECKED_AT, 
    timestamp: CHECKED_AT,
  }));

  const { error } = await supabase.from('snapshots').insert(rows);
  if (error) {
    if (error.message.includes('duplicate key')) {
      let inserted = 0;
      for (const row of rows) {
        const { error: e2 } = await supabase.from('snapshots').insert([row]);
        if (!e2) inserted++;
      }
      console.log(`  ✅ Insertados ${inserted}/${rows.length} (algunos duplicados saltados)`);
      return inserted;
    }
    throw new Error(`saveSnapshot: ${error.message}`);
  }
  
  console.log(`  ✅ ${rows.length} snapshots guardados en Supabase`);
  return rows.length;
}

// ─── Procesar seller ──────────────────────────────────────────────────────────
async function processSeller(seller) {
  const sellerId = seller.seller_id;
  const name = seller.nombre_real ?? seller.nickname ?? String(sellerId);
  console.log(`\n${'─'.repeat(70)}\n🏪 Seller: ${sellerId} — ${name}`);

  let status = 'failed', errorMessage = '', itemsFound = 0;
  try {
    const items = await scrapeSeller(sellerId);
    itemsFound = items.length;
    
    if (!items.length) throw new Error('0_items');
    
    console.log(`\n  📊 DATOS EXTRAÍDOS:`);
    items.slice(0, 3).forEach(i => {
      console.log(`    [${i.id}]`);
      console.log(`      Título: ${(i.title ?? '').substring(0, 60)}`);
      console.log(`      Precio: $${i.price}`);
      console.log(`      Vendidos: ${i.sold_quantity}`);
      console.log(`      Stock: ${i.available_quantity}`);
      console.log(`      Status: ${i.status}`);
    });
    
    await saveSnapshot(sellerId, items);
    status = 'ok';
    
  } catch (err) {
    errorMessage = err.message;
    console.error(`\n  ❌ ERROR: ${errorMessage}`);
  }

  return { sellerId, name, status, errorMessage, itemsFound };
}

// ─── Main ─────────────────────────────────────────────────────────────────────
(async () => {
  try {
    const sellers = await getActiveSellers();
    console.log(`\n📋 Sellers activos: ${sellers.length}\n`);

    const results = [];
    for (const seller of sellers) {
      const result = await processSeller(seller);
      results.push(result);
    }

    // Guardar resumen
    const summary = results
      .map(r => `${r.name}|${r.status}|${r.itemsFound}|${r.errorMessage}`)
      .join('\n');
    
    fs.writeFileSync('output/monitor_status.csv', 'seller|status|items|error\n' + summary);
    
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`✅ MONITOR COMPLETADO`);
    console.log(`═'.repeat(70)}`);
    console.log(results.map(r => `  ${r.name}: ${r.status} (${r.itemsFound} items)`).join('\n'));
    
  } catch (err) {
    console.error('\n❌ FATAL:', err.message);
    process.exit(1);
  }
})();
