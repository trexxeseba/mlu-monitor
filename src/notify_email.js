'use strict';

['SUPABASE_URL', 'SUPABASE_KEY', 'RESEND_API_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta variable ${k}`); process.exit(1); }
});

const https = require('https');
const { createClient } = require('@supabase/supabase-js');

const supabase      = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const RESEND_KEY    = process.env.RESEND_API_KEY;
const FROM          = process.env.NOTIFY_FROM || 'MLU Monitor <onboarding@resend.dev>';
const TO            = process.env.NOTIFY_TO   || 'undiaes@gmail.com';
const OXYLABS_USER  = process.env.OXYLABS_USER || '';
const OXYLABS_PASS  = process.env.OXYLABS_PASS || '';

const SEP = '═'.repeat(70);

// ─── HTTP helpers ──────────────────────────────────────────────────────────────
function httpPost(hostname, path, headers, bodyStr) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path, method: 'POST',
      headers: { 'Content-Length': Buffer.byteLength(bodyStr), ...headers },
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout')); });
    req.write(bodyStr);
    req.end();
  });
}

// ─── Fetch item details via Oxylabs → ML API ──────────────────────────────────
// Oxylabs actúa como proxy residencial para bypassear el 403 de GitHub Actions
async function fetchItemDetails(itemIds) {
  const details = {};
  if (!itemIds.length) return details;

  const useOxy = OXYLABS_USER && OXYLABS_PASS;

  // Chunked en grupos de 20 (límite ML API)
  const CHUNK = 20;
  for (let i = 0; i < itemIds.length; i += CHUNK) {
    const chunk = itemIds.slice(i, i + CHUNK);
    const mlUrl = `https://api.mercadolibre.com/items?ids=${chunk.join(',')}`;

    try {
      let body;

      if (useOxy) {
        // Llamar ML API a través de Oxylabs (evita bloqueo por IP de GitHub Actions)
        const payload = JSON.stringify({ source: 'universal', url: mlUrl });
        const auth    = Buffer.from(`${OXYLABS_USER}:${OXYLABS_PASS}`).toString('base64');
        const res = await httpPost('realtime.oxylabs.io', '/v1/queries', {
          'Content-Type':  'application/json',
          'Authorization': `Basic ${auth}`,
        }, payload);

        if (res.status !== 200) { console.warn(`  ⚠️  Oxylabs HTTP ${res.status}`); continue; }
        const oxy = JSON.parse(res.body);
        body = oxy.results?.[0]?.content;
        // content puede ser string JSON o ya objeto
        if (typeof body === 'string') body = JSON.parse(body);
      } else {
        // Fallback: directo (solo funciona en local)
        const r = await new Promise((resolve, reject) => {
          const req = https.request({ hostname:'api.mercadolibre.com', path:`/items?ids=${chunk.join(',')}`, method:'GET', headers:{'Accept':'application/json'} },
            res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>resolve({status:res.statusCode,body:d})); });
          req.on('error',reject); req.setTimeout(15000,()=>req.destroy()); req.end();
        });
        if (r.status !== 200) continue;
        body = JSON.parse(r.body);
      }

      if (!Array.isArray(body)) continue;

      for (const row of body) {
        if ((row.code === 200 || !row.code) && row.body?.id) {
          const b = row.body;
          details[b.id] = {
            title:     b.title || null,
            price:     b.price != null ? b.price : null,
            currency:  b.currency_id || 'UYU',
            thumbnail: b.thumbnail || b.pictures?.[0]?.url || null,
            url:       b.permalink || `https://articulo.mercadolibre.com.uy/-_${b.id}`,
          };
        }
      }
    } catch (e) {
      console.warn(`  ⚠️  fetchItemDetails chunk error: ${e.message}`);
    }
  }

  console.log(`   ${Object.keys(details).length}/${itemIds.length} ítems con detalle`);
  return details;
}

// ─── Detecciones de hoy ───────────────────────────────────────────────────────
async function getTodayDetections() {
  const now     = new Date();
  const todayUY = new Date(now.toLocaleString('en-US', { timeZone: 'America/Montevideo' }));
  todayUY.setHours(0, 0, 0, 0);
  const startUTC = new Date(todayUY.getTime() + 3 * 60 * 60 * 1000).toISOString();
  const endUTC   = new Date(todayUY.getTime() + 27 * 60 * 60 * 1000).toISOString();

  console.log(`📅 Detecciones entre ${startUTC.slice(0,16)} y ${endUTC.slice(0,16)} UTC`);

  const { data, error } = await supabase
    .from('bajas_detectadas')
    .select('tipo, item_id, seller_id, fecha_deteccion, run_id, title, price_anterior')
    .gte('fecha_deteccion', startUTC)
    .lt('fecha_deteccion', endUTC)
    .order('seller_id')
    .order('tipo');

  if (error) throw new Error(`getTodayDetections: ${error.message}`);
  return data || [];
}

// ─── Nombres de sellers ───────────────────────────────────────────────────────
async function getSellerNames(sellerIds) {
  if (!sellerIds.length) return {};
  const { data } = await supabase
    .from('sellers')
    .select('seller_id, nombre_real, nickname')
    .in('seller_id', sellerIds);

  const map = {};
  for (const s of (data || []))
    map[String(s.seller_id)] = s.nombre_real || s.nickname || String(s.seller_id);
  return map;
}

// ─── Card HTML de un ítem ─────────────────────────────────────────────────────
function itemCard(itemId, det, tipo) {
  const url       = det?.url || `https://articulo.mercadolibre.com.uy/-_${itemId}`;
  const title     = det?.title || itemId;
  const priceStr  = det?.price != null
    ? `$${Number(det.price).toLocaleString('es-UY')} ${det.currency || ''}`
    : '';
  const img       = det?.thumbnail
    ? det.thumbnail.replace('http://', 'https://')
    : null;

  const borderColor = tipo === 'bajada' ? '#c0392b' : '#27ae60';
  const imgHtml = img
    ? `<img src="${img}" alt="" width="80" height="80" style="object-fit:cover;border-radius:6px;margin-right:12px;flex-shrink:0">`
    : `<div style="width:80px;height:80px;background:#eee;border-radius:6px;margin-right:12px;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:24px">📦</div>`;

  return `
    <a href="${url}" style="text-decoration:none;color:inherit">
      <div style="display:flex;align-items:center;border:1px solid #e0e0e0;border-left:4px solid ${borderColor};border-radius:8px;padding:10px 12px;margin:8px 0;background:#fff;cursor:pointer">
        ${imgHtml}
        <div style="flex:1;overflow:hidden">
          <div style="font-size:13px;font-weight:600;color:#222;line-height:1.3;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${title}</div>
          ${priceStr ? `<div style="font-size:15px;font-weight:700;color:${borderColor}">${priceStr}</div>` : ''}
          <div style="font-size:11px;color:#999;margin-top:2px">${itemId}</div>
        </div>
      </div>
    </a>`;
}

// ─── Bloque por seller ────────────────────────────────────────────────────────
function buildSellerBlock(sellerName, bajadas, subidas, itemDetails) {
  if (!bajadas.length && !subidas.length) return '';
  const total = bajadas.length + subidas.length;

  let html = `
    <div style="border:1px solid #ddd;border-radius:10px;padding:16px;margin-bottom:20px;background:#fafafa">
      <h2 style="margin:0 0 14px;font-size:17px;color:#1a1a2e;border-bottom:2px solid #eee;padding-bottom:8px">
        🏪 ${sellerName}
        <span style="font-size:12px;font-weight:normal;color:#888;margin-left:8px">${total} cambio${total !== 1 ? 's' : ''} hoy</span>
      </h2>`;

  if (bajadas.length) {
    html += `
      <div style="margin-bottom:14px">
        <div style="background:#c0392b;color:#fff;font-size:13px;font-weight:700;padding:6px 12px;border-radius:6px;margin-bottom:8px;display:inline-block">
          📉 BAJADA — ${bajadas.length} ítem${bajadas.length !== 1 ? 's' : ''} ya no está${bajadas.length !== 1 ? 'n' : ''}
        </div>
        ${bajadas.map(r => itemCard(r.item_id, itemDetails[r.item_id], 'bajada')).join('')}
      </div>`;
  }

  if (subidas.length) {
    html += `
      <div>
        <div style="background:#27ae60;color:#fff;font-size:13px;font-weight:700;padding:6px 12px;border-radius:6px;margin-bottom:8px;display:inline-block">
          📈 SUBIDA — ${subidas.length} ítem${subidas.length !== 1 ? 's' : ''} nuevo${subidas.length !== 1 ? 's' : ''}
        </div>
        ${subidas.map(r => itemCard(r.item_id, itemDetails[r.item_id], 'subida')).join('')}
      </div>`;
  }

  html += `\n    </div>`;
  return html;
}

// ─── Email HTML ───────────────────────────────────────────────────────────────
function buildEmailHtml(detections, sellerNames, itemDetails) {
  const fecha = new Date().toLocaleString('es-UY', {
    timeZone: 'America/Montevideo',
    weekday: 'long', day: 'numeric', month: 'long', year: 'numeric',
  });

  const bySeller = {};
  for (const d of detections) {
    const key = String(d.seller_id);
    if (!bySeller[key]) bySeller[key] = { bajadas: [], subidas: [] };
    if (d.tipo === 'desaparecido_no_confirmado') bySeller[key].bajadas.push(d);
    else if (d.tipo === 'nuevo')                 bySeller[key].subidas.push(d);
  }

  const totalBajadas = detections.filter(d => d.tipo === 'desaparecido_no_confirmado').length;
  const totalSubidas  = detections.filter(d => d.tipo === 'nuevo').length;

  let sellerBlocks = '';
  for (const [sellerId, { bajadas, subidas }] of Object.entries(bySeller))
    sellerBlocks += buildSellerBlock(sellerNames[sellerId] || sellerId, bajadas, subidas, itemDetails);

  return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:Arial,sans-serif;max-width:680px;margin:0 auto;padding:20px;background:#f0f2f5;color:#222">

  <div style="background:#1a1a2e;color:#fff;padding:20px 24px;border-radius:12px;margin-bottom:24px">
    <h1 style="margin:0 0 4px;font-size:20px;font-weight:700">📊 MLU Monitor</h1>
    <p style="margin:0 0 14px;font-size:13px;color:#aaa;text-transform:capitalize">${fecha}</p>
    <div style="display:flex;gap:24px;font-size:16px">
      <div style="background:rgba(231,76,60,0.25);padding:8px 16px;border-radius:8px">
        📉 <strong style="color:#e74c3c">${totalBajadas}</strong> <span style="color:#ccc">bajadas</span>
      </div>
      <div style="background:rgba(46,204,113,0.2);padding:8px 16px;border-radius:8px">
        📈 <strong style="color:#2ecc71">${totalSubidas}</strong> <span style="color:#ccc">subidas</span>
      </div>
    </div>
  </div>

  ${detections.length === 0
    ? `<div style="background:#fff;border-radius:10px;padding:32px;text-align:center;color:#888">Sin cambios detectados hoy.</div>`
    : sellerBlocks}

  <p style="font-size:11px;color:#aaa;text-align:center;margin-top:20px">
    MLU Monitor · GitHub Actions · Automático
  </p>
</body>
</html>`;
}

// ─── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  console.log(`\n${SEP}`);
  console.log('NOTIFY EMAIL v3 — INICIO');
  console.log(`${SEP}\n`);

  let detections;
  try {
    detections = await getTodayDetections();
    console.log(`📋 ${detections.length} detecciones hoy`);
  } catch (err) {
    console.error(`❌ ${err.message}`);
    process.exit(1);
  }

  const bajadas = detections.filter(d => d.tipo === 'desaparecido_no_confirmado');
  const subidas  = detections.filter(d => d.tipo === 'nuevo');
  console.log(`  📉 ${bajadas.length} bajadas  📈 ${subidas.length} subidas`);

  if (detections.length === 0) {
    console.log('\nℹ️  Sin cambios hoy — no se manda email.');
    process.exit(0);
  }

  const sellerIds   = [...new Set(detections.map(d => String(d.seller_id)))];
  const sellerNames = await getSellerNames(sellerIds);

  // Detalles de ítems: primero usamos title/price ya guardados en bajas_detectadas
  // El fetch externo solo se usa para ítems sin título (legado o fallo de enriquecimiento)
  const itemDetails = {};
  for (const d of detections) {
    if (d.title || d.price_anterior) {
      itemDetails[d.item_id] = {
        title:    d.title     || null,
        price:    d.price_anterior ?? null,
        currency: 'UYU',
        thumbnail: null,
        url: `https://articulo.mercadolibre.com.uy/-_${d.item_id}`,
      };
    }
  }
  const sinDetalle = [...new Set(detections.map(d => d.item_id))].filter(id => !itemDetails[id]);
  if (sinDetalle.length && OXYLABS_USER) {
    console.log(`\n🔍 Fetching detalles faltantes para ${sinDetalle.length} ítems...`);
    const extra = await fetchItemDetails(sinDetalle);
    Object.assign(itemDetails, extra);
  }

  const subject = `📊 MLU Monitor — 📉${bajadas.length} bajadas · 📈${subidas.length} subidas`;
  const html    = buildEmailHtml(detections, sellerNames, itemDetails);

  console.log(`\n📧 Enviando a ${TO}...`);
  const res = await httpPost('api.resend.com', '/emails', {
    'Authorization': `Bearer ${RESEND_KEY}`,
    'Content-Type':  'application/json',
  }, JSON.stringify({ from: FROM, to: [TO], subject, html }));

  if (res.status === 200 || res.status === 201) {
    console.log(`✅ Email enviado`);
    process.exit(0);
  } else {
    console.error(`❌ Resend HTTP ${res.status}: ${res.body}`);
    process.exit(1);
  }
})();
