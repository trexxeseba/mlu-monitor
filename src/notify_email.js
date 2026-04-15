'use strict';

/**
 * notify_email.js — v2
 *
 * Lee TODAS las detecciones del día de hoy desde bajas_detectadas,
 * agrupa por seller y manda un email HTML con secciones claras:
 *   📉 BAJADAS (desaparecido_no_confirmado) — ítem ya no está en el listado
 *   📈 SUBIDAS (nuevo)                      — ítem apareció hoy
 *
 * Variables de entorno requeridas:
 *   SUPABASE_URL, SUPABASE_KEY  — credenciales Supabase
 *   RESEND_API_KEY              — API key de Resend
 *   NOTIFY_FROM                 — dirección "from" verificada en Resend
 *   NOTIFY_TO                   — destinatario (default: undiaes@gmail.com)
 *
 * Códigos de salida:
 *   0 → email enviado (o sin cambios)
 *   1 → error técnico
 */

['SUPABASE_URL', 'SUPABASE_KEY', 'RESEND_API_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta variable ${k}`); process.exit(1); }
});

const https = require('https');
const { createClient } = require('@supabase/supabase-js');

const supabase   = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const RESEND_KEY = process.env.RESEND_API_KEY;
const FROM       = process.env.NOTIFY_FROM || 'MLU Monitor <onboarding@resend.dev>';
const TO         = process.env.NOTIFY_TO   || 'undiaes@gmail.com';

const SEP = '═'.repeat(70);

// ─── HTTP helper ──────────────────────────────────────────────────────────────
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
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout 30s')); });
    req.write(bodyStr);
    req.end();
  });
}

function httpGet(hostname, path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path, method: 'GET',
      headers: { 'User-Agent': 'MLUMonitor/2.0', 'Accept': 'application/json' },
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout 30s')); });
    req.end();
  });
}

// ─── Detecciones de hoy ───────────────────────────────────────────────────────
async function getTodayDetections() {
  // Rango UTC del día de hoy en Uruguay (UTC-3): desde 03:00 UTC hasta mañana 03:00 UTC
  const now     = new Date();
  const todayUY = new Date(now.toLocaleString('en-US', { timeZone: 'America/Montevideo' }));
  todayUY.setHours(0, 0, 0, 0);
  const startUTC = new Date(todayUY.getTime() + 3 * 60 * 60 * 1000).toISOString(); // 03:00 UTC
  const endUTC   = new Date(todayUY.getTime() + 27 * 60 * 60 * 1000).toISOString(); // +24h

  console.log(`📅 Buscando detecciones entre ${startUTC} y ${endUTC}`);

  const { data, error } = await supabase
    .from('bajas_detectadas')
    .select('tipo, item_id, seller_id, fecha_deteccion, run_id')
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
  for (const s of (data || [])) {
    map[String(s.seller_id)] = s.nombre_real || s.nickname || String(s.seller_id);
  }
  return map;
}

// ─── Detalles de ítems via MLU API pública ────────────────────────────────────
async function fetchItemDetails(itemIds) {
  const details = {};
  if (!itemIds.length) return details;

  // Test rápido para detectar bloqueo de IP
  const testRes = await httpGet('api.mercadolibre.com', `/items/${itemIds[0]}`).catch(() => ({ status: 0 }));
  if (testRes.status !== 200) {
    console.warn(`  ⚠️  MLU API no accesible (HTTP ${testRes.status}) — email sin títulos`);
    return details;
  }

  const CHUNK = 20;
  for (let i = 0; i < itemIds.length; i += CHUNK) {
    const chunk = itemIds.slice(i, i + CHUNK);
    try {
      const res = await httpGet('api.mercadolibre.com', `/items?ids=${chunk.join(',')}`);
      if (res.status !== 200) continue;
      const rows = JSON.parse(res.body);
      for (const row of rows) {
        if (row.code === 200 && row.body?.id) {
          const b = row.body;
          const price = b.price != null
            ? `$${Number(b.price).toLocaleString('es-UY')} ${b.currency_id || ''}`
            : null;
          details[b.id] = { title: b.title || null, price };
        }
      }
    } catch (e) {
      console.warn(`  ⚠️  MLU API chunk error: ${e.message}`);
    }
  }
  return details;
}

// ─── Construir bloque HTML por seller ────────────────────────────────────────
function buildSellerBlock(sellerName, bajadas, subidas, itemDetails) {
  const total = bajadas.length + subidas.length;
  if (total === 0) return '';

  const itemLink = (itemId) => {
    const url  = `https://articulo.mercadolibre.com.uy/-_${itemId}`;
    const det  = itemDetails[itemId];
    const label = det?.title
      ? `${det.title}${det.price ? ` — ${det.price}` : ''}`
      : itemId;
    return `<li style="margin:4px 0"><a href="${url}" style="color:#1a73e8;text-decoration:none">${label}</a></li>`;
  };

  let html = `
    <div style="border:1px solid #ddd;border-radius:8px;padding:16px;margin-bottom:20px">
      <h2 style="margin:0 0 14px;font-size:17px;color:#1a1a2e;border-bottom:2px solid #eee;padding-bottom:8px">
        🏪 ${sellerName}
        <span style="font-size:13px;font-weight:normal;color:#666;margin-left:8px">${total} cambios hoy</span>
      </h2>`;

  if (bajadas.length > 0) {
    html += `
      <div style="margin-bottom:14px">
        <h3 style="margin:0 0 8px;font-size:14px;color:#fff;background:#c0392b;padding:6px 10px;border-radius:5px;display:inline-block">
          📉 BAJADA — ${bajadas.length} ítem${bajadas.length !== 1 ? 's' : ''} ya no está${bajadas.length !== 1 ? 'n' : ''}
        </h3>
        <ul style="margin:6px 0 0;padding-left:20px">
          ${bajadas.map(r => itemLink(r.item_id)).join('\n          ')}
        </ul>
      </div>`;
  }

  if (subidas.length > 0) {
    html += `
      <div>
        <h3 style="margin:0 0 8px;font-size:14px;color:#fff;background:#27ae60;padding:6px 10px;border-radius:5px;display:inline-block">
          📈 SUBIDA — ${subidas.length} ítem${subidas.length !== 1 ? 's' : ''} nuevo${subidas.length !== 1 ? 's' : ''}
        </h3>
        <ul style="margin:6px 0 0;padding-left:20px">
          ${subidas.map(r => itemLink(r.item_id)).join('\n          ')}
        </ul>
      </div>`;
  }

  html += `\n    </div>`;
  return html;
}

// ─── Armar email HTML ─────────────────────────────────────────────────────────
function buildEmailHtml(detections, sellerNames, itemDetails) {
  const fecha = new Date().toLocaleString('es-UY', { timeZone: 'America/Montevideo' });

  // Agrupar por seller
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
  for (const [sellerId, { bajadas, subidas }] of Object.entries(bySeller)) {
    const name = sellerNames[sellerId] || sellerId;
    sellerBlocks += buildSellerBlock(name, bajadas, subidas, itemDetails);
  }

  return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:Arial,sans-serif;max-width:720px;margin:0 auto;padding:20px;color:#222;background:#f5f5f5">

  <div style="background:#1a1a2e;color:#fff;padding:18px 22px;border-radius:10px;margin-bottom:24px">
    <h1 style="margin:0;font-size:22px">📊 MLU Monitor — Reporte del día</h1>
    <p style="margin:8px 0 0;font-size:13px;color:#aaa">${fecha}</p>
    <div style="margin-top:12px;display:flex;gap:20px;font-size:15px">
      <span>📉 <strong style="color:#e74c3c">${totalBajadas} BAJADAS</strong></span>
      <span>📈 <strong style="color:#2ecc71">${totalSubidas} SUBIDAS</strong></span>
    </div>
  </div>

  ${detections.length === 0
    ? `<div style="background:#fff;border-radius:8px;padding:24px;text-align:center;color:#666;font-size:15px">
         Sin cambios detectados hoy.
       </div>`
    : sellerBlocks}

  <hr style="border:none;border-top:1px solid #ddd;margin:24px 0">
  <p style="font-size:11px;color:#999;text-align:center">
    Generado automáticamente por MLU Monitor · GitHub Actions
  </p>
</body>
</html>`;
}

// ─── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  console.log(`\n${SEP}`);
  console.log('NOTIFY EMAIL v2 — INICIO');
  console.log(`${SEP}\n`);

  // 1. Detecciones de hoy
  let detections;
  try {
    detections = await getTodayDetections();
    console.log(`📋 ${detections.length} detecciones hoy en total`);
  } catch (err) {
    console.error(`❌ ${err.message}`);
    process.exit(1);
  }

  const bajadas = detections.filter(d => d.tipo === 'desaparecido_no_confirmado');
  const subidas  = detections.filter(d => d.tipo === 'nuevo');
  console.log(`  📉 BAJADAS: ${bajadas.length}`);
  console.log(`  📈 SUBIDAS: ${subidas.length}`);

  if (detections.length === 0) {
    console.log('\nℹ️  Sin cambios hoy — no se manda email.');
    process.exit(0);
  }

  // 2. Nombres de sellers
  const sellerIds   = [...new Set(detections.map(d => String(d.seller_id)))];
  const sellerNames = await getSellerNames(sellerIds);
  console.log(`\n🏪 Sellers con cambios: ${sellerIds.length}`);
  for (const id of sellerIds) {
    const n  = sellerNames[id] || id;
    const b  = detections.filter(d => String(d.seller_id) === id && d.tipo === 'desaparecido_no_confirmado').length;
    const s  = detections.filter(d => String(d.seller_id) === id && d.tipo === 'nuevo').length;
    console.log(`  ${n}: 📉${b} 📈${s}`);
  }

  // 3. Detalles de ítems
  const allItemIds = [...new Set(detections.map(d => d.item_id))];
  console.log(`\n🔍 Obteniendo detalles de ${allItemIds.length} ítems...`);
  const itemDetails = await fetchItemDetails(allItemIds);
  console.log(`   ${Object.keys(itemDetails).length}/${allItemIds.length} ítems con título/precio`);

  // 4. Construir y enviar email
  const subject = `📊 MLU Monitor — 📉${bajadas.length} bajadas · 📈${subidas.length} subidas hoy`;
  const html    = buildEmailHtml(detections, sellerNames, itemDetails);

  console.log(`\n📧 Enviando email a ${TO}...`);
  const bodyStr = JSON.stringify({ from: FROM, to: [TO], subject, html });
  let res;
  try {
    res = await httpPost('api.resend.com', '/emails', {
      'Authorization': `Bearer ${RESEND_KEY}`,
      'Content-Type':  'application/json',
    }, bodyStr);
  } catch (err) {
    console.error(`❌ Error enviando email: ${err.message}`);
    process.exit(1);
  }

  if (res.status === 200 || res.status === 201) {
    let id = '';
    try { id = JSON.parse(res.body).id || ''; } catch(_) {}
    console.log(`✅ Email enviado${id ? ` (id: ${id})` : ''}`);
    console.log(`\n${SEP}`);
    process.exit(0);
  } else {
    console.error(`❌ Resend HTTP ${res.status}: ${res.body}`);
    process.exit(1);
  }
})();
