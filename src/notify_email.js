'use strict';

/**
 * notify_email.js
 *
 * Lee las detecciones del último run válido desde Supabase y manda
 * un email HTML con 3 secciones via Resend API.
 *
 * Variables de entorno requeridas:
 *   SUPABASE_URL, SUPABASE_KEY  — credenciales Supabase
 *   RESEND_API_KEY              — API key de Resend (re_xxxx...)
 *   NOTIFY_FROM                 — dirección "from" (ej: monitor@tudominio.com)
 *   NOTIFY_TO                   — destino (default: undiaes@gmail.com)
 *
 * Códigos de salida:
 *   0 → email enviado (o sin cambios, nada que notificar)
 *   1 → error técnico
 */

['SUPABASE_URL', 'SUPABASE_KEY', 'RESEND_API_KEY'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta variable ${k}`); process.exit(1); }
});

const https = require('https');
const { createClient } = require('@supabase/supabase-js');

// ─── HTTP helper ──────────────────────────────────────────────────────────────
function httpGet(hostname, path, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path, method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MLUMonitor/2.0)',
        'Accept':     'application/json',
        ...extraHeaders,
      },
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

const supabase   = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const RESEND_KEY = process.env.RESEND_API_KEY;
const FROM       = process.env.NOTIFY_FROM || 'MLU Monitor <onboarding@resend.dev>';
const TO         = process.env.NOTIFY_TO   || 'undiaes@gmail.com';

const SEP = '═'.repeat(70);

// ─── Obtener el último run válido ─────────────────────────────────────────────
async function getLatestValidRunId() {
  // Intento 1: monitor_runs
  {
    const { data, error } = await supabase
      .from('monitor_runs')
      .select('run_id, finished_at, sellers_ok, sellers_total, total_items')
      .eq('status', 'valid')
      .order('finished_at', { ascending: false })
      .limit(1);

    if (!error && data?.length) return data[0];
  }

  // Intento 2: execution_logs
  {
    const { data, error } = await supabase
      .from('execution_logs')
      .select('run_id, executed_at, sellers_success, sellers_total, items_processed')
      .eq('status', 'success')
      .order('executed_at', { ascending: false })
      .limit(1);

    if (!error && data?.length) {
      const r = data[0];
      return {
        run_id:        r.run_id,
        finished_at:   r.executed_at,
        sellers_ok:    r.sellers_success,
        sellers_total: r.sellers_total,
        total_items:   r.items_processed,
      };
    }
  }

  throw new Error('No se encontró ningún run válido');
}

// ─── Leer detecciones del run ──────────────────────────────────────────────────
async function getDetections(runId) {
  const { data, error } = await supabase
    .from('bajas_detectadas')
    .select('tipo, item_id, seller_id, fecha_deteccion')
    .eq('run_id', runId)
    .order('tipo')
    .order('seller_id');

  if (error) throw new Error(`getDetections: ${error.message}`);
  return data || [];
}

// ─── Leer nombres de sellers ───────────────────────────────────────────────────
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

// ─── Obtener título y precio de items via MLU API pública ─────────────────────
// Endpoint: GET https://api.mercadolibre.com/items?ids=MLU1,MLU2,...
// Sin auth, máximo 20 IDs por request.
async function fetchItemDetails(itemIds) {
  const details = {};
  if (!itemIds.length) return details;

  // Test rápido con 1 ítem para detectar bloqueo de IP antes de iterar
  const testId  = itemIds[0];
  const testRes = await httpGet('api.mercadolibre.com', `/items/${testId}`).catch(e => ({ status: 0, body: e.message }));
  if (testRes.status !== 200) {
    console.warn(`  ⚠️  MLU API no accesible desde este runner (HTTP ${testRes.status}) — email sin títulos`);
    return details;
  }

  const CHUNK = 20;
  for (let i = 0; i < itemIds.length; i += CHUNK) {
    const chunk = itemIds.slice(i, i + CHUNK);
    const path  = `/items?ids=${chunk.join(',')}`;
    try {
      const res = await httpGet('api.mercadolibre.com', path);
      if (res.status !== 200) {
        console.warn(`  ⚠️  MLU API chunk ${i/CHUNK+1}: HTTP ${res.status}`);
        continue;
      }
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

// ─── Construir tabla HTML ──────────────────────────────────────────────────────
function buildTable(rows, sellerNames, itemDetails, emptyMsg) {
  if (!rows.length) {
    return `<p style="color:#888;font-style:italic;margin:8px 0">${emptyMsg}</p>`;
  }

  // Agrupar por seller
  const bySeller = {};
  for (const r of rows) {
    const key = String(r.seller_id);
    if (!bySeller[key]) bySeller[key] = [];
    bySeller[key].push(r);
  }

  let html = '';
  for (const [sellerId, items] of Object.entries(bySeller)) {
    const name = sellerNames[sellerId] || sellerId;
    html += `<p style="margin:12px 0 4px;font-weight:bold;color:#333">${name} (${items.length} ítems)</p>`;
    html += '<ul style="margin:0 0 8px 0;padding-left:20px">';
    for (const item of items) {
      const url  = `https://articulo.mercadolibre.com.uy/-_${item.item_id}`;
      const det  = itemDetails[item.item_id];
      const label = det?.title
        ? `${det.title}${det.price ? ` — ${det.price}` : ''}`
        : item.item_id;
      html += `<li style="margin:4px 0"><a href="${url}" style="color:#1a73e8;text-decoration:none">${label}</a></li>`;
    }
    html += '</ul>';
  }
  return html;
}

// ─── Armar email HTML completo ─────────────────────────────────────────────────
function buildEmailHtml(run, desaparecidos, nuevos, reaparecidos, sellerNames, itemDetails) {
  const fecha = new Date(run.finished_at).toLocaleString('es-UY', { timeZone: 'America/Montevideo' });

  const section = (emoji, title, color, rows, emptyMsg) => `
    <div style="margin-bottom:28px">
      <h2 style="margin:0 0 10px;padding:8px 12px;background:${color};border-radius:6px;font-size:16px;color:#fff">
        ${emoji} ${title} (${rows.length})
      </h2>
      ${buildTable(rows, sellerNames, itemDetails, emptyMsg)}
    </div>`;

  const total = desaparecidos.length + nuevos.length + reaparecidos.length;

  return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px;color:#222">

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-bottom:24px">
    <h1 style="margin:0;font-size:20px">MLU Monitor — Reporte de cambios</h1>
    <p style="margin:6px 0 0;font-size:13px;color:#aaa">
      Run: ${run.run_id} &nbsp;|&nbsp; ${fecha} &nbsp;|&nbsp;
      Sellers: ${run.sellers_ok}/${run.sellers_total} OK &nbsp;|&nbsp;
      Items totales: ${run.total_items}
    </p>
  </div>

  ${total === 0
    ? `<p style="font-size:15px;color:#555">Sin cambios detectados en este run.</p>`
    : `
      ${section('📉', 'POSIBLES VENTAS', '#c0392b', desaparecidos,
        'Sin desapariciones en este run.')}
      ${section('🆕', 'ÍTEMS NUEVOS', '#27ae60', nuevos,
        'Sin ítems nuevos en este run.')}
      ${section('🔄', 'REAPARICIONES', '#e67e22', reaparecidos,
        'Sin reapariciones en este run.')}
    `}

  <hr style="border:none;border-top:1px solid #eee;margin:24px 0">
  <p style="font-size:11px;color:#aaa;text-align:center">
    Generado automáticamente por MLU Monitor · GitHub Actions
  </p>
</body>
</html>`;
}

// ─── Enviar email via Resend API ───────────────────────────────────────────────
function sendEmail(subject, html) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ from: FROM, to: [TO], subject, html });

    const req = https.request({
      hostname: 'api.resend.com',
      path:     '/emails',
      method:   'POST',
      headers: {
        'Authorization': `Bearer ${RESEND_KEY}`,
        'Content-Type':  'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout 30s')); });
    req.write(body);
    req.end();
  });
}

// ─── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  console.log(`\n${SEP}`);
  console.log('NOTIFY EMAIL — INICIO');
  console.log(`${SEP}\n`);

  // 1. Último run válido
  let run;
  try {
    run = await getLatestValidRunId();
    console.log(`📌 Run: ${run.run_id} (${run.finished_at})`);
  } catch (err) {
    console.error(`❌ ${err.message}`);
    process.exit(1);
  }

  // 2. Detecciones
  let detections;
  try {
    detections = await getDetections(run.run_id);
    console.log(`📋 ${detections.length} detecciones para este run`);
  } catch (err) {
    console.error(`❌ ${err.message}`);
    process.exit(1);
  }

  const desaparecidos = detections.filter(d => d.tipo === 'desaparecido_no_confirmado');
  const nuevos        = detections.filter(d => d.tipo === 'nuevo');
  const reaparecidos  = detections.filter(d => d.tipo === 'reaparecido');

  console.log(`  📉 Desaparecidos: ${desaparecidos.length}`);
  console.log(`  🆕 Nuevos:        ${nuevos.length}`);
  console.log(`  🔄 Reaparecidos:  ${reaparecidos.length}`);

  const total = desaparecidos.length + nuevos.length + reaparecidos.length;

  if (total === 0) {
    console.log('\nℹ️  Sin cambios detectados — no se manda email.');
    process.exit(0);
  }

  // 3. Nombres de sellers
  const sellerIds = [...new Set(detections.map(d => String(d.seller_id)))];
  const sellerNames = await getSellerNames(sellerIds);

  // 4. Título y precio de cada ítem via MLU API
  const allItemIds = [...new Set(detections.map(d => d.item_id))];
  console.log(`🔍 Obteniendo detalles de ${allItemIds.length} ítems desde MLU API...`);
  const itemDetails = await fetchItemDetails(allItemIds);
  console.log(`   ${Object.keys(itemDetails).length}/${allItemIds.length} ítems con título/precio`);

  // 5. Construir email
  const subject = `MLU Monitor — ${desaparecidos.length} ventas · ${nuevos.length} nuevos · ${reaparecidos.length} reapariciones`;
  const html = buildEmailHtml(run, desaparecidos, nuevos, reaparecidos, sellerNames, itemDetails);

  // 6. Enviar
  console.log(`\n📧 Enviando email a ${TO}...`);
  let res;
  try {
    res = await sendEmail(subject, html);
  } catch (err) {
    console.error(`❌ Error enviando email: ${err.message}`);
    process.exit(1);
  }

  if (res.status === 200 || res.status === 201) {
    let id = '';
    try { id = JSON.parse(res.body).id || ''; } catch(_) {}
    console.log(`✅ Email enviado correctamente${id ? ` (id: ${id})` : ''}`);
    process.exit(0);
  } else {
    console.error(`❌ Resend respondió HTTP ${res.status}: ${res.body}`);
    process.exit(1);
  }
})();
