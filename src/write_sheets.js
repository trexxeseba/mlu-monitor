'use strict';

/**
 * write_sheets.js
 *
 * Lee output/detector_summary.json y escribe una fila por seller en Google Sheets.
 * Columnas: fecha | seller | items_actuales | desaparecidos | run_actual | run_anterior
 *
 * Variables de entorno requeridas:
 *   GSHEETS_SA_KEY_PATH — path al JSON de la service account (decodificado en el runner)
 *   GSHEETS_SHEET_ID    — ID del Google Sheet
 *
 * Salida:
 *   0 → OK
 *   1 → error técnico
 */

const fs    = require('fs');
const path  = require('path');
const { google } = require('googleapis');

const SA_PATH  = process.env.GSHEETS_SA_KEY_PATH;
const SHEET_ID = process.env.GSHEETS_SHEET_ID || '1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE';
const TAB      = 'Historial';  // nombre de la pestaña

const SEP = '═'.repeat(70);

(async () => {
  console.log(`\n${SEP}`);
  console.log('WRITE SHEETS — INICIO');
  console.log(`${SEP}\n`);

  // 1. Leer summary del detector
  const summaryPath = path.join('output', 'detector_summary.json');
  if (!fs.existsSync(summaryPath)) {
    console.error('❌ No existe output/detector_summary.json — ¿corrió el detector?');
    process.exit(1);
  }

  const summary = JSON.parse(fs.readFileSync(summaryPath, 'utf8'));
  console.log(`📄 Summary: ${summary.det_run_id} (${summary.sellers.length} sellers)`);

  if (!summary.sellers.length) {
    console.log('ℹ️  Sin sellers en el summary — nada que escribir.');
    process.exit(0);
  }

  // 2. Auth con service account
  if (!SA_PATH || !fs.existsSync(SA_PATH)) {
    console.error(`❌ Service account no encontrada en: ${SA_PATH}`);
    process.exit(1);
  }

  const auth = new google.auth.GoogleAuth({
    keyFile: SA_PATH,
    scopes:  ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  // 3. Construir filas — una por seller
  const fecha = new Date(summary.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' });

  const rows = summary.sellers.map(s => [
    fecha,
    s.name,
    s.items_actuales,
    s.desaparecidos,
    s.run_actual,
    s.run_anterior,
  ]);

  console.log(`📊 Escribiendo ${rows.length} filas en Sheet...`);

  // 4. Append al Sheet
  try {
    const res = await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range:         `${TAB}!A:F`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: rows },
    });

    const updates = res.data.updates;
    console.log(`✅ ${updates.updatedRows} filas escritas en ${updates.updatedRange}`);
  } catch (err) {
    console.error(`❌ Error escribiendo en Sheets: ${err.message}`);
    process.exit(1);
  }

  console.log(`\n${SEP}`);
  console.log('WRITE SHEETS COMPLETADO');
  console.log(`${SEP}\n`);

  process.exit(0);
})();
