const https = require('https');
const fs    = require('fs');

['BD_API_KEY','BD_ZONE'].forEach(k => {
  if (!process.env[k]) { console.error(`FATAL: falta ${k}`); process.exit(1); }
});

const BD_API_KEY = process.env.BD_API_KEY;
const BD_ZONE    = process.env.BD_ZONE;

function scrape(url) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({ zone: BD_ZONE, url, format: 'raw' });
    const options = {
      hostname: 'api.brightdata.com',
      path:     '/request',
      method:   'POST',
      headers:  {
        'Content-Type':   'application/json',
        'Authorization':  `Bearer ${BD_API_KEY}`,
        'Content-Length': Buffer.byteLength(payload),
      },
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, html: data }));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  const url = 'https://listado.mercadolibre.com.uy/_CustId_42794274';
  console.log(`Scraping: ${url}`);

  const { status, html } = await scrape(url);
  console.log(`HTTP status: ${status}`);
  console.log(`HTML length: ${html.length.toLocaleString()} chars`);

  fs.mkdirSync('output', { recursive: true });

  // Guardar HTML completo
  fs.writeFileSync('output/debug_raw.html', html);
  console.log('Guardado: output/debug_raw.html');

  // Diagnóstico rápido
  console.log('\n--- DIAGNÓSTICO ---');
  console.log('Tiene "polycard"    :', html.includes('"polycard"'));
  console.log('Tiene "components"  :', html.includes('"components"'));
  console.log('Tiene "blocked"     :', html.toLowerCase().includes('blocked'));
  console.log('Tiene "captcha"     :', html.toLowerCase().includes('captcha'));
  console.log('Tiene "listado"     :', html.toLowerCase().includes('listado'));
  console.log('Tiene "MLU"         :', html.includes('MLU'));

  // Primeros 2000 chars para ver qué está devolviendo
  console.log('\n--- PRIMEROS 2000 CHARS ---');
  console.log(html.substring(0, 2000));
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
