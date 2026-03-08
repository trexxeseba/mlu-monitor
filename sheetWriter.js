const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');

// Validación estricta de datos bidimensionales
function assert2D(values, name = 'values') {
  if (!Array.isArray(values)) {
    throw new Error(`${name} debe ser un array`);
  }
  if (values.length === 0) {
    throw new Error(`${name} no puede estar vacío`);
  }
  if (!values.every(row => Array.isArray(row))) {
    throw new Error(`${name} debe ser un array bidimensional [[...], [...]]`);
  }
}

// Retry con backoff exponencial para fallos transitorios
async function withRetry(fn, maxRetries = 4, label = 'Operation') {
  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      
      // Detectar códigos HTTP retriables
      const status =
        err?.response?.status ||
        err?.status ||
        err?.code ||
        null;

      const retriable =
        status === 429 ||  // Rate limit
        status === 500 ||  // Internal server error
        status === 502 ||  // Bad gateway
        status === 503 ||  // Service unavailable
        status === 504;    // Gateway timeout

      if (!retriable || attempt === maxRetries) {
        throw err;
      }

      // Backoff exponencial: 1s, 2s, 4s, 8s
      const delay = Math.min(1000 * Math.pow(2, attempt), 8000);
      console.log(`  [!] ${label} failed (${status}). Retry ${attempt + 1}/${maxRetries} in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  throw lastError;
}

class SheetWriter {
  constructor({
    spreadsheetId,
    clientEmail,
    privateKey
  }) {
    // Validación de parámetros obligatorios
    if (!spreadsheetId) throw new Error('spreadsheetId es obligatorio');
    if (!clientEmail) throw new Error('clientEmail es obligatorio');
    if (!privateKey) throw new Error('privateKey es obligatoria');

    this.spreadsheetId = spreadsheetId;
    this.clientEmail = clientEmail;
    this.privateKey = privateKey.replace(/\\n/g, '\n');
    
    // Cache de sheets para evitar re-loadInfo
    this.doc = null;
    this.sheets = {};
  }

  // Inicializar documento y conectar
  async init() {
    const auth = new JWT({
      email: this.clientEmail,
      key: this.privateKey,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });

    this.doc = new GoogleSpreadsheet(this.spreadsheetId, auth);

    await withRetry(
      () => this.doc.loadInfo(),
      3,
      'Load Sheet Info'
    );

    console.log(`  [✓] Google Sheets conectado (${Object.keys(this.doc.sheetsByTitle).length} hojas)`);
  }

  // Asegurar que el documento esté inicializado
  async ensureInit() {
    if (!this.doc) {
      await this.init();
    }
  }

  // Obtener o crear una hoja
  async getOrCreateSheet(sheetTitle) {
    await this.ensureInit();

    let sheet = this.doc.sheetsByTitle[sheetTitle];

    if (!sheet) {
      sheet = await withRetry(
        () => this.doc.addSheet({ title: sheetTitle }),
        3,
        `Create sheet "${sheetTitle}"`
      );
      console.log(`  [+] Sheet creado: ${sheetTitle}`);
    }

    return sheet;
  }

  // Asegurar que el header sea correcto (sin sobrescribir si es igual)
  async ensureHeader(sheetTitle, headerRow) {
    const sheet = await this.getOrCreateSheet(sheetTitle);

    await withRetry(async () => {
      // Cargar headers actuales
      await sheet.loadHeaderRow().catch(() => null);

      const currentHeader = sheet.headerValues || [];
      const sameHeader =
        currentHeader.length === headerRow.length &&
        currentHeader.every((v, i) => v === headerRow[i]);

      // Solo set si cambió
      if (!sameHeader) {
        await sheet.setHeaderRow(headerRow);
      }
    }, 3, `Ensure header for "${sheetTitle}"`);
  }

  // Limpiar todas las filas (excepto header si existe)
  async clearRows(sheetTitle) {
    const sheet = await this.getOrCreateSheet(sheetTitle);

    await withRetry(
      () => sheet.clearRows(),
      3,
      `Clear rows in "${sheetTitle}"`
    );
  }

  // Sobrescribir completamente (borrar + header + rows nuevas)
  async overwriteSheet(sheetTitle, headerRow, rows) {
    const sheet = await this.getOrCreateSheet(sheetTitle);
    
    assert2D(rows, 'rows');

    await withRetry(async () => {
      // 1. Limpiar todo
      await sheet.clear();

      // 2. Set header
      await sheet.setHeaderRow(headerRow);

      // 3. Agregar rows si hay datos
      if (rows.length > 0) {
        // Convertir arrays a objetos con keys de header
        const objects = rows.map(row =>
          Object.fromEntries(
            headerRow.map((key, i) => [key, row[i] ?? ''])
          )
        );
        await sheet.addRows(objects);
      }
    }, 3, `Overwrite "${sheetTitle}"`);

    console.log(`  [✓] ${sheetTitle}: ${rows.length} filas escritas`);
  }

  // Agregar rows al final (append)
  async appendRows(sheetTitle, headerRow, rows) {
    if (rows.length === 0) {
      console.log(`  [~] ${sheetTitle}: sin rows para append`);
      return;
    }

    const sheet = await this.getOrCreateSheet(sheetTitle);
    assert2D(rows, 'rows');

    // Asegurar que el header es correcto
    await this.ensureHeader(sheetTitle, headerRow);

    await withRetry(async () => {
      // Convertir arrays a objetos
      const objects = rows.map(row =>
        Object.fromEntries(
          headerRow.map((key, i) => [key, row[i] ?? ''])
        )
      );
      await sheet.addRows(objects);
    }, 3, `Append to "${sheetTitle}"`);

    console.log(`  [✓] ${sheetTitle}: ${rows.length} filas agregadas`);
  }

  // Borrar una hoja completa
  async deleteSheet(sheetTitle) {
    await this.ensureInit();

    const sheet = this.doc.sheetsByTitle[sheetTitle];
    if (!sheet) {
      console.log(`  [~] Sheet "${sheetTitle}" no existe`);
      return;
    }

    await withRetry(
      () => sheet.delete(),
      3,
      `Delete sheet "${sheetTitle}"`
    );

    console.log(`  [+] Sheet eliminado: ${sheetTitle}`);
  }

  // Listar todas las hojas
  async listSheets() {
    await this.ensureInit();
    return Object.keys(this.doc.sheetsByTitle);
  }
}

module.exports = { SheetWriter };
