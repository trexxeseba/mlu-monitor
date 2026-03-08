const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');

class SheetWriter {
  constructor(spreadsheetId, clientEmail, privateKey) {
    // Formatear la private key correctamente (vital para GitHub Actions)
    const formattedKey = privateKey.replace(/\\n/g, '\n');

    // Inicializar autenticación
    const auth = new JWT({
      email: clientEmail,
      key: formattedKey,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });

    this.doc = new GoogleSpreadsheet(spreadsheetId, auth);
  }

  async init() {
    // Carga las propiedades del documento y las hojas
    await this.doc.loadInfo();
  }

  async writeSheet(sheetTitle, headers, rows) {
    // 1. Ubicar la hoja por su nombre
    let sheet = this.doc.sheetsByTitle[sheetTitle];
    
    if (!sheet) {
      // Crear la hoja si no existe
      sheet = await this.doc.addSheet({ title: sheetTitle });
      console.log(`  [+] Sheet creado: ${sheetTitle}`);
    }

    // 2. Configurar la cabecera (Fila 1)
    // google-spreadsheet requiere un array simple para los headers: ['col1', 'col2']
    const headerArray = Array.isArray(headers[0]) ? headers[0] : headers;
    await sheet.setHeaderRow(headerArray);

    // 3. Limpiar las filas antiguas (esto borra todo EXCEPTO la fila 1 de headers)
    await sheet.clearRows();

    // 4. Escribir las filas nuevas
    if (rows && rows.length > 0) {
      // addRows acepta directamente tu array de arrays: [['a', 'b'], ['c', 'd']]
      await sheet.addRows(rows);
    }
    
    console.log(`  [✓] ${sheetTitle}: ${rows.length} filas escritas`);
  }

  async ensureSheets(sheetNames) {
    for (const name of sheetNames) {
      let sheet = this.doc.sheetsByTitle[name];
      if (!sheet) {
        sheet = await this.doc.addSheet({ title: name });
        console.log(`  [+] Sheet creado: ${name}`);
      }
    }
  }
}

module.exports = SheetWriter;
