#!/usr/bin/env node

/**
 * Script para descargar tablas de Supabase a CSV
 * Ejecutar: node download-supabase.js
 */

const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');

const SUPABASE_URL = 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Convertir array a CSV
function arrayToCSV(data, headers) {
  if (!data || data.length === 0) return headers.join(',');
  
  const rows = data.map(row =>
    headers.map(header => {
      const value = row[header];
      if (value === null || value === undefined) return '';
      const str = String(value).replace(/"/g, '""');
      return str.includes(',') || str.includes('"') ? `"${str}"` : str;
    }).join(',')
  );
  
  return [headers.join(','), ...rows].join('\n');
}

async function downloadTables() {
  console.log('📥 Descargando tablas de Supabase...\n');

  try {
    // 1. SELLERS
    console.log('[→] Descargando sellers...');
    const { data: sellers, error: sellersErr } = await supabase
      .from('sellers')
      .select('*')
      .order('id', { ascending: true });
    
    if (sellersErr) throw new Error(`sellers: ${sellersErr.message}`);
    
    const sellersHeaders = sellers.length > 0 
      ? Object.keys(sellers[0]) 
      : ['id', 'seller_id', 'nombre_real', 'nickname'];
    
    const sellersCSV = arrayToCSV(sellers, sellersHeaders);
    fs.writeFileSync('SELLERS.csv', sellersCSV);
    console.log(`  ✅ SELLERS.csv (${sellers.length} filas)`);

    // 2. SNAPSHOTS
    console.log('[→] Descargando snapshots...');
    const { data: snapshots, error: snapshotsErr } = await supabase
      .from('snapshots')
      .select('*')
      .order('timestamp', { ascending: false });
    
    if (snapshotsErr) throw new Error(`snapshots: ${snapshotsErr.message}`);
    
    const snapshotsHeaders = snapshots.length > 0
      ? Object.keys(snapshots[0])
      : ['id', 'seller_id', 'timestamp', 'status'];
    
    const snapshotsCSV = arrayToCSV(snapshots, snapshotsHeaders);
    fs.writeFileSync('SNAPSHOTS.csv', snapshotsCSV);
    console.log(`  ✅ SNAPSHOTS.csv (${snapshots.length} filas)`);

    // 3. RESUMEN
    console.log('[→] Generando resumen...');
    const resumenData = sellers.map(seller => {
      const sellerSnaps = snapshots.filter(s => s.seller_id === seller.id);
      return {
        seller_id: seller.seller_id,
        nombre: seller.nombre_real || seller.nickname || '—',
        total_snapshots: sellerSnaps.length,
        ultimo_update: sellerSnaps.length > 0 
          ? sellerSnaps[0].timestamp 
          : 'N/A'
      };
    });
    
    const resumenHeaders = ['seller_id', 'nombre', 'total_snapshots', 'ultimo_update'];
    const resumenCSV = arrayToCSV(resumenData, resumenHeaders);
    fs.writeFileSync('RESUMEN.csv', resumenCSV);
    console.log(`  ✅ RESUMEN.csv (${resumenData.length} filas)`);

    console.log('\n✅ Descarga completada!\n');
    console.log('Archivos generados:');
    console.log('  📄 SELLERS.csv');
    console.log('  📄 SNAPSHOTS.csv');
    console.log('  📄 RESUMEN.csv');
    console.log('\nAbre cualquiera en Excel o Google Sheets\n');

  } catch (err) {
    console.error(`\n❌ Error: ${err.message}`);
    process.exit(1);
  }
}

downloadTables();
