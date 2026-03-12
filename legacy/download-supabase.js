#!/usr/bin/env node

/**
 * Export items VENDIDOS de Supabase → CSV
 * Compara: AYER vs HOY
 * Ejecutar: npm run export
 */

const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');

const SUPABASE_URL = 'https://drggfikyqtooqxqqwefy.supabase.co';
const SUPABASE_KEY = 'sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

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

function getYesterdayRange() {
  const hoy = new Date();
  const ayer = new Date(hoy);
  ayer.setDate(ayer.getDate() - 1);
  
  const ayerStart = new Date(ayer.getFullYear(), ayer.getMonth(), ayer.getDate(), 0, 0, 0);
  const ayerEnd = new Date(ayer.getFullYear(), ayer.getMonth(), ayer.getDate(), 23, 59, 59);
  
  return { start: ayerStart.toISOString(), end: ayerEnd.toISOString() };
}

function getTodayRange() {
  const hoy = new Date();
  const hoyStart = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate(), 0, 0, 0);
  const hoyEnd = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate(), 23, 59, 59);
  
  return { start: hoyStart.toISOString(), end: hoyEnd.toISOString() };
}

async function exportVendidosComparativa() {
  console.log('📊 Vendidos: AYER vs HOY\n');

  try {
    // Traer sellers
    const { data: sellers, error: sellersErr } = await supabase
      .from('sellers')
      .select('id, seller_id, nombre_real, nickname')
      .order('id', { ascending: true });
    
    if (sellersErr) throw new Error(`sellers: ${sellersErr.message}`);

    const ayer = getYesterdayRange();
    const hoy = getTodayRange();

    // Vendidos AYER
    console.log(`[→] Ayer (${new Date(ayer.start).toLocaleDateString('es-UY')})...`);
    const { data: vendidosAyer, error: ayerErr } = await supabase
      .from('snapshots')
      .select('*')
      .eq('status', 'sold')
      .gte('timestamp', ayer.start)
      .lte('timestamp', ayer.end)
      .order('timestamp', { ascending: false });
    
    if (ayerErr) throw new Error(`ayer: ${ayerErr.message}`);
    console.log(`  ✓ ${vendidosAyer.length} items vendidos\n`);

    // Vendidos HOY
    console.log(`[→] Hoy (${new Date(hoy.start).toLocaleDateString('es-UY')})...`);
    const { data: vendidosHoy, error: hoyErr } = await supabase
      .from('snapshots')
      .select('*')
      .eq('status', 'sold')
      .gte('timestamp', hoy.start)
      .lte('timestamp', hoy.end)
      .order('timestamp', { ascending: false });
    
    if (hoyErr) throw new Error(`hoy: ${hoyErr.message}`);
    console.log(`  ✓ ${vendidosHoy.length} items vendidos\n`);

    // Procesar AYER
    const vendidosAyerEnriquecido = vendidosAyer.map(item => {
      const seller = sellers.find(s => s.id === item.seller_id);
      return {
        fecha: item.timestamp ? new Date(item.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' }) : '—',
        seller_id: item.seller_id,
        vendedor: seller?.nombre_real || seller?.nickname || '—',
        item_id: item.meli_item_id || item.item_id || '—',
        titulo: (item.title || '—').substring(0, 80),
        precio: item.price || '—'
      };
    });

    // Procesar HOY
    const vendidosHoyEnriquecido = vendidosHoy.map(item => {
      const seller = sellers.find(s => s.id === item.seller_id);
      return {
        fecha: item.timestamp ? new Date(item.timestamp).toLocaleString('es-UY', { timeZone: 'America/Montevideo' }) : '—',
        seller_id: item.seller_id,
        vendedor: seller?.nombre_real || seller?.nickname || '—',
        item_id: item.meli_item_id || item.item_id || '—',
        titulo: (item.title || '—').substring(0, 80),
        precio: item.price || '—'
      };
    });

    const headers = ['fecha', 'seller_id', 'vendedor', 'item_id', 'titulo', 'precio'];
    
    const csvAyer = arrayToCSV(vendidosAyerEnriquecido, headers);
    fs.writeFileSync('VENDIDOS_AYER.csv', csvAyer);
    console.log('[✓] VENDIDOS_AYER.csv\n');

    const csvHoy = arrayToCSV(vendidosHoyEnriquecido, headers);
    fs.writeFileSync('VENDIDOS_HOY.csv', csvHoy);
    console.log('[✓] VENDIDOS_HOY.csv\n');

    // RESUMEN COMPARATIVO
    console.log('[→] Generando resumen comparativo...');
    const resumenAyer = {};
    const resumenHoy = {};

    vendidosAyerEnriquecido.forEach(v => {
      if (!resumenAyer[v.seller_id]) {
        resumenAyer[v.seller_id] = { vendedor: v.vendedor, count: 0 };
      }
      resumenAyer[v.seller_id].count++;
    });

    vendidosHoyEnriquecido.forEach(v => {
      if (!resumenHoy[v.seller_id]) {
        resumenHoy[v.seller_id] = { vendedor: v.vendedor, count: 0 };
      }
      resumenHoy[v.seller_id].count++;
    });

    const allSellerIds = new Set([...Object.keys(resumenAyer), ...Object.keys(resumenHoy)]);
    const resumenComparativo = Array.from(allSellerIds).map(sellerId => ({
      seller_id: sellerId,
      vendedor: resumenAyer[sellerId]?.vendedor || resumenHoy[sellerId]?.vendedor || '—',
      ayer: resumenAyer[sellerId]?.count || 0,
      hoy: resumenHoy[sellerId]?.count || 0,
      diferencia: (resumenHoy[sellerId]?.count || 0) - (resumenAyer[sellerId]?.count || 0)
    })).sort((a, b) => b.hoy - a.hoy);

    const resumenHeaders = ['seller_id', 'vendedor', 'ayer', 'hoy', 'diferencia'];
    const csvResumen = arrayToCSV(resumenComparativo, resumenHeaders);
    
    fs.writeFileSync('RESUMEN_COMPARATIVO.csv', csvResumen);
    console.log('[✓] RESUMEN_COMPARATIVO.csv\n');

    console.log('✅ EXPORTACIÓN COMPLETADA\n');
    console.log('Archivos:');
    console.log(`  📄 VENDIDOS_AYER.csv — ${vendidosAyerEnriquecido.length} items`);
    console.log(`  📄 VENDIDOS_HOY.csv — ${vendidosHoyEnriquecido.length} items`);
    console.log(`  📄 RESUMEN_COMPARATIVO.csv — totales por vendedor\n`);

  } catch (err) {
    console.error(`\n❌ Error: ${err.message}`);
    process.exit(1);
  }
}

exportVendidosComparativa();
