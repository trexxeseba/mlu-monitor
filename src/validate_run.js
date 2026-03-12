'use strict';

/**
 * validate_run.js
 *
 * Lee el run más reciente de monitor_runs y verifica si es válido.
 * Códigos de salida:
 *   0 → run válido, el detector puede continuar
 *   1 → error técnico (tabla no encontrada, DB caída, run no encontrado)
 *   2 → run inválido (scraping roto, demasiados sellers fallidos, caída de items)
 *
 * En GitHub Actions se usa con continue-on-error: true en el step de validate.
 * El detector solo corre si este script retorna 0.
 */

['SUPABASE_URL', 'SUPABASE_KEY'].forEach(k => {
  if (!process.env[k]) {
    console.error(`FATAL: falta variable ${k}`);
    process.exit(1);
  }
});

const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const SEP = '─'.repeat(70);

(async () => {
  console.log(`\n${SEP}`);
  console.log('VALIDATE RUN — INICIO');
  console.log(SEP);

  // Obtener el run más reciente (por finished_at desc)
  const { data, error } = await supabase
    .from('monitor_runs')
    .select('*')
    .not('finished_at', 'is', null)
    .order('finished_at', { ascending: false })
    .limit(1)
    .single();

  if (error) {
    console.error(`\n❌ Error consultando monitor_runs: ${error.message}`);
    console.error('   ¿Corriste el SQL de migración 001_monitor_runs.sql?\n');
    process.exit(1);
  }

  if (!data) {
    console.error('\n❌ No hay runs registrados en monitor_runs\n');
    process.exit(1);
  }

  console.log(`\n  run_id:          ${data.run_id}`);
  console.log(`  status:          ${data.status}`);
  console.log(`  started_at:      ${data.started_at}`);
  console.log(`  finished_at:     ${data.finished_at}`);
  console.log(`  sellers_ok:      ${data.sellers_ok}/${data.sellers_total}`);
  console.log(`  total_items:     ${data.total_items}`);
  if (data.items_delta_pct !== null && data.items_delta_pct !== undefined) {
    const sign = data.items_delta_pct >= 0 ? '↓' : '↑';
    console.log(`  items vs previo: ${sign}${Math.abs(data.items_delta_pct).toFixed(1)}%`);
  }
  if (data.invalid_reason) {
    console.log(`  motivo inválido: ${data.invalid_reason}`);
  }
  console.log('');

  if (data.status === 'running') {
    console.error('❌ Run aún en estado "running" — el monitor no terminó correctamente\n');
    process.exit(1);
  }

  if (data.status !== 'valid') {
    console.error(`❌ RUN INVÁLIDO — abortando pipeline de detección`);
    console.error(`   Motivo: ${data.invalid_reason || data.status}\n`);
    process.exit(2);
  }

  console.log(`✅ Run válido — el detector puede continuar\n`);
  process.exit(0);
})();
