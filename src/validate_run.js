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

  // ── Intentar monitor_runs (tabla ideal) ─────────────────────────────────
  let data = null;
  let source = 'monitor_runs';
  {
    const { data: d, error } = await supabase
      .from('monitor_runs')
      .select('*')
      .not('finished_at', 'is', null)
      .order('finished_at', { ascending: false })
      .limit(1)
      .single();

    if (!error && d) {
      data = d;
    } else if (error?.message?.includes('does not exist') || error?.code === '42P01'
               || error?.message?.includes('JSON object requested')
               || error?.message?.includes('schema cache')
               || error?.code === 'PGRST116') {
      console.log(`  ⚠️  monitor_runs no existe aún → fallback a execution_logs`);
      source = 'execution_logs';
    } else if (error) {
      console.error(`\n❌ Error consultando monitor_runs: ${error.message}\n`);
      process.exit(1);
    }
  }

  // ── Fallback: execution_logs ─────────────────────────────────────────────
  if (!data && source === 'execution_logs') {
    const { data: d2, error: e2 } = await supabase
      .from('execution_logs')
      .select('*')
      .order('executed_at', { ascending: false })
      .limit(1)
      .single();

    if (e2 || !d2) {
      console.error(`\n❌ No hay runs en execution_logs: ${e2?.message}\n`);
      process.exit(1);
    }
    // Normalizar campos
    data = {
      run_id:       d2.run_id,
      status:       d2.status === 'success' ? 'valid' : d2.status === 'partial' ? 'partial' : d2.status,
      finished_at:  d2.executed_at,
      started_at:   d2.executed_at,
      sellers_ok:   d2.sellers_success,
      sellers_total:d2.sellers_total,
      total_items:  d2.items_processed,
      invalid_reason: d2.message,
      _source:      'execution_logs',
    };
  }

  console.log(`\n  fuente:          ${data._source || source}`);
  console.log(`  run_id:          ${data.run_id}`);
  console.log(`  status:          ${data.status}`);
  console.log(`  finished_at:     ${(data.finished_at || '').substring(0, 19)}`);
  console.log(`  sellers_ok:      ${data.sellers_ok}/${data.sellers_total}`);
  console.log(`  total_items:     ${data.total_items}`);
  if (data.items_delta_pct) {
    const sign = data.items_delta_pct >= 0 ? '↓' : '↑';
    console.log(`  items vs previo: ${sign}${Math.abs(data.items_delta_pct).toFixed(1)}%`);
  }
  console.log('');

  if (data.status === 'running') {
    console.error('❌ Run aún en estado "running" — el monitor no terminó\n');
    process.exit(1);
  }

  // En fallback execution_logs, 'partial' se acepta como válido para la prueba
  const isValid = data.status === 'valid' || (data._source === 'execution_logs' && data.status === 'partial');
  if (!isValid) {
    console.error(`❌ RUN INVÁLIDO — abortando pipeline de detección`);
    console.error(`   Motivo: ${data.invalid_reason || data.status}\n`);
    process.exit(2);
  }

  if (data._source === 'execution_logs') {
    console.log(`⚠️  Usando execution_logs como fuente (monitor_runs aún no existe)`);
    console.log(`   Aplicar docs/sql/001_monitor_runs.sql para activar validación completa`);
  }
  console.log(`✅ Run válido — el detector puede continuar\n`);
  process.exit(0);
})();
