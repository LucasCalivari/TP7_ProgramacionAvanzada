import { getDb } from '../../lib/db';

export const dynamic = 'force-dynamic';

const pct = (a, b) => (b ? (100 * a) / b : null);
const fmt = (v) => (v == null ? '-' : `${v.toFixed(1)}%`);

function agrupar(docs, clave, fn) {
  const m = new Map();
  for (const d of docs) {
    const k = clave(d);
    if (!m.has(k)) m.set(k, {});
    fn(m.get(k), d);
  }
  return [...m.entries()].sort(([a], [b]) => String(a).localeCompare(String(b)));
}
const sumar = (acc, d, campos) => campos.forEach((c) => { acc[c] = (acc[c] ?? 0) + (d[c] ?? 0); });

export default async function Analitica({ searchParams }) {
  const { zona = '', hub = '', desde = '' } = searchParams;
  const filtro = {};
  if (zona) filtro.zonaId = zona;
  if (hub) filtro.hubId = hub;
  if (desde) filtro.fecha = { $gte: desde };
  const db = await getDb();
  const docs = await db.collection('metricas_diarias').find(filtro).toArray();

  const campos1 = ['primerIntento_total', 'primerIntento_ok'];
  const porZonaFranja = agrupar(docs, (d) => `${d.zonaId} · ${d.franja}`, (a, d) => sumar(a, d, campos1));
  const porDia = agrupar(docs, (d) => d.fecha, (a, d) => sumar(a, d, campos1));

  const MOT = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];
  const motivos = agrupar(docs, (d) => d.zonaId, (a, d) => MOT.forEach((m) => { a[m] = (a[m] ?? 0) + (d.motivos?.[m] ?? 0); }));

  const sla = agrupar(docs, (d) => `${d.hubId} · ${d.fecha}`, (a, d) => sumar(a, d, ['sla_total', 'sla_ok']));
  const tiempos = agrupar(docs, (d) => d.hubId, (a, d) => sumar(a, d, ['tiempoHub_n', 'tiempoHub_sumMs'])).filter(([, v]) => v.tiempoHub_n);

  return (
    <>
      <h1>Analítica</h1>
      <form className="filtros">
        <select name="zona" defaultValue={zona}><option value="">Todas las zonas</option>{['Z1', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6'].map((z) => <option key={z}>{z}</option>)}</select>
        <select name="hub" defaultValue={hub}><option value="">Todos los hubs</option>{['H1', 'H2', 'H3'].map((h) => <option key={h}>{h}</option>)}</select>
        <input type="date" name="desde" defaultValue={desde} />
        <button>Filtrar</button>
      </form>

      <h2>1. Tasa de entrega en primer intento</h2>
      <table>
        <thead><tr><th>Zona · franja</th><th>Primeros intentos</th><th>Exitosos</th><th>Tasa</th><th></th></tr></thead>
        <tbody>
          {porZonaFranja.map(([k, v]) => {
            const p = pct(v.primerIntento_ok, v.primerIntento_total);
            return <tr key={k}><td>{k}</td><td>{v.primerIntento_total}</td><td>{v.primerIntento_ok ?? 0}</td><td>{fmt(p)}</td><td style={{ width: 160 }}><div className="bar" style={{ width: `${p ?? 0}%` }} /></td></tr>;
          })}
        </tbody>
      </table>
      <table>
        <thead><tr><th>Día</th><th>Primeros intentos</th><th>Exitosos</th><th>Tasa</th></tr></thead>
        <tbody>
          {porDia.map(([k, v]) => <tr key={k}><td>{k}</td><td>{v.primerIntento_total}</td><td>{v.primerIntento_ok ?? 0}</td><td>{fmt(pct(v.primerIntento_ok, v.primerIntento_total))}</td></tr>)}
        </tbody>
      </table>

      <h2>2. Distribución de motivos de fallo</h2>
      <table>
        <thead><tr><th>Zona</th>{MOT.map((m) => <th key={m}>{m}</th>)}</tr></thead>
        <tbody>
          {motivos.map(([k, v]) => <tr key={k}><td>{k}</td>{MOT.map((m) => <td key={m}>{v[m] ?? 0}</td>)}</tr>)}
        </tbody>
      </table>

      <h2>3. Cumplimiento de SLA</h2>
      <table>
        <thead><tr><th>Hub · día</th><th>Entregas</th><th>En término</th><th>Cumplimiento</th></tr></thead>
        <tbody>
          {sla.map(([k, v]) => <tr key={k}><td>{k}</td><td>{v.sla_total ?? 0}</td><td>{v.sla_ok ?? 0}</td><td>{fmt(pct(v.sla_ok ?? 0, v.sla_total))}</td></tr>)}
        </tbody>
      </table>

      <h2>4. Tiempo medio INGRESADO_HUB → EN_REPARTO por hub</h2>
      {tiempos.length ? (
        <table>
          <thead><tr><th>Hub</th><th>Envíos</th><th>Tiempo medio (horas simuladas)</th></tr></thead>
          <tbody>
            {tiempos.map(([k, v]) => <tr key={k}><td>{k}</td><td>{v.tiempoHub_n}</td><td>{(v.tiempoHub_sumMs / v.tiempoHub_n / 3600000).toFixed(2)}</td></tr>)}
          </tbody>
        </table>
      ) : (
        <p>Sin datos: activá el grupo nuevo con <code>docker compose --profile reproceso up -d analitica-tiempos</code>.</p>
      )}
    </>
  );
}
