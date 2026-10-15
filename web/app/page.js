import { getDb } from '../lib/db';
import Live from '../components/Live';

export const dynamic = 'force-dynamic';

const ORDEN = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'];
const PESO = { CRITICA: 3, ALTA: 2, MEDIA: 1 };

export default async function Torre() {
  const db = await getDb();
  const [porEstado, alertas, recientes] = await Promise.all([
    db.collection('envios').aggregate([{ $group: { _id: '$estado', n: { $sum: 1 } } }]).toArray(),
    db.collection('alertas').find({ estado: 'ABIERTA' }).limit(300).toArray(),
    db.collection('envios').find({ estado: { $ne: null } }).sort({ actualizadoEn: -1 }).limit(15).toArray(),
  ]);
  const conteo = Object.fromEntries(porEstado.map((e) => [e._id, e.n]));
  alertas.sort((a, b) => (PESO[b.prioridad] - PESO[a.prioridad]) || String(b.abiertaEn).localeCompare(String(a.abiertaEn)));

  return (
    <>
      <h1>Torre de control <Live /></h1>
      <div className="cards">
        {ORDEN.map((e) => (
          <div className="card" key={e}><b>{conteo[e] ?? 0}</b>{e}</div>
        ))}
      </div>

      <h2>Alertas abiertas ({alertas.length})</h2>
      <table>
        <thead><tr><th>Prioridad</th><th>Envío</th><th>Regla</th><th>Causa</th></tr></thead>
        <tbody>
          {alertas.slice(0, 25).map((a) => (
            <tr key={a._id.toString()}>
              <td className={a.prioridad}>{a.prioridad}</td>
              <td><a href={`/envio/${a.shipmentId}`}>{a.shipmentId}</a></td>
              <td>{a.regla}</td>
              <td>{a.causa}</td>
            </tr>
          ))}
          {!alertas.length && <tr><td colSpan="4">Sin alertas abiertas</td></tr>}
        </tbody>
      </table>

      <h2>Últimos eventos</h2>
      <table>
        <thead><tr><th>Envío</th><th>Estado</th><th>Zona</th><th>Hub</th><th>Intentos</th><th>Ocurrido (reloj simulado)</th><th></th></tr></thead>
        <tbody>
          {recientes.map((e) => (
            <tr key={e.shipmentId}>
              <td><a href={`/envio/${e.shipmentId}`}>{e.shipmentId}</a></td>
              <td>{e.estado}{e.motivoFallo ? ` (${e.motivoFallo})` : ''}</td>
              <td>{e.zonaId}</td>
              <td>{e.hubId}</td>
              <td>{e.intentos ?? '-'}</td>
              <td>{e.ultimoEventoEn}</td>
              <td><a href={`/tracking/${e.shipmentId}`}>tracking público</a></td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
