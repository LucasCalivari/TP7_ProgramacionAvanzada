import { getDb } from '../../../lib/db';
import Live from '../../../components/Live';

export const dynamic = 'force-dynamic';

export default async function Detalle({ params }) {
  const id = params.id;
  const db = await getDb();
  const [envio, alertas, notifs] = await Promise.all([
    db.collection('envios').findOne({ shipmentId: id }),
    db.collection('alertas').find({ shipmentId: id }).toArray(),
    db.collection('notificaciones').find({ shipmentId: id }).sort({ enviadaEn: 1 }).toArray(),
  ]);
  if (!envio) return <><h1>Envío {id}</h1><p>No encontrado.</p></>;

  const historial = [...(envio.historial ?? [])].sort((a, b) => String(a.ocurridoEn).localeCompare(String(b.ocurridoEn)));
  return (
    <>
      <h1>Envío {id} <Live shipmentId={id} /></h1>
      <div className="cards">
        <div className="card"><b>{envio.estado}</b>Estado</div>
        <div className="card"><b>{envio.intentos ?? 0}</b>Intentos</div>
        <div className="card"><b>{envio.zonaId}</b>Zona ({envio.hubId})</div>
        <div className="card"><b style={{ fontSize: 14 }}>{envio.promesa?.venceEn ?? '-'}</b>Promesa vence</div>
      </div>
      {envio.reprogramacion && (
        <p>
          <strong>Reprogramación:</strong> {envio.reprogramacion.tipo}
          {envio.reprogramacion.proximoIntento && ` — próximo intento ${envio.reprogramacion.proximoIntento.fecha} (${envio.reprogramacion.proximoIntento.franja})`}
          {envio.reprogramacion.detalle && ` — ${envio.reprogramacion.detalle}`}
        </p>
      )}

      <h2>Línea de tiempo</h2>
      <table>
        <thead><tr><th>Ocurrido</th><th>Evento</th><th>Detalle</th><th>Sec.</th></tr></thead>
        <tbody>
          {historial.map((h) => (
            <tr key={h.eventId}>
              <td>{h.ocurridoEn}</td><td>{h.tipo}</td>
              <td>{[h.motivo, h.intento != null && `intento ${h.intento}`].filter(Boolean).join(' · ')}</td>
              <td>{h.secuencia ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h2>Alertas</h2>
      <table>
        <thead><tr><th>Regla</th><th>Prioridad</th><th>Estado</th><th>Causa</th></tr></thead>
        <tbody>
          {alertas.map((a) => (
            <tr key={a.regla}><td>{a.regla}</td><td className={a.prioridad}>{a.prioridad}</td><td>{a.estado}</td><td>{a.causa}</td></tr>
          ))}
          {!alertas.length && <tr><td colSpan="4">Sin alertas</td></tr>}
        </tbody>
      </table>

      <h2>Notificaciones</h2>
      <table>
        <thead><tr><th>Enviada</th><th>Canal</th><th>Mensaje</th></tr></thead>
        <tbody>
          {notifs.map((n) => (
            <tr key={n.eventId}><td>{n.enviadaEn}</td><td>{n.canal}</td><td>{n.mensaje}</td></tr>
          ))}
          {!notifs.length && <tr><td colSpan="3">Sin notificaciones</td></tr>}
        </tbody>
      </table>
      <p><a href={`/tracking/${id}`}>Ver tracking público</a></p>
    </>
  );
}
