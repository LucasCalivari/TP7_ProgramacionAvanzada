// Servicio de reprogramación (grupo "reprogramacion"): decide el próximo intento tras una entrega fallida.
import { conectarMongo, correrConsumidor, yaProcesado, marcarProcesado, decidirReprogramacion, T, log } from './lib.js';

const SVC = 'reprogramacion';
process.env.SERVICE = SVC;
const { db } = await conectarMongo();

await correrConsumidor({
  service: SVC,
  groupId: 'reprogramacion',
  topics: [T.estados],
  fromBeginning: true,
  handler: async (ev, { producer }) => {
    if (ev.tipo !== 'ENTREGA_FALLIDA' || (await yaProcesado(db, SVC, ev.eventId))) return;
    const d = decidirReprogramacion(ev.motivo, ev.intento ?? 1, ev.ocurridoEn);
    const salida = {
      eventId: `repro-${ev.eventId}`,
      shipmentId: ev.shipmentId,
      tipo: d.tipo, // REPROGRAMADO | RETENIDO | DEVOLUCION
      ocurridoEn: ev.ocurridoEn,
      motivo: ev.motivo,
      intento: ev.intento ?? 1,
      proximoIntento: d.fecha ? { fecha: d.fecha, franja: d.franja } : null,
      detalle: d.detalle ?? null,
      version: 1,
    };
    await producer.send({ topic: T.reprog, messages: [{ key: ev.shipmentId, value: JSON.stringify(salida) }] });
    await marcarProcesado(db, SVC, ev.eventId);
    log(SVC, 'reprogramación decidida', { shipmentId: ev.shipmentId, decision: d.tipo });
  },
});
