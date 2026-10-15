// Servicio de notificaciones (grupo "notificaciones"): registra avisos simulados ante fallos y alertas.
import { conectarMongo, correrConsumidor, yaProcesado, marcarProcesado, T, log } from './lib.js';

const SVC = 'notificaciones';
process.env.SERVICE = SVC;
const { db } = await conectarMongo();

await correrConsumidor({
  service: SVC,
  groupId: 'notificaciones',
  topics: [T.estados, T.alertas],
  fromBeginning: true,
  handler: async (ev, { topic, producer }) => {
    let notif = null;
    if (topic === T.estados && ev.tipo === 'ENTREGA_FALLIDA') {
      notif = {
        motivo: ev.motivo,
        mensaje: `No pudimos entregar tu envío ${ev.shipmentId} (${ev.motivo}). Podés reprogramar la entrega.`,
        opciones: ['Reprogramar mañana', 'Reprogramar tarde', 'Retirar en sucursal'],
      };
    } else if (topic === T.alertas && ev.tipo === 'ALERTA_ABIERTA') {
      notif = {
        motivo: ev.regla,
        mensaje: `Tu envío ${ev.shipmentId} podría demorarse (${ev.causa}). Estamos trabajando para cumplir la fecha prometida.`,
      };
    }
    if (!notif || (await yaProcesado(db, SVC, ev.eventId))) return;

    // eventId determinista: si hay un reintento tras una caída, el proyector descarta el duplicado
    const salida = {
      eventId: `notif-${ev.eventId}`,
      shipmentId: ev.shipmentId,
      tipo: 'NOTIFICACION_ENVIADA',
      ocurridoEn: ev.ocurridoEn,
      canal: 'email',
      version: 1,
      ...notif,
    };
    await producer.send({ topic: T.notifs, messages: [{ key: ev.shipmentId, value: JSON.stringify(salida) }] });
    await marcarProcesado(db, SVC, ev.eventId);
    log(SVC, 'notificación registrada', { shipmentId: ev.shipmentId, origen: ev.eventId });
  },
});
