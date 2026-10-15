// Cuarto indicador (reprocesamiento): tiempo medio INGRESADO_HUB -> EN_REPARTO por hub.
// Es un grupo NUEVO que lee envios.estados desde el inicio: completa el histórico sin tocar a los demás.
import { conectarMongo, correrConsumidor, unaVez, franjaDe, T } from './lib.js';

const SVC = 'analitica-tiempos';
process.env.SERVICE = SVC;
const mongo = await conectarMongo();
const { db } = mongo;

await correrConsumidor({
  service: SVC,
  groupId: 'analitica-tiempos',
  topics: [T.estados],
  fromBeginning: true,
  handler: (ev) => {
    if (ev.tipo === 'INGRESADO_HUB') {
      return unaVez(mongo, SVC, ev.eventId, (session) =>
        db.collection('tiempos_estado').updateOne(
          { shipmentId: ev.shipmentId },
          { $set: { ingresoEn: ev.ocurridoEn } },
          { upsert: true, session },
        ));
    }
    if (ev.tipo !== 'EN_REPARTO' || ev.intento !== 1) return;
    return unaVez(mongo, SVC, ev.eventId, async (session) => {
      const t = await db.collection('tiempos_estado').findOne({ shipmentId: ev.shipmentId }, { session });
      if (!t) return;
      const ms = Date.parse(ev.ocurridoEn) - Date.parse(t.ingresoEn);
      if (ms < 0) return;
      await db.collection('metricas_diarias').updateOne(
        { fecha: ev.ocurridoEn.slice(0, 10), hubId: ev.hubId, zonaId: ev.zonaId, franja: franjaDe(Date.parse(ev.ocurridoEn)) },
        { $inc: { tiempoHub_n: 1, tiempoHub_sumMs: ms } },
        { upsert: true, session },
      );
    });
  },
});
