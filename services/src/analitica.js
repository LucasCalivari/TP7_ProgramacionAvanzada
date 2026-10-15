// Servicio de analítica (grupo "analitica"): agregados incrementales en metricas_diarias.
import { conectarMongo, correrConsumidor, unaVez, franjaDe, T } from './lib.js';

const SVC = 'analitica';
process.env.SERVICE = SVC;
const mongo = await conectarMongo();
const { db } = mongo;

await correrConsumidor({
  service: SVC,
  groupId: 'analitica',
  topics: [T.estados],
  fromBeginning: true,
  handler: (ev) => {
    if (ev.tipo !== 'ENTREGADO' && ev.tipo !== 'ENTREGA_FALLIDA') return;
    const inc = {};
    if (ev.intento === 1) {
      inc.primerIntento_total = 1;
      if (ev.tipo === 'ENTREGADO') inc.primerIntento_ok = 1;
    }
    if (ev.tipo === 'ENTREGA_FALLIDA') {
      inc.fallos_total = 1;
      inc[`motivos.${ev.motivo}`] = 1;
    } else {
      inc.sla_total = 1;
      if (ev.venceEn && Date.parse(ev.ocurridoEn) <= Date.parse(ev.venceEn)) inc.sla_ok = 1;
    }
    const filtro = {
      fecha: ev.ocurridoEn.slice(0, 10),
      hubId: ev.hubId,
      zonaId: ev.zonaId,
      franja: franjaDe(Date.parse(ev.ocurridoEn)),
    };
    return unaVez(mongo, SVC, ev.eventId, (session) =>
      db.collection('metricas_diarias').updateOne(filtro, { $inc: inc }, { upsert: true, session }));
  },
});
