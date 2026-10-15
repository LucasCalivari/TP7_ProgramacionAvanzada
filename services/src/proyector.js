// Proyector (grupo "proyector"): mantiene envios, alertas y notificaciones en MongoDB.
import { conectarMongo, correrConsumidor, unaVez, T } from './lib.js';

const SVC = 'proyector';
process.env.SERVICE = SVC;
const mongo = await conectarMongo();
const { db } = mongo;

async function aplicarEstado(ev, session) {
  const envios = db.collection('envios');
  const actual = await envios.findOne({ shipmentId: ev.shipmentId }, { session });
  // regla de orden: secuencia menor o igual a la ya aplicada se descarta
  if (actual?.secuencia != null && actual.secuencia >= ev.secuencia) return;

  const set = {
    estado: ev.tipo,
    secuencia: ev.secuencia,
    zonaId: ev.zonaId,
    hubId: ev.hubId,
    repartidorId: ev.repartidorId,
    ultimoEventoEn: ev.ocurridoEn,
    actualizadoEn: new Date(),
  };
  if (ev.intento != null) set.intentos = ev.intento;
  if (ev.tipo === 'ENTREGA_FALLIDA') set.motivoFallo = ev.motivo;
  const hist = {
    eventId: ev.eventId, tipo: ev.tipo, ocurridoEn: ev.ocurridoEn, secuencia: ev.secuencia,
    ...(ev.motivo && { motivo: ev.motivo }), ...(ev.intento != null && { intento: ev.intento }),
  };
  await envios.updateOne(
    { shipmentId: ev.shipmentId },
    { $set: set, $push: { historial: hist }, $setOnInsert: { shipmentId: ev.shipmentId, creadoEn: new Date() } },
    { upsert: true, session },
  );
}

async function aplicar(ev, topic, session) {
  const envios = db.collection('envios');
  const base = { $setOnInsert: { shipmentId: ev.shipmentId, creadoEn: new Date() } };
  switch (topic) {
    case T.estados:
      return aplicarEstado(ev, session);
    case T.promesas:
      return envios.updateOne(
        { shipmentId: ev.shipmentId },
        { $set: { promesa: { venceEn: ev.venceEn }, zonaId: ev.zonaId, hubId: ev.hubId, actualizadoEn: new Date() }, ...base },
        { upsert: true, session },
      );
    case T.reprog: {
      const set = { reprogramacion: { tipo: ev.tipo, proximoIntento: ev.proximoIntento ?? null, detalle: ev.detalle ?? null }, actualizadoEn: new Date() };
      if (ev.tipo === 'RETENIDO') set.retenido = true;
      if (ev.tipo === 'DEVOLUCION') set.devolucion = true;
      return envios.updateOne(
        { shipmentId: ev.shipmentId },
        { $set: set, $push: { historial: { eventId: ev.eventId, tipo: ev.tipo, ocurridoEn: ev.ocurridoEn } }, ...base },
        { upsert: true, session },
      );
    }
    case T.alertas: {
      const alertas = db.collection('alertas');
      if (ev.tipo === 'ALERTA_ABIERTA') {
        // $setOnInsert del estado: si el cierre llegó antes (reconstrucción), no se reabre
        return alertas.updateOne(
          { shipmentId: ev.shipmentId, regla: ev.regla },
          { $set: { prioridad: ev.prioridad, causa: ev.causa, abiertaEn: ev.ocurridoEn }, $setOnInsert: { estado: 'ABIERTA' } },
          { upsert: true, session },
        );
      }
      for (const regla of ev.reglas ?? []) {
        await alertas.updateOne(
          { shipmentId: ev.shipmentId, regla },
          { $set: { estado: 'CERRADA', cerradaEn: ev.ocurridoEn } },
          { upsert: true, session },
        );
      }
      return;
    }
    case T.notifs:
      return db.collection('notificaciones').updateOne(
        { eventId: ev.eventId },
        { $setOnInsert: { eventId: ev.eventId, shipmentId: ev.shipmentId, motivo: ev.motivo ?? null, mensaje: ev.mensaje, canal: ev.canal, opciones: ev.opciones ?? [], enviadaEn: ev.ocurridoEn } },
        { upsert: true, session },
      );
  }
}

await correrConsumidor({
  service: SVC,
  groupId: 'proyector',
  topics: [T.estados, T.promesas, T.reprog, T.alertas, T.notifs],
  fromBeginning: true,
  handler: (ev, ctx) => unaVez(mongo, SVC, ev.eventId, (session) => aplicar(ev, ctx.topic, session)),
});
