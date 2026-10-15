import { Kafka, logLevel } from 'kafkajs';
import { MongoClient } from 'mongodb';

export const T = {
  estados: 'envios.estados',
  promesas: 'envios.promesas',
  reprog: 'envios.reprogramaciones',
  alertas: 'alertas.sla',
  notifs: 'notificaciones.enviadas',
  dlq: 'envios.estados.dlq',
};

export const ESTADOS = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'];
export const MOTIVOS = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];

// 6 zonas, 3 hubs. La tasa de fallo base va de 5% a 30%.
export const ZONAS = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3].map((tasa, i) => ({
  zonaId: `Z${i + 1}`,
  hubId: `H${Math.floor(i / 2) + 1}`,
  tasa,
}));

export const HORA = 3600000;

export const kafka = new Kafka({
  clientId: process.env.SERVICE || 'rutasur',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 30, initialRetryTime: 500 },
});

export async function conectarMongo() {
  const client = new MongoClient(process.env.MONGO_URL || 'mongodb://mongo:27017/?replicaSet=rs0');
  await client.connect();
  return { client, db: client.db('rutasur') };
}

export function log(svc, msg, extra = {}) {
  console.log(JSON.stringify({ ts: new Date().toISOString(), svc, msg, ...extra }));
}

// ---- Reglas de negocio compartidas (simulador y servicio de reprogramación) ----

export const franjaDe = (ms) => (new Date(ms).getUTCHours() < 14 ? 'MANANA' : 'TARDE');

function diaHabilSiguiente(ms, franja) {
  const d = new Date(ms);
  do d.setUTCDate(d.getUTCDate() + 1);
  while (d.getUTCDay() === 0 || d.getUTCDay() === 6);
  d.setUTCHours(franja === 'MANANA' ? 10 : 16, 0, 0, 0);
  return d.toISOString();
}

/** Decide qué pasa después de una entrega fallida. */
export function decidirReprogramacion(motivo, intento, ocurridoEn) {
  const ms = Date.parse(ocurridoEn);
  const franja = franjaDe(ms);
  if (intento >= 3) return { tipo: 'DEVOLUCION', detalle: 'tercer intento fallido' };
  switch (motivo) {
    case 'AUSENTE':
      return {
        tipo: 'REPROGRAMADO',
        fecha: diaHabilSiguiente(ms, franja === 'MANANA' ? 'TARDE' : 'MANANA'),
        franja: franja === 'MANANA' ? 'TARDE' : 'MANANA',
      };
    case 'DIRECCION_INCORRECTA':
      return { tipo: 'RETENIDO', detalle: 'retenido hasta corrección de dirección' };
    case 'RECHAZADO':
      return { tipo: 'DEVOLUCION', detalle: 'rechazado por el destinatario' };
    default: // ZONA_INACCESIBLE: la consigna no define regla; se reintenta el siguiente día hábil, misma franja
      return { tipo: 'REPROGRAMADO', fecha: diaHabilSiguiente(ms, franja), franja };
  }
}

// ---- Validación de esquema (JSON) ----

export function validar(ev, topic) {
  if (!ev || typeof ev !== 'object') return 'el mensaje no es un objeto';
  for (const k of ['eventId', 'shipmentId', 'tipo', 'ocurridoEn']) {
    if (typeof ev[k] !== 'string' || !ev[k]) return `campo ${k} ausente o inválido`;
  }
  if (Number.isNaN(Date.parse(ev.ocurridoEn))) return 'ocurridoEn inválido';
  if (ev.version !== 1) return 'versión de esquema no soportada';
  if (topic === T.estados) {
    if (!ESTADOS.includes(ev.tipo)) return `tipo de estado desconocido: ${ev.tipo}`;
    if (!Number.isInteger(ev.secuencia)) return 'secuencia ausente o inválida';
    if (ev.tipo === 'ENTREGA_FALLIDA' && !MOTIVOS.includes(ev.motivo)) return 'motivo de fallo inválido';
  }
  if (topic === T.promesas && Number.isNaN(Date.parse(ev.venceEn))) return 'venceEn inválido';
  return null;
}

/**
 * Consumidor genérico: valida, deriva inválidos a la DLQ, registra eventId/partición/offset
 * y confirma el offset a mano SOLO después de que el handler terminó (al menos una vez).
 */
export async function correrConsumidor({ service, groupId, topics, fromBeginning = false, handler }) {
  const producer = kafka.producer();
  await producer.connect();
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString() ?? '';
      let ev = null;
      let motivo = null;
      try {
        ev = JSON.parse(raw);
      } catch {
        motivo = 'JSON inválido';
      }
      if (!motivo) motivo = validar(ev, topic);

      if (motivo) {
        await producer.send({
          topic: T.dlq,
          messages: [{
            key: message.key?.toString() ?? 'desconocido',
            value: JSON.stringify({ consumidor: groupId, topicOrigen: topic, partition, offset: message.offset, motivo, original: raw }),
          }],
        });
        log(service, 'evento enviado a DLQ', { topic, partition, offset: message.offset, motivo });
      } else {
        log(service, 'evento', { eventId: ev.eventId, tipo: ev.tipo, topic, partition, offset: message.offset });
        await handler(ev, { topic, partition, offset: message.offset, producer });
      }
      await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
    },
  });
  return { consumer, producer };
}

// ---- Idempotencia con eventos_procesados ----

export const yaProcesado = (db, consumidor, eventId) =>
  db.collection('eventos_procesados').findOne({ consumidor, eventId }, { projection: { _id: 1 } });

export const marcarProcesado = (db, consumidor, eventId, session) =>
  db.collection('eventos_procesados').updateOne(
    { consumidor, eventId },
    { $setOnInsert: { consumidor, eventId, procesadoEn: new Date() } },
    { upsert: true, session },
  );

/** Marca el evento y aplica los efectos en una única transacción: sin duplicados ni pérdidas. */
export async function unaVez({ client, db }, consumidor, eventId, fn) {
  if (await yaProcesado(db, consumidor, eventId)) return false;
  const session = client.startSession();
  try {
    await session.withTransaction(async () => {
      await fn(session);
      await marcarProcesado(db, consumidor, eventId, session);
    });
  } finally {
    await session.endSession();
  }
  return true;
}
