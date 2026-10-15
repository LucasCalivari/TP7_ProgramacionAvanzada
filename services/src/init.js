// Tarea de arranque: crea los 6 topics (retención/compactación según el catálogo) e índices de MongoDB.
import { kafka, conectarMongo, T, log } from './lib.js';

const DIA = 24 * 3600 * 1000;
const topics = [
  { topic: T.estados, numPartitions: 3, configEntries: [{ name: 'retention.ms', value: String(30 * DIA) }] },
  { topic: T.promesas, numPartitions: 3, configEntries: [{ name: 'cleanup.policy', value: 'compact' }] },
  { topic: T.reprog, numPartitions: 3, configEntries: [{ name: 'retention.ms', value: String(30 * DIA) }] },
  { topic: T.alertas, numPartitions: 3, configEntries: [{ name: 'retention.ms', value: String(7 * DIA) }] },
  { topic: T.notifs, numPartitions: 3, configEntries: [{ name: 'retention.ms', value: String(7 * DIA) }] },
  { topic: T.dlq, numPartitions: 1, configEntries: [{ name: 'retention.ms', value: String(30 * DIA) }] },
];

const admin = kafka.admin();
await admin.connect();
await admin.createTopics({ topics: topics.map((t) => ({ ...t, replicationFactor: 1 })), waitForLeaders: true });
await admin.disconnect();
log('init', 'topics listos', { topics: topics.map((t) => t.topic) });

const { client, db } = await conectarMongo();
await db.collection('envios').createIndex({ shipmentId: 1 }, { unique: true });
await db.collection('envios').createIndex({ estado: 1, zonaId: 1 });
await db.collection('envios').createIndex({ 'promesa.venceEn': 1 });
await db.collection('alertas').createIndex({ shipmentId: 1, regla: 1 }, { unique: true });
await db.collection('alertas').createIndex({ estado: 1, prioridad: 1 });
await db.collection('notificaciones').createIndex({ eventId: 1 }, { unique: true });
await db.collection('notificaciones').createIndex({ shipmentId: 1 });
await db.collection('metricas_diarias').createIndex({ fecha: 1, hubId: 1, zonaId: 1, franja: 1 }, { unique: true });
await db.collection('eventos_procesados').createIndex({ consumidor: 1, eventId: 1 }, { unique: true });
await db.collection('eventos_procesados').createIndex({ procesadoEn: 1 }, { expireAfterSeconds: 7 * 24 * 3600 });
// colecciones auxiliares de estado interno (respaldo del monitor y join del cuarto indicador)
await db.collection('monitor_estado').createIndex({ shipmentId: 1 }, { unique: true });
await db.collection('tiempos_estado').createIndex({ shipmentId: 1 }, { unique: true });
log('init', 'indices listos');
await client.close();
