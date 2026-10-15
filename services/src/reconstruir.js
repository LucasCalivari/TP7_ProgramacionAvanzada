// Reconstrucción de las vistas del proyector: borra envios/alertas/notificaciones y rebobina su grupo.
// Uso (con el proyector detenido):
//   docker compose stop proyector
//   docker compose run --rm proyector node src/reconstruir.js
//   docker compose start proyector
import { kafka, conectarMongo, T, log } from './lib.js';

const { client, db } = await conectarMongo();
for (const c of ['envios', 'alertas', 'notificaciones']) await db.collection(c).deleteMany({});
await db.collection('eventos_procesados').deleteMany({ consumidor: 'proyector' });

const admin = kafka.admin();
await admin.connect();
for (const topic of [T.estados, T.promesas, T.reprog, T.alertas, T.notifs]) {
  await admin.resetOffsets({ groupId: 'proyector', topic, earliest: true });
}
await admin.disconnect();
await client.close();
log('reconstruir', 'vistas borradas y offsets del grupo proyector en el inicio');
