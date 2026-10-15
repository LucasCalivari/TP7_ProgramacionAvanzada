// Monitor de SLA (grupo "monitor-sla"): estado en memoria con respaldo en MongoDB (monitor_estado).
// Usa el reloj SIMULADO que viaja en los eventos (ocurridoEn + factor), no el reloj de la máquina.
import { conectarMongo, correrConsumidor, T, HORA, log } from './lib.js';

const SVC = 'monitor-sla';
process.env.SERVICE = SVC;
const { db } = await conectarMongo();
const coleccion = db.collection('monitor_estado');

const REGLAS = {
  SIN_INGRESO: { prioridad: 'MEDIA', causa: 'faltan menos de 10 h y el envío no ingresó al hub' },
  SIN_SALIDA: { prioridad: 'ALTA', causa: 'faltan menos de 4 h y el envío no está en reparto' },
  FALLO_DIA_LIMITE: { prioridad: 'ALTA', causa: 'entrega fallida el día en que vence la promesa' },
  VENCIDO: { prioridad: 'CRITICA', causa: 'pasó el vencimiento sin entrega' },
};

// ---- estado (se restaura al arrancar: un reinicio no pierde ni repite alertas) ----
const envios = new Map();
for (const s of await coleccion.find({}).toArray()) envios.set(s.shipmentId, s);
log(SVC, 'estado restaurado', { envios: envios.size });

let producer;
let reloj = null; // { sim, wall, factor }
const ahoraSim = () => (reloj ? reloj.sim + (Date.now() - reloj.wall) * reloj.factor : null);

function observarReloj(ev) {
  const t = Date.parse(ev.ocurridoEn);
  const actual = ahoraSim();
  if (actual === null || t > actual) reloj = { sim: t, wall: Date.now(), factor: ev.factor || 1 };
}

const guardar = (s) => coleccion.replaceOne({ shipmentId: s.shipmentId }, s, { upsert: true });
const obtener = (id) => {
  if (!envios.has(id)) envios.set(id, { shipmentId: id, estado: null, secuencia: 0, venceEn: null, alertadas: [], cerrado: false });
  return envios.get(id);
};

async function alertar(s, regla, ocurridoEn) {
  if (s.alertadas.includes(regla)) return; // sin repetición
  const r = REGLAS[regla];
  const ev = {
    eventId: `alerta-${s.shipmentId}-${regla}`, // determinista: un reintento no genera otra alerta
    shipmentId: s.shipmentId, tipo: 'ALERTA_ABIERTA', regla, prioridad: r.prioridad, causa: r.causa,
    ocurridoEn: new Date(ocurridoEn).toISOString(), version: 1,
  };
  await producer.send({ topic: T.alertas, messages: [{ key: s.shipmentId, value: JSON.stringify(ev) }] });
  s.alertadas.push(regla);
  await guardar(s);
  log(SVC, 'alerta emitida', { shipmentId: s.shipmentId, regla, prioridad: r.prioridad });
}

async function evaluar(s, ahora) {
  if (s.cerrado || !s.venceEn) return;
  const restante = s.venceEn - ahora;
  const antesDelHub = s.estado === null || s.estado === 'CREADO';
  if (restante < 10 * HORA && antesDelHub) await alertar(s, 'SIN_INGRESO', ahora);
  if (restante < 4 * HORA && (antesDelHub || s.estado === 'INGRESADO_HUB' || s.estado === 'CLASIFICADO')) {
    await alertar(s, 'SIN_SALIDA', ahora);
  }
  if (restante < 0) await alertar(s, 'VENCIDO', ahora);
}

async function onEvento(ev, { topic }) {
  observarReloj(ev);
  const s = obtener(ev.shipmentId);
  if (topic === T.promesas) {
    s.venceEn = Date.parse(ev.venceEn);
    s.hubId = ev.hubId;
  } else {
    if (ev.secuencia <= s.secuencia) return; // duplicado o fuera de orden
    s.secuencia = ev.secuencia;
    s.estado = ev.tipo;
    if (ev.tipo === 'ENTREGADO') {
      s.cerrado = true;
      if (s.alertadas.length) {
        const cierre = {
          eventId: `cierre-${s.shipmentId}`, shipmentId: s.shipmentId, tipo: 'ALERTA_CERRADA',
          reglas: s.alertadas, ocurridoEn: ev.ocurridoEn, version: 1,
        };
        await producer.send({ topic: T.alertas, messages: [{ key: s.shipmentId, value: JSON.stringify(cierre) }] });
        log(SVC, 'alertas cerradas', { shipmentId: s.shipmentId, reglas: s.alertadas });
      }
    } else if (ev.tipo === 'ENTREGA_FALLIDA' && s.venceEn !== null) {
      const t = Date.parse(ev.ocurridoEn);
      if (new Date(t).toISOString().slice(0, 10) === new Date(s.venceEn).toISOString().slice(0, 10) && s.venceEn >= t) {
        await alertar(s, 'FALLO_DIA_LIMITE', t);
      }
    }
  }
  await guardar(s);
  if (!s.cerrado) await evaluar(s, Date.parse(ev.ocurridoEn));
}

({ producer } = await correrConsumidor({
  service: SVC,
  groupId: 'monitor-sla',
  topics: [T.estados, T.promesas],
  fromBeginning: true,
  handler: onEvento,
}));

// evaluación periódica: la ausencia de eventos también dispara alertas
let evaluando = false;
setInterval(async () => {
  const ahora = ahoraSim();
  if (ahora === null || evaluando) return;
  evaluando = true;
  try {
    for (const s of envios.values()) await evaluar(s, ahora);
  } catch (e) {
    log(SVC, 'error en evaluación periódica', { error: String(e) });
  } finally {
    evaluando = false;
  }
}, 2000);
