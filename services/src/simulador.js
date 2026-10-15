// Simulador de envíos: reemplaza a los sistemas de origen. Publica en envios.estados y envios.promesas.
// Flujo continuo + escenarios a pedido por HTTP (puerto 4000). Reloj acelerado por FACTOR.
import http from 'node:http';
import { kafka, T, ZONAS, MOTIVOS, HORA, decidirReprogramacion, franjaDe, log } from './lib.js';

const SVC = 'simulador';
process.env.SERVICE = SVC;

const CFG = {
  porMinuto: Number(process.env.ENVIOS_POR_MINUTO || 20),
  factor: Number(process.env.FACTOR || 120), // 1 h simulada = 30 s reales
  seed: Number(process.env.SEED || 42),
  pDemora: 0.1,
  pDuplicado: 0.02,
  pFueraDeOrden: 0.02,
  pMalformado: 0.01,
};

function mulberry32(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rngFlujo = mulberry32(CFG.seed); // flujo continuo: reproducible con la semilla
const rngEscenarios = mulberry32(CFG.seed + 1);
const rngRuido = mulberry32(CFG.seed + 2); // duplicados / malformados

const wallInicio = Date.now();
const simInicio = Date.now();
const simAhora = () => simInicio + (Date.now() - wallInicio) * CFG.factor;

const MOTIVO_PESOS = [['AUSENTE', 0.6], ['DIRECCION_INCORRECTA', 0.2], ['RECHAZADO', 0.15], ['ZONA_INACCESIBLE', 0.05]];
function elegirMotivo(rng) {
  let x = rng();
  for (const [m, p] of MOTIVO_PESOS) { if ((x -= p) < 0) return m; }
  return 'AUSENTE';
}

// ---- Plan de un envío: lista de eventos con su instante simulado ----
function planEnvio({ id, t0, zona, rng, forzar = null }) {
  const hubN = Number(zona.hubId.slice(1));
  const repartidorId = `R${String((hubN - 1) * 40 + 1 + Math.floor(rng() * 40)).padStart(3, '0')}`;
  const base = { shipmentId: id, hubId: zona.hubId, zonaId: zona.zonaId, repartidorId, version: 1, factor: CFG.factor };
  const promesaH = forzar === 'demorado' ? 10.5 : 24;
  const venceEn = t0 + promesaH * HORA;
  const items = [];
  let seq = 0;
  const add = (at, tipo, extra = {}) => {
    seq += 1;
    items.push({ at, topic: T.estados, ev: { ...base, eventId: `${id}-e${seq}`, tipo, ocurridoEn: new Date(at).toISOString(), secuencia: seq, ...extra } });
  };

  items.push({ at: t0, topic: T.promesas, ev: { ...base, eventId: `${id}-p`, tipo: 'PROMESA', ocurridoEn: new Date(t0).toISOString(), venceEn: new Date(venceEn).toISOString() } });
  add(t0, 'CREADO');

  let t = t0;
  if (forzar === 'demorado') t += 0.25 * HORA;
  else {
    t += (1 + rng()) * HORA;
    if (!forzar && rng() < CFG.pDemora) t += (12 + rng() * 4) * HORA; // demora en hub
  }
  add(t, 'INGRESADO_HUB');
  t += (forzar === 'demorado' ? 9 : 1) * HORA; // "demorado": queda detenido ~9 h antes de clasificarse
  add(t, 'CLASIFICADO');
  t += (2 + rng()) * HORA;

  for (let intento = 1; ; intento++) {
    add(t, 'EN_REPARTO', { intento });
    t += (2 + 2 * rng()) * HORA;
    const pFallo = Math.min(0.6, zona.tasa * (franjaDe(t) === 'MANANA' ? 1.3 : 0.8));
    const falla =
      forzar === 'feliz' || forzar === 'demorado' ? false
        : forzar === 'ausente' ? intento === 1
          : forzar === 'tercer' ? true
            : rng() < pFallo;
    if (!falla) { add(t, 'ENTREGADO', { intento, venceEn: new Date(venceEn).toISOString() }); break; }
    const motivo = forzar === 'ausente' || forzar === 'tercer' ? 'AUSENTE' : elegirMotivo(rng);
    add(t, 'ENTREGA_FALLIDA', { intento, motivo });
    const d = decidirReprogramacion(motivo, intento, new Date(t).toISOString());
    if (d.tipo !== 'REPROGRAMADO') break;
    t = Date.parse(d.fecha);
  }

  // fuera de orden: la publicación de un estado se retrasa hasta justo después del siguiente.
  // (Nunca se adelanta un evento: ocurridoEn no puede estar en el futuro del reloj simulado.)
  const estados = items.filter((i) => i.topic === T.estados);
  for (let i = 0; i < estados.length - 1; i++) {
    if (rng() < CFG.pFueraDeOrden && estados[i + 1].at - estados[i].at < 6 * HORA) {
      estados[i].at = estados[i + 1].at + 1;
      i++;
    }
  }
  return items;
}

// ---- Cola ordenada por tiempo simulado ----
let cola = [];
const schedule = (items) => { cola = cola.concat(items).sort((a, b) => a.at - b.at); };

// Verdad de terreno para validar los indicadores contra un recuento manual
const verdad = { porZona: {}, motivos: {}, entregas: 0, fallos: 0 };
function contar(ev) {
  const z = (verdad.porZona[ev.zonaId] ??= { primerIntento_total: 0, primerIntento_ok: 0 });
  if (ev.intento === 1) {
    z.primerIntento_total += 1;
    if (ev.tipo === 'ENTREGADO') z.primerIntento_ok += 1;
  }
  if (ev.tipo === 'ENTREGADO') verdad.entregas += 1;
  if (ev.tipo === 'ENTREGA_FALLIDA') {
    verdad.fallos += 1;
    verdad.motivos[ev.motivo] = (verdad.motivos[ev.motivo] ?? 0) + 1;
  }
}

const producer = kafka.producer({ allowAutoTopicCreation: false });
await producer.connect();
let malformados = 0;

async function emitir({ topic, ev }) {
  const msg = { key: ev.shipmentId, value: JSON.stringify(ev) };
  const msgs = [msg];
  if (topic === T.estados) {
    if (rngRuido() < CFG.pDuplicado) msgs.push(msg);
    if (rngRuido() < CFG.pMalformado) msgs.push(malformado(ev));
    if (ev.tipo === 'ENTREGADO' || ev.tipo === 'ENTREGA_FALLIDA') contar(ev);
  }
  await producer.send({ topic, messages: msgs });
}

function malformado(ev) {
  malformados += 1;
  if (malformados % 2) return { key: ev.shipmentId, value: '{"eventId": "roto", ' }; // JSON inválido
  const { shipmentId, tipo, ...resto } = ev; // esquema inválido: sin shipmentId ni tipo
  return { key: ev.shipmentId, value: JSON.stringify({ ...resto, eventId: `${ev.eventId}-malo` }) };
}

// ---- Generación ----
let contador = 0;
let contadorDemo = 0;
const siguienteId = (p = 'ENV') => `${p}-${String(++contador).padStart(6, '0')}`;

function crearContinuo() {
  const zona = ZONAS[Math.floor(rngFlujo() * ZONAS.length)];
  schedule(planEnvio({ id: siguienteId(), t0: simAhora(), zona, rng: rngFlujo }));
}

function escenario(nombre) {
  const mapa = { feliz: 'feliz', ausente: 'ausente', demorado: 'demorado', 'tercer-fallo': 'tercer' };
  const n = nombre === 'rafaga' ? 500 : 1;
  const ids = [];
  for (let k = 0; k < n; k++) {
    const zona = ZONAS[Math.floor(rngEscenarios() * ZONAS.length)];
    const id = `DEMO-${nombre.toUpperCase()}-${String(++contadorDemo).padStart(3, '0')}`;
    // ráfaga: 500 envíos repartidos en ~5 s reales
    const t0 = simAhora() + (k * 10) * CFG.factor;
    schedule(planEnvio({ id, t0, zona, rng: rngEscenarios, forzar: nombre === 'rafaga' ? null : mapa[nombre] }));
    ids.push(id);
  }
  return ids;
}

async function eventoInvalido() {
  await producer.send({
    topic: T.estados,
    messages: [{ key: 'INVALIDO', value: JSON.stringify({ eventId: `inv-${Date.now()}`, tipo: 'TELETRANSPORTADO', ocurridoEn: 'ayer', version: 1 }) }],
  });
}

// ---- Bucle principal ----
let ocupado = false;
setInterval(async () => {
  if (ocupado) return;
  ocupado = true;
  try {
    const ahora = simAhora();
    let n = 0;
    while (cola.length && cola[0].at <= ahora && n < 500) {
      await emitir(cola.shift());
      n += 1;
    }
  } catch (e) {
    log(SVC, 'error al emitir', { error: String(e) });
  } finally {
    ocupado = false;
  }
}, 100);
setInterval(crearContinuo, 60000 / CFG.porMinuto);

// ---- API de control (la usa el panel de demo) ----
const GRUPOS = {
  proyector: [T.estados, T.promesas, T.reprog, T.alertas, T.notifs],
  notificaciones: [T.estados, T.alertas],
  reprogramacion: [T.estados],
  'monitor-sla': [T.estados, T.promesas],
  analitica: [T.estados],
  'analitica-tiempos': [T.estados],
};
const admin = kafka.admin();
await admin.connect();

async function lag() {
  const out = {};
  for (const [groupId, topics] of Object.entries(GRUPOS)) {
    let total = 0;
    let existe = false;
    for (const topic of topics) {
      const [committed] = await admin.fetchOffsets({ groupId, topics: [topic] });
      const fin = await admin.fetchTopicOffsets(topic);
      for (const p of committed.partitions) {
        const f = fin.find((x) => x.partition === p.partition);
        if (Number(p.offset) >= 0) existe = true;
        total += Math.max(0, Number(f.high) - Math.max(0, Number(p.offset)));
      }
    }
    out[groupId] = existe ? total : null; // null = grupo sin offsets (no iniciado)
  }
  return out;
}

async function leerDlq() {
  const consumer = kafka.consumer({ groupId: `dlq-lector-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: T.dlq, fromBeginning: true });
  const msgs = [];
  await consumer.run({ eachMessage: async ({ partition, message }) => {
    let v; try { v = JSON.parse(message.value.toString()); } catch { v = { motivo: 'ilegible' }; }
    msgs.push({ partition, offset: message.offset, ...v });
  } });
  await new Promise((r) => setTimeout(r, 2500));
  await consumer.disconnect();
  return msgs.slice(-30).reverse();
}

const json = (res, code, body) => {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
};

http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  try {
    if (req.method === 'POST' && url.pathname.startsWith('/escenario/')) {
      const nombre = url.pathname.split('/')[2];
      if (nombre === 'invalido') { await eventoInvalido(); return json(res, 200, { ok: true }); }
      if (!['feliz', 'ausente', 'demorado', 'tercer-fallo', 'rafaga'].includes(nombre)) return json(res, 404, { error: 'escenario desconocido' });
      return json(res, 200, { ok: true, envios: escenario(nombre).slice(0, 5) });
    }
    if (url.pathname === '/lag') return json(res, 200, await lag());
    if (url.pathname === '/dlq') return json(res, 200, await leerDlq());
    if (url.pathname === '/verdad') return json(res, 200, verdad);
    if (url.pathname === '/config') return json(res, 200, { ...CFG, pendientes: cola.length });
    json(res, 404, { error: 'no encontrado' });
  } catch (e) {
    json(res, 500, { error: String(e) });
  }
}).listen(4000, () => log(SVC, 'simulador en marcha', { ...CFG, MOTIVOS }));
