import { getDb } from '../../../lib/db';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

// Server-Sent Events alimentados por los change streams de envios y alertas.
export async function GET(req) {
  const db = await getDb();
  const enc = new TextEncoder();
  const streams = [];
  let ping;
  const cerrar = () => {
    clearInterval(ping);
    streams.forEach((s) => s.close().catch(() => {}));
  };

  const stream = new ReadableStream({
    start(controller) {
      const send = (o) => {
        try { controller.enqueue(enc.encode(`data: ${JSON.stringify(o)}\n\n`)); } catch {}
      };
      for (const c of ['envios', 'alertas']) {
        const cs = db.collection(c).watch([], { fullDocument: 'updateLookup' });
        cs.on('change', (ch) => send({ c, shipmentId: ch.fullDocument?.shipmentId }));
        cs.on('error', () => {});
        streams.push(cs);
      }
      ping = setInterval(() => send({ ping: 1 }), 15000);
      req.signal.addEventListener('abort', () => {
        cerrar();
        try { controller.close(); } catch {}
      });
    },
    cancel: cerrar,
  });

  return new Response(stream, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive' },
  });
}
