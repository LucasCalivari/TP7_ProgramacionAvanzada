export const dynamic = 'force-dynamic';

const SIM = process.env.SIMULADOR_URL || 'http://localhost:4000';

// Proxy hacia la API de control del simulador (escenarios, lag de grupos, DLQ).
export async function POST(req) {
  const { escenario } = await req.json();
  const r = await fetch(`${SIM}/escenario/${encodeURIComponent(escenario)}`, { method: 'POST' });
  return Response.json(await r.json(), { status: r.status });
}

export async function GET(req) {
  const q = new URL(req.url).searchParams.get('q');
  if (!['lag', 'dlq', 'verdad'].includes(q)) return Response.json({ error: 'consulta inválida' }, { status: 400 });
  try {
    const r = await fetch(`${SIM}/${q}`, { cache: 'no-store' });
    return Response.json(await r.json());
  } catch (e) {
    return Response.json({ error: String(e) }, { status: 502 });
  }
}
