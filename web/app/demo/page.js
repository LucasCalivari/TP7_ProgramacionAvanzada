'use client';
import { useEffect, useState } from 'react';

const ESCENARIOS = [
  ['feliz', 'Envío feliz'],
  ['ausente', 'Ausente'],
  ['demorado', 'Demorado en hub'],
  ['tercer-fallo', 'Tercer fallo'],
  ['rafaga', 'Ráfaga (500 envíos)'],
  ['invalido', 'Evento inválido'],
];

export default function Demo() {
  const [msg, setMsg] = useState('');
  const [lag, setLag] = useState({});
  const [dlq, setDlq] = useState([]);
  const [lanzados, setLanzados] = useState([]);

  const consultar = async (q) => (await fetch(`/api/demo?q=${q}`)).json();
  useEffect(() => {
    const t = setInterval(() => consultar('lag').then(setLag).catch(() => {}), 2000);
    return () => clearInterval(t);
  }, []);

  async function disparar(escenario) {
    const r = await fetch('/api/demo', { method: 'POST', body: JSON.stringify({ escenario }) });
    const j = await r.json();
    if (j.envios?.length) {
      setMsg('');
      setLanzados((l) => [{ escenario, id: j.envios[0], n: j.envios.length }, ...l].slice(0, 10));
    } else {
      setMsg(`Escenario ${escenario}: ${j.ok ? 'enviado' : JSON.stringify(j)}`);
    }
  }

  return (
    <>
      <h1>Panel de demo</h1>
      <div>
        {ESCENARIOS.map(([k, label]) => <button key={k} onClick={() => disparar(k)}>{label}</button>)}
      </div>
      <p>{msg}</p>

      {lanzados.length > 0 && (
        <>
          <h2>Envíos lanzados</h2>
          <table>
            <thead><tr><th>Escenario</th><th>Envío</th><th>Detalle (interno)</th><th>Tracking público</th></tr></thead>
            <tbody>
              {lanzados.map((l) => (
                <tr key={l.id}>
                  <td>{l.escenario}{l.n > 1 ? ` (${l.n} envíos, primer id)` : ''}</td>
                  <td>{l.id}</td>
                  <td><a href={`/envio/${l.id}`} target="_blank">Ver detalle</a></td>
                  <td><a href={`/tracking/${l.id}`} target="_blank">Ver tracking</a></td>
                </tr>
              ))}
            </tbody>
          </table>
          <p>El envío aparece en el detalle unos segundos después de lanzarlo.</p>
        </>
      )}

      <h2>Lag por grupo de consumidores</h2>
      <table>
        <thead><tr><th>Grupo</th><th>Mensajes pendientes</th></tr></thead>
        <tbody>
          {Object.entries(lag).map(([g, v]) => <tr key={g}><td>{g}</td><td>{v === null ? 'sin iniciar' : v}</td></tr>)}
        </tbody>
      </table>

      <h2>Dead-letter queue <button onClick={() => consultar('dlq').then(setDlq)}>Actualizar</button></h2>
      <pre>{dlq.length ? JSON.stringify(dlq, null, 2) : 'Sin mensajes cargados'}</pre>
    </>
  );
}
