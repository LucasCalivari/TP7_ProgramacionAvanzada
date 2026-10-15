'use client';
import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

// Escucha el SSE (change streams de MongoDB) y pide al servidor que vuelva a renderizar.
export default function Live({ shipmentId }) {
  const router = useRouter();
  useEffect(() => {
    const es = new EventSource('/api/stream');
    let t;
    es.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m.ping) return;
      if (shipmentId && m.shipmentId !== shipmentId) return;
      clearTimeout(t);
      t = setTimeout(() => router.refresh(), 150);
    };
    return () => { clearTimeout(t); es.close(); };
  }, [shipmentId, router]);
  return <span className="live">● en vivo</span>;
}
