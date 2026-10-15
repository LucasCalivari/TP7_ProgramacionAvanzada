import { getDb } from '../../../lib/db';
import Live from '../../../components/Live';

export const dynamic = 'force-dynamic';

const TEXTO = {
  CREADO: 'Tu envío fue registrado por el comercio',
  INGRESADO_HUB: 'Tu paquete llegó a nuestro centro de distribución',
  CLASIFICADO: 'Tu paquete está listo para salir a reparto',
  EN_REPARTO: 'Tu paquete está en camino',
  ENTREGADO: 'Tu paquete fue entregado',
  ENTREGA_FALLIDA: 'No pudimos entregar tu paquete',
};

// Vista pública: sin datos internos (zona, hub, repartidor, alertas ni historial detallado).
export default async function Tracking({ params }) {
  const db = await getDb();
  const envio = await db.collection('envios').findOne({ shipmentId: params.id });
  if (!envio) return <><h1>Seguimiento</h1><p>No encontramos el envío {params.id}.</p></>;

  let texto = TEXTO[envio.estado] ?? 'Procesando tu envío';
  if (envio.devolucion) texto = 'Tu paquete será devuelto al comercio';
  else if (envio.retenido) texto = 'Necesitamos que corrijas la dirección de entrega';
  const estimada = envio.devolucion || envio.estado === 'ENTREGADO'
    ? null
    : envio.reprogramacion?.proximoIntento?.fecha ?? envio.promesa?.venceEn ?? null;

  return (
    <>
      <h1>Seguimiento de envío <Live shipmentId={params.id} /></h1>
      <div className="cards">
        <div className="card"><b style={{ fontSize: 16 }}>{params.id}</b>Número de envío</div>
        <div className="card"><b style={{ fontSize: 16 }}>{texto}</b>Estado actual</div>
        {estimada && <div className="card"><b style={{ fontSize: 16 }}>{estimada.slice(0, 10)}</b>Fecha estimada</div>}
      </div>
    </>
  );
}
