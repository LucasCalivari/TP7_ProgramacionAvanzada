# RutaSur - seguimiento de envíos orientado a eventos (TP7)

Plataforma de seguimiento de envíos en tiempo real con Kafka (modo KRaft), Node.js, MongoDB (replica set) y Next.js. Todo corre en local con Docker Compose.

## Arranque

Con un solo comando se levanta todo el entorno:

    docker compose up --build

Una vez levantado, la interfaz está disponible en estas direcciones:

- Torre de control: http://localhost:3000
- Detalle de un envío: http://localhost:3000/envio/ID_DEL_ENVIO
- Tracking público: http://localhost:3000/tracking/ID_DEL_ENVIO
- Analítica: http://localhost:3000/analitica
- Panel de demo (escenarios, lag por grupo y DLQ): http://localhost:3000/demo

Para reiniciar desde cero, borrando los datos de Kafka y MongoDB:

    docker compose down -v

## Arquitectura

Kafka es el único punto de contacto entre los servicios: ninguno llama a otro de forma directa. MongoDB guarda vistas derivadas de los eventos y la interfaz solo lee de ahí, nunca consume Kafka.

El simulador (services/src/simulador.js) reemplaza a los sistemas de origen y publica en los topics envios.estados y envios.promesas.

El proyector (services/src/proyector.js, grupo proyector) consume los cinco topics de negocio y mantiene las colecciones envios, alertas y notificaciones en MongoDB.

El servicio de notificaciones (services/src/notificaciones.js, grupo notificaciones) consume envios.estados y alertas.sla, y publica en notificaciones.enviadas.

El servicio de reprogramación (services/src/reprogramacion.js, grupo reprogramacion) consume envios.estados y publica en envios.reprogramaciones la decisión sobre el próximo intento.

El monitor de SLA (services/src/monitor-sla.js, grupo monitor-sla) consume envios.estados y envios.promesas, evalúa las cuatro reglas con el reloj simulado que viaja en los eventos y publica en alertas.sla. Mantiene su estado en memoria con respaldo en la colección monitor_estado.

La analítica (services/src/analitica.js, grupo analitica) consume envios.estados y actualiza metricas_diarias con incrementos atómicos.

La web (carpeta web) tiene la torre de control, el detalle del envío, el tracking público, la analítica y el panel de demo. Las actualizaciones en vivo llegan por Server-Sent Events desde un route handler que escucha los change streams de MongoDB.

## Decisiones de diseño

- Garantía de entrega al menos una vez, con consumidores idempotentes: la colección eventos_procesados (con TTL de 7 días) se actualiza en la misma transacción de MongoDB que el efecto.
- Confirmación manual de offsets, después de escribir en MongoDB.
- Tres particiones en envios.estados, con shipmentId como clave de partición.
- Eventos en JSON con validación de esquema en cada consumidor; los inválidos van a envios.estados.dlq con su motivo.
- Los eventos derivados (notificaciones, reprogramaciones, alertas) tienen eventId determinista, para que un reintento no genere efectos repetidos.

## Simulador

Se configura con variables de entorno en docker-compose.yml: ENVIOS_POR_MINUTO (20 por defecto), FACTOR (120, es decir 1 hora simulada igual a 30 segundos) y SEED (42, semilla fija para repetir la corrida). Las probabilidades de demora en hub (10%), duplicado (2%), fuera de orden (2%) y malformado (1%) están fijas en el código, igual que la tasa de fallo por zona (entre 5% y 30%) y la distribución de motivos (60% ausente, 20% dirección, 15% rechazo, 5% zona).

Los escenarios a pedido (envío feliz, ausente, demorado, tercer fallo, ráfaga de 500 envíos y evento inválido) se disparan desde el panel de demo o con una llamada HTTP al simulador:

    curl -X POST http://localhost:4000/escenario/ausente

Los nombres válidos son feliz, ausente, demorado, tercer-fallo, rafaga e invalido. Con GET http://localhost:4000/verdad se obtiene el recuento real de lo emitido, para contrastarlo con los indicadores de la analítica.

## Pasos de la demo

Caída de un servicio: detener las notificaciones con el comando siguiente, observar en /demo cómo crece el lag del grupo, y volver a iniciarlas con docker compose start notificaciones para ver que procesan lo pendiente sin duplicar avisos.

    docker compose stop notificaciones

Cuarto indicador y reprocesamiento: iniciar un grupo de consumidores nuevo que lee envios.estados desde el principio, sin tocar los demás servicios.

    docker compose --profile reproceso up -d analitica-tiempos

Reconstrucción de la colección envios: con el proyector detenido, se borran las vistas, se rebobinan los offsets de su grupo y se vuelve a iniciar.

    docker compose stop proyector
    docker compose run --rm proyector node src/reconstruir.js
    docker compose start proyector

Observabilidad: cada servicio registra eventId, partición y offset (por ejemplo con docker compose logs -f proyector), y el lag de cada grupo se consulta en /demo.

## Supuestos

- La consigna no define regla para ZONA_INACCESIBLE: se reprograma el siguiente día hábil en la misma franja.
- El evento ENTREGADO lleva el campo venceEn, para que la analítica calcule el cumplimiento de SLA sin consumir envios.promesas.
- Un evento con secuencia menor o igual a la ya aplicada se descarta, también de la línea de tiempo.
- El escenario demorado usa una promesa de 10,5 horas simuladas, para que la alerta aparezca en unos 3 minutos reales con el factor por defecto.
- La reconstrucción de notificaciones solo recupera lo que retiene el topic (7 días).
