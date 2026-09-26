# ADR 003 — MongoDB como almacén de pedidos, inbox y outbox

Registrado retroactivamente. La decisión se tomó al inicio de la implementación y se asumió como premisa en la
propuesta y en el [ADR 002](002-consistency.md); este documento recupera las alternativas y los costos aceptados.

## Contexto

El worker persiste cuatro cosas: el resultado vigente de cada `orderId`, el snapshot confirmado por revisión, un
registro de eventos ya procesados y los mensajes pendientes de publicar. Todo eso debe escribirse en una sola
unidad atómica; el [ADR 002](002-consistency.md) explica por qué.

- El estado es un agregado autocontenido. Una orden guarda su entrada, su resultado y su procedencia; no se consulta
  cruzando pedidos, clientes o productos. El alcance no incluye reporting, conciliación contable ni analítica.
- Los importes exigen decimal exacto de punta a punta. El dominio usa `BigDecimal` con dos decimales y `HALF_UP`;
  el almacén no puede degradar eso a coma flotante binaria.
- Los invariantes de idempotencia son restricciones de unicidad: un efecto por `eventId`, una revisión confirmada
  por `orderId` + `orderVersion`. Deben imponerse en el motor, no en el código de aplicación.
- `inbox` y la DLT conservan el mensaje original recibido, de forma variable y sin esquema fijo por adelantado.
- Entorno local con Docker Compose, sin DBA ni operación dedicada.

## Alternativas

- PostgreSQL: transacción nativa sin configuración previa, `NUMERIC`, constraints declarativos, migraciones
  versionadas y `SELECT ... FOR UPDATE SKIP LOCKED` para repartir la outbox entre publicadores. Exige definir y
  migrar esquema para documentos que hoy se guardan completos, y `jsonb` para el mensaje crudo.
- MongoDB standalone: suficiente para el modelo de documentos, pero sin transacciones multi-documento. Descartada:
  no permite confirmar inbox, revisión, orden y outbox como una unidad.
- MongoDB en replica set: transacciones multi-documento con `readConcern snapshot` y `writeConcern majority`,
  `Decimal128` para importes, índices únicos y documentos completos sin mapeo relacional.
- Estado en Kafka Streams o almacén de log: evita un segundo motor, pero traslada consultas puntuales por `orderId`
  a un state store y no ofrece las restricciones de unicidad que sostienen la idempotencia.

## Decisión

- MongoDB 7.0 en replica set `rs0`, inicializado por Compose. El replica set no aporta redundancia en este entorno:
  existe para habilitar transacciones.
- Colecciones `orders` (resultado vigente por `_id = orderId`), `revisions` (snapshot por revisión), `inbox`
  (recibos por `_id = event:<eventId>` e `invalid:<receiptId>`), `outbox` y `metadata`.
- Importes como `Decimal128`. `BsonConverter` convierte `BigDecimal` explícitamente antes de escribir; ningún
  importe atraviesa `double`.
- Índice único `(orderId, orderVersion)` en `revisions` y `_id` del inbox como restricciones que deciden el ganador
  bajo concurrencia. Las lecturas previas son un atajo, no la garantía.
- Base con `readConcern majority` y `writeConcern majority`; la transacción añade `readConcern snapshot`.
- Marcador `storage-format` en `metadata` y `ensureCompatibleStorage` al arrancar: ante una base con formato o
  índices incompatibles, el worker no arranca y no modifica los datos existentes.

## Consecuencias y revisión

- La transacción depende del replica set: un despliegue standalone rompe el mecanismo de consistencia completo,
  no solo una optimización. Hereda el límite de duración por transacción y la degradación ante write conflicts;
  `withTransaction` y el reintento sobre `TRANSIENT_TRANSACTION_ERROR` absorben esa diferencia respecto a SQL.
- Sin equivalente a `SKIP LOCKED`, la outbox no tiene leasing: varias instancias leen el mismo lote y publican el
  mismo mensaje. Es compatible con la entrega al menos una vez ya asumida, pero el motor no lo evita por sí solo.
  Introducir leasing exigiría `findOneAndUpdate` con marca de propiedad y expiración, escrito a mano.
- Sin migraciones versionadas ni validación de esquema en las colecciones. `ensureCompatibleStorage` solo detecta
  incompatibilidad y exige una base vacía; no transforma datos. Un cambio de layout con datos existentes no tiene
  hoy camino de actualización.
- Sin consultas entre agregados, el modelo documental no cuesta nada en este alcance. Si más adelante se requiere
  analítica o conciliación, habrá que resolverla con aggregation pipelines o exportar a otro almacén.
- `inbox`, `revisions` y `outbox` crecen sin límite. La retención queda pendiente; los índices TTL son el camino
  previsto.
- Revisar la decisión si aparecen reporting o conciliación sobre los pedidos, si se necesita orden estricto o
  exclusividad entre publicadores, o si el esquema debe evolucionar sobre datos ya almacenados. El puerto
  `Ports.Store` aísla el almacén: sustituirlo es reimplementar un adaptador, no el dominio ni el caso de uso.

Referencia del driver: https://www.mongodb.com/docs/drivers/java/sync/current/crud/transactions/
