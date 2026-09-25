# Propuesta de arquitectura

Esta es la revisión vigente de la propuesta inicial. Incorpora la aclaración posterior del evaluador sobre la versión
del esquema y la decisión de añadir `orderVersion`; no representa una copia intacta del documento previo a la
implementación. Los cambios y la evidencia se registran en [implementation-notes.md](implementation-notes.md) y
[verification.md](verification.md).

## Problema y alcance

- Procesar pedidos B2B de MX, CO y PE con reglas monetarias deterministas, tres servicios y recuperación ante fallos.
- Concentrar el dominio y la consistencia en `order-processor`, con Java 21 y Spring Boot 3.x. Mantener Products API
  en Go y Clients API en NestJS pequeñas, funcionales y testeables.
- Usar Kafka para transportar eventos y MongoDB como replica set para mantener resultados, inbox, revisiones y outbox.
- Mantener semillas en memoria en las APIs, detrás de repositorios sustituibles, y ejecutar el conjunto con Docker
  Compose.

## Supuestos y contratos

- Cada evento contiene el estado completo del pedido, no un delta. Se permiten saltos de orderVersion; la revisión
  nueva sustituye el estado anterior. TECHNICAL_FAILURE cierra esa revisión; su recuperación necesita un nuevo evento
  y revisión superior del productor. No se implementa replay automático desde DLT.

- `eventVersion` representa la versión del esquema, según aclaración del evaluador; `.v1` identifica esa versión en el
  tópico. En esta entrega se admite `eventVersion=1`.
- Se introduce `orderVersion`, revisión positiva y obligatoria asignada e incrementada por el productor, para detectar
  actualizaciones obsoletas del mismo `orderId`. El worker no deduce esa revisión del orden de llegada ni de
  `occurredAt`.
- La entrada usa `orders.created.v1`, con clave `orderId`. La salida de aprobados y rechazados usa
  `orders.processed.v1`;
  conserva `orderVersion`, emite `eventVersion=1` y registra el esquema de entrada en `sourceEventVersion`.
- Repetir el mismo `eventId` conserva el primer efecto confirmado. Para la misma orden y revisión con distintos
  `eventId`, gana la primera transacción confirmada y el otro evento se registra como conflicto y se envía a DLT.
- Una revisión inferior a la vigente se registra como obsoleta, salvo que ya corresponda a un duplicado o conflicto.
  No reemplaza el resultado ni genera otro evento de negocio. Una revisión superior puede actualizar la orden.
- Rechazos y fallos tienen totales nulos y una razón explícita; no se publican cálculos parciales.

## Responsabilidades, capas y dependencias

- El dominio Java contiene validación, elegibilidad, impuestos, descuentos y redondeo. No depende de Spring, Kafka,
  HTTP, MongoDB ni DTOs externos. Usa `BigDecimal`, dos decimales y `HALF_UP`; suma importes de líneas ya redondeados.
- La aplicación orquesta el caso de uso mediante puertos de catálogo y almacenamiento. Los adaptadores implementan
  esos puertos; la configuración técnica construye y conecta las dependencias.
- El adaptador Kafka recibe eventos; los adaptadores HTTP consultan proveedores; MongoDB persiste resultados y salidas
  pendientes. Los modelos externos, de dominio y de persistencia se conectan mediante mapeos explícitos.
- Products posee el estado y la categoría fiscal del producto por mercado; Clients posee estado, segmento, régimen
  fiscal y mercado del cliente. Sus detalles de almacenamiento no se exponen a los consumidores.
- El worker posee los cálculos, la decisión y el contrato de salida. El contrato de entrada se acuerda con el productor
  externo y se mantiene como JSON Schema en este repositorio. No se comparten librerías de dominio entre stacks.
- La arquitectura completa y los flujos se muestran en [diagrams.md](diagrams.md).

## Modelo de procesamiento y concurrencia

- Procesar cada partición de forma síncrona, con concurrencia configurable del consumidor y clave `orderId`.
  Kafka distribuye el trabajo entre particiones; MongoDB protege los invariantes incluso ante ejecuciones concurrentes.
- Consultar el cliente una vez por pedido (con los reintentos HTTP definidos). Solo si está activo y pertenece al
  mercado del pedido, consultar sus productos concurrentemente mediante virtual threads de Java 21.
- Un semáforo compartido por instancia limita a `providers.products-concurrency` las consultas de producto en curso
  entre todos los pedidos; valor predeterminado 20, configurable con `PRODUCTS_CONCURRENCY`. El permiso cubre
  la llamada completa, incluidos retries y backoff, y se libera en `finally`.
- Mantener HTTP blocking y la política de retries existente. Recoger resultados en el orden original del pedido:
  si varias consultas fallan, se selecciona el primer fallo en ese orden, no el que termina antes.
- Ante fallo se cancelan las tareas restantes y se espera su terminación antes de persistir. Una interrupción
  cancela el trabajo y se propaga sin guardar resultado ni confirmar offset. No se usan resultados parciales.
- Acotar el tamaño del pedido y mantener su presupuesto de procesamiento, incluida la espera por permisos,
  por debajo de `max.poll.interval.ms`. El límite es por instancia; varias réplicas multiplican la presión agregada.
- Justificación y condiciones de revisión: [ADR de concurrencia](adr/001-concurrency.md).

## Flujo principal y errores

- Flujo principal: Kafka → validación → detección de duplicado conocido → cliente válido → productos concurrentes → cálculo → transacción
  inbox/revisión/resultado/outbox → confirmación del offset. La transacción vuelve a comprobar identidad y revisión.
- Un cliente no elegible permite omitir las consultas de productos. Un recurso inexistente (`404`), cliente bloqueado,
  mercado incorrecto o producto descontinuado produce `REJECTED`, con razón explícita y salida en `orders.processed.v1`.
- Timeout y respuestas `429`, `500`, `502` o `503`: hasta tres intentos totales por consulta, uno inicial y dos
  reintentos.
  Conexión de 1 s, respuesta de 2 s y esperas de 200/400 ms más jitter; `Retry-After` se respeta hasta un máximo de 2 s.
- Otros errores HTTP definitivos o respuestas malformadas no se reintentan. Un fallo técnico definitivo o el agotamiento
  de intentos persiste `TECHNICAL_FAILURE`, con totales nulos, y programa una salida a `orders.processing.dlt`.
- Una entrada inválida, incluido un esquema no soportado, se registra en inbox y DLT sin crear un pedido procesado.
  La DLT conserva el original, IDs recuperables, versiones recuperables, categoría, causa, intentos, timestamp,
  componente y posición de origen en Kafka.
- Si MongoDB falla, no se confirma el offset: se reintenta o se recibe nuevamente el mensaje. Si Kafka no permite
  publicar la salida, esta permanece pendiente en outbox y el publicador reintenta con frecuencia limitada.

## Idempotencia y consistencia MongoDB–Kafka

- Transacciones con readConcern snapshot y writeConcern majority; consultas de inbox/outbox fuera de la transacción
  con readConcern majority. Se conserva un snapshot auditable del resultado y entrada por revisión confirmada.
- Un error de envío aplaza únicamente esa salida; errores permanentes de tamaño/serialización se aparcan con contadores
  y payload intacto. createdAt organiza intentos y no define orden de negocio; no se promete publicación secuencial.

- Una transacción MongoDB registra inbox único por evento, revisión única por `orderId` + `orderVersion`, resultado
  actualizado con comparación atómica de versión y salida pendiente. Evita resultados confirmados sin outbox.
- Los conflictos transaccionales se reevalúan contra el estado confirmado. Los mensajes inválidos se identifican por
  tópico/partición/offset. El offset se confirma después de guardar durablemente el resultado o la disposición del
  mensaje.
- El publicador lee únicamente salidas confirmadas, publica en Kafka y después marca `sent`. Así no publica un resultado
  cuya transacción MongoDB todavía no se haya confirmado.
- Una caída después de publicar y antes de marcar puede repetir la entrega con el mismo `eventId`. La garantía es
  al menos una vez; los consumidores deben deduplicar y comparar `orderVersion` para evitar retroceder su estado.
- Varios publicadores pueden duplicar entregas; las restricciones y la transacción protegen los efectos persistidos
  del worker. No se afirma entrega exactamente una vez ni orden global de las salidas.
- Alternativas y consecuencias: [ADR de consistencia](adr/002-consistency.md).

## Compatibilidad y acuerdos entre equipos

- Cada proveedor posee su OpenAPI; el worker mantiene los JSON Schema de eventos, con revisión del productor y los
  consumidores afectados. CI valida contratos y ejemplos antes de integrar cambios.
- Se toleran campos adicionales compatibles. Los cambios incompatibles requieren nueva versión y convivencia temporal:
  acordar contratos, desplegar consumidores compatibles, migrar productores y retirar la versión anterior cuando ya no
  se use.
- Son acuerdos compartidos el error HTTP con `code`, `message` y `correlationId`, la correlación por `orderId`/
  `eventId`,
  las reglas de versionado y las verificaciones de CI. No se registran secretos ni datos sensibles innecesarios.
- Cada equipo decide su organización interna y herramientas idiomáticas, respetando contratos y límites. No se exige
  replicar las mismas abstracciones en Java, Go y NestJS. La coordinación se detalla en
  [technical-leadership.md](technical-leadership.md).

## Riesgos y posibles regresiones

- MongoDB indisponible bloquea el avance de las particiones; Kafka indisponible acumula outbox. Observar lag, antigüedad
  de pendientes y capacidad; definir una política de retención antes de producción.
- Saturar el semáforo, pedidos grandes y reintentos pueden aumentar la espera y provocar rebalanceos si exceden
  el intervalo de procesamiento. Medir latencia, espera por permisos y carga del proveedor antes de aumentar el límite.
- Cambiar redondeo, descuentos o tasas puede alterar totales: proteger las reglas con casos monetarios esperados.
- Cambios de contrato o confundir `eventVersion` con `orderVersion` pueden romper consumidores o descartar
  actualizaciones
  válidas: probar compatibilidad y revisiones de negocio diferentes sobre el mismo esquema.
- El almacenamiento anterior no debe reinterpretarse automáticamente. La implementación rechaza el formato incompatible;
  una base nueva sirve para validación local, mientras una migración productiva necesita un procedimiento específico.
- La entrega duplicada y fuera de orden de la outbox exige consumidores idempotentes. Diagnóstico y métricas propuestas
  de éxito, rechazo, retries, duplicados, DLT y latencia: [operations.md](operations.md).

## Estrategia de pruebas

- Concurrencia: verificar virtual threads, solapamiento real con barreras, límite compartido entre pedidos, ausencia
  de consultas para clientes no elegibles, orden estable, cancelación y liberación de permisos tras fallo/interrupción.

- Unitarias de dominio: impuestos por mercado/categoría, exención, descuento en cantidades 19/20, redondeo, agregación,
  elegibilidad, productos repetidos y campos fuera de rango.
- Pruebas idiomáticas de Go y NestJS: repositorios, validación, errores y al menos un handler/endpoint por API.
- Dobles HTTP controlables: recuperación de timeout/503, agotamiento de intentos, `404`, errores definitivos y
  respuestas
  malformadas; comprobar que no se publiquen totales parciales.
- Integración con Testcontainers: duplicados concurrentes, igual revisión con distintos eventos, revisiones fuera de
  orden,
  rollback antes del commit y caída después de publicar pero antes de marcar la outbox.
- Contratos y recorrido completo: validar ejemplos y respuestas; comprobar evento de entrada → resultado MongoDB →
  salida
  o DLT. Distinguir esquema y revisión en las aserciones. Registrar resultados reales
  en [verification.md](verification.md).

## Alternativas descartadas y motivos

- Transacciones Kafka como solución a la atomicidad MongoDB–Kafka: no se incluye la escritura MongoDB en su
  transacción. Se elige transacción local más outbox para recuperar publicaciones pendientes.
- Verificación `exists` seguida de `save` como única protección: deja una carrera entre ambas operaciones.
  Se requieren restricciones únicas y operaciones transaccionales.
- HTTP completamente secuencial: acumula latencias de productos independientes. Se reemplaza por virtual threads
  con semáforo, conservando primero la validación del cliente para evitar consultas innecesarias.
- Programación reactiva: no se requiere para solapar I/O; virtual threads permiten conservar HTTP blocking y retries.
- Concurrencia de productos sin límite: puede saturar Products API. Un semáforo por pedido tampoco protege la carga
  agregada del worker, por lo que el límite se comparte entre pedidos.
- Caché: introduce invalidación y riesgo de datos de elegibilidad desactualizados sin necesidad demostrada.
- Frontend: es opcional y desplaza tiempo del dominio, la consistencia y las pruebas obligatorias.

## Plan de implementación incremental

- 0–1 h: propuesta inicial, contratos, supuestos y ADRs antes del código.
- 1–3 h: dominio y pruebas monetarias y de elegibilidad.
- 3–4.5 h: APIs, semillas, errores, healthchecks y pruebas.
- 4.5–7.5 h: enriquecimiento HTTP, Kafka, transacciones MongoDB, deduplicación y outbox.
- 7.5–10 h: integración y simulación de fallos, concurrencia y recuperación.
- 10–12 h: Compose, CI, operación, README y preparación de la defensa.
