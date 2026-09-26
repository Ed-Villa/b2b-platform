# ADR 002 — inbox y outbox transaccionales

## Contexto

La clave de negocio es orderId y su revisión es orderVersion, asignada por el productor/cliente. eventVersion
describe el esquema; el sufijo .v1 identifica ese contrato en Kafka. La salida conserva la revisión de la orden
y usa eventVersion=1.

Contexto: Mongo y Kafka no comparten una transacción local. La elección de Mongo como almacén, con sus
alternativas y costos, se registra en el [ADR 003](003-persistencia.md); aquí se asume dada.
Alternativas: guardar/publicar directamente, transacciones Kafka, outbox.
Decisión: transacción Mongo replica set registra inbox, revisión, resultado y outbox. Commit de offset después. Publicar
outbox antes de marcar enviado, identificador estable.
Consecuencias: sin pérdida por la brecha entre persistir/publicar; redelivery posible, consumidores idempotentes
obligatorios. Mongo indisponible bloquea procesamiento. Retención y limpieza quedan pendientes documentados.
Revisar con mayores volúmenes, CDC o requerimientos de orden estricto de eventos de salida.

## Valores y garantías

- La transacción usa readConcern snapshot y writeConcern majority. Lecturas externas, incluida la comprobación temprana
  de inbox y la selección de outbox, usan readConcern majority; escrituras de seguimiento usan majority.
- withTransaction gestiona los reintentos de commit con resultado desconocido. Si la operación no se resuelve y llega
  a Kafka como error, no se confirma offset. Al reentregar, el mismo eventId se comprueba antes del conflicto de
  revisión.
- revisions conserva el snapshot confirmado por revisión, no solamente su clave. Se añade calculationPolicyVersion=v1;
  al cambiar reglas de calculos se debe cambiar esta identificación. No se reconstruye retroactivamente el historial
  anterior.
- Eventos completos, no deltas: se permiten saltos y consumidores aplican solo revisiones superiores. createdAt es un
  criterio de planificación de la outbox, no una garantía de orden de negocio.
- Los fallos de envío se aíslan por registro, con contador, espera y aparcamiento para errores deterministas. No hay
  orden estricto ni leasing entre publicadores: siguen siendo posibles duplicados y salidas fuera de orden.
- TECHNICAL_FAILURE requiere recuperación mediante nuevo evento y revisión superior emitidos por el productor.

Referencia del driver: https://www.mongodb.com/docs/drivers/java/sync/current/crud/transactions/
