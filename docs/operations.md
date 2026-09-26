# Operación y diagnóstico

Los logs del worker se pueden filtrar por `transition`, `orderId` y `eventId`. Ten en cuenta:

- `transactionDecision` se registra dentro de la transacción, que puede repetirse o abortarse. No significa que haya habido commit.
- `durably_handled` solo indica que el puerto de persistencia respondió. La prueba real de que algo quedó guardado es el inbox.
- Las APIs exponen `correlationId` y latencia.
- Nunca registres cuerpos HTTP, nombres de clientes ni secretos.

## Métricas propuestas (todavía no se exportan)

| Métrica | Labels de baja cardinalidad | Para qué sirve |
|---|---|---|
| orders_processed_total | market, status | Ver si caen las aprobaciones o suben los rechazos |
| order_processing_seconds | market | Histograma p50/p95/p99 |
| provider_requests_total / latency | provider, statusClass | Detectar un proveedor lento |
| provider_retries_total | provider, category | Alertar si crecen de forma sostenida |
| orders_disposition_total | duplicate, obsolete, conflict, invalid | Detectar replays o un productor defectuoso |
| dlt_total | category | Revisar la causa y quién es el responsable |
| outbox_pending / oldest_age_seconds | topic | Alertar si la antigüedad supera 60 s durante 5 min |
| consumer_lag | topic, partition | Alertar si crece sin parar durante 10 min |

No uses `eventId` ni `orderId` como labels. Los umbrales son un punto de partida y hay que ajustarlos con tráfico real; en esta prueba no se midió ningún SLO de rendimiento.

## Se envió un pedido pero no aparece su estado

1. Consigue el `orderId`/`eventId` y la ventana de tiempo, sin pedir datos personales que no hagan falta. Confirma que el productor recibió el ack de Kafka y usó el tópico, la key y la revisión correctos.
2. Busca el log `received`, el tópico/partición/offset y el lag del grupo. Si el mensaje nunca llegó, revisa productor, broker, asignaciones y offsets. No empieces reseteando offsets.
3. Busca en el inbox: PROCESSED, INVALID, CONFLICT u OBSOLETE. Un duplicado apunta al evento original. Los errores de validación y los conflictos van a la DLT; los obsoletos no pisan la revisión actual.
4. Consulta `orders` por `_id=orderId`. TECHNICAL_FAILURE también es un resultado guardado, aunque no sea aprobado ni rechazado. Revisa la razón y la DLT. Reenviar el mismo `eventId` no va a recalcular nada.
5. Si hay resultado pero nadie vio el evento de salida, revisa el outbox (pendiente o `sent`) y el `eventId` de salida. Si está pendiente: broker, permisos o red. Si está `sent`: consumidor downstream, deduplicación y offsets. Ojo: puede haber una reentrega entre el envío y el marcado como `sent`.
6. Si el mensaje llegó pero no hay inbox, revisa Mongo, el índice único, las transacciones y los reintentos del consumidor. Mientras no haya commit del offset, el mensaje se volverá a entregar.
7. Documenta línea de tiempo, alcance y causa. Arregla la dependencia y deja que el sistema se recupere solo. Si hay que recalcular una orden ya terminada, publica una revisión más alta mediante un procedimiento autorizado y auditable.

## Recuperación y límites

**Mongo caído.** Si el worker está arrancando, puede fallar al crear índices; si ya estaba corriendo, los mensajes se siguen reentregando. Reinícialo cuando Mongo vuelva.

**Kafka caído.** El outbox se va acumulando. Recupera el broker y verifica que se vacíe.

- Cada error de envío se registra por salida y el resto del lote sigue.
- `nextAttemptAt` aplaza el siguiente intento (`OUTBOX_RETRY_DELAY_MS`, 5000 ms por defecto).
- Solo se aparcan los errores deterministas (`RecordTooLargeException`, `SerializationException`), tras `OUTBOX_MAX_PERMANENT_FAILURES` intentos (3 por defecto). Los timeouts y fallos de infraestructura nunca se aparcan por ese contador.
- Si Mongo falla al registrar el intento o al marcar `sent`, el ciclo se detiene, pero no se pierde ninguna salida.

**Retención.** Inbox, revisiones y outbox no tienen TTL. Si borras datos de deduplicación antes del horizonte máximo de replay, los efectos pueden repetirse. En producción habrá que archivar las salidas enviadas y conservar las claves de idempotencia durante el horizonte acordado. La DLT guarda los mensajes originales: limita los permisos y acuerda la retención antes de trabajar con datos reales.

**Consumidores.** Con entrega al menos una vez, cada consumidor debe guardar la deduplicación y su efecto local en una misma operación atómica. El productor idempotente de Kafka, por sí solo, no cubre los reinicios del publicador. Las revisiones pueden llegar desordenadas: compara `orderVersion` para no retroceder el estado. `eventVersion` y `sourceEventVersion` son versiones de esquema y no sirven para ordenar.

## Espera por productos

Primero se consulta el cliente; después, los productos en virtual threads con un límite compartido `PRODUCTS_CONCURRENCY` (20).

Si la latencia sube, antes de aumentar el límite revisa errores y reintentos de Products API, número de líneas, concurrencia de Kafka y réplicas. Durante fallos hay backoff, así que no esperes un throughput constante. Cambiar el valor requiere reiniciar o recrear el worker, y el límite total se multiplica por el número de réplicas.

Métricas que convendría añadir (aún no existen): consultas en curso, tiempo esperando permiso y latencia de enriquecimiento.

## Recuperación de TECHNICAL_FAILURE

La DLT sirve para diagnosticar e intervenir; no hay un consumidor que haga replay automático. Lo que queda cerrado es la revisión fallida, no la orden completa.

- Reenviar el mismo `eventId` → se trata como duplicado.
- Otro `eventId` con la misma revisión → conflicto.

Una vez arreglado el proveedor, el productor debe autorizar y publicar el snapshot completo con un `eventId` nuevo y un `orderVersion` mayor que el vigente. No edites a mano el inbox ni las revisiones, ni agregues un contador en el worker.

Si negocio necesita mantener la misma revisión, habría que diseñar aparte un contrato administrativo de replay (identidad de intento, autorización, payload inmutable y exclusión frente a revisiones nuevas). Eso no está implementado.

## Publicaciones aparcadas

Busca en el outbox `{sent:false, parked:true}` y alerta con `transition=outbox_parked`. No te fíes solo de `pending()`: no incluye las salidas en espera ni las aparcadas. El registro conserva payload, `eventId` y contadores.

Para reactivar una salida:

1. Corrige el tamaño, la configuración o el serializador, y deja constancia de la intervención.
2. Actualiza solo el `_id` afectado, con la condición `sent:false, parked:true`: pon `parked:false`, `permanentFailures:0` y elimina `nextAttemptAt`.
3. Mantén `publicationAttempts` y el `eventId`. El reenvío puede llegar como duplicado.

No cambies el contenido de un evento que ya tiene `eventId`, y no uses la DLT de Kafka para recuperarlo: si el payload es demasiado grande, tampoco cabrá allí.

No se garantiza el orden de todas las revisiones: `createdAt` solo sirve para priorizar, y otras órdenes y revisiones pueden seguir avanzando. Si un consumidor necesita cada transición en orden, hace falta otro contrato; el que solo quiere el estado actual debe deduplicar por `eventId` y aplicar únicamente `orderVersion` mayores.

## Auditoría de cálculos

`orders` tiene el estado vigente. Las revisiones nuevas en `revisions` guardan un snapshot con la entrada completa, el resultado, el cliente, las líneas enriquecidas, los importes y `calculationPolicyVersion`. Usa ese snapshot; no vuelvas a consultar los catálogos actuales.

- Los registros anteriores a este cambio no se rellenan con datos actuales ni tienen historial.
- Si una ejecución se aborta y se reentrega, puede recalcular con catálogos distintos. El snapshot refleja lo que se confirmó, no una foto de todos los proveedores en el mismo instante.
- Los contratos de catálogo no tienen ETags ni consultas temporales.
- En fallos o rechazos puede guardarse solo el enriquecimiento disponible, sin totales.

## Simular proveedores a mano

Desde la raíz del repositorio:

```sh
python scripts/fake-provider.py --port 8082 --fail-first 2 --status 503
```

- Worker fuera de Docker: `PRODUCTS_URL=http://localhost:8082`.
- Con Docker Desktop: define `PRODUCTS_URL=http://host.docker.internal:8082` en un override de Compose y recrea el worker.

Opciones útiles:

- `--fail-first 100 --status 503`: agota los reintentos.
- `--status 404`: provoca un rechazo.
- `--delay-ms 2500`: supera el timeout por defecto.

Los contadores van por evento y ruta. El simulador no se incluye en las imágenes de los servicios.

## Worker local con el MongoDB de Compose

Para correr el worker fuera de los contenedores contra el MongoDB de Compose:

```
MONGODB_URI=mongodb://localhost:27017/b2b?replicaSet=rs0&directConnection=true
```

`directConnection=true` evita que el host tenga que resolver `mongo`, el nombre interno que anuncia el replica set. Si usas otra base, configura `MONGODB_DATABASE` y usa ese mismo nombre en mongosh.
