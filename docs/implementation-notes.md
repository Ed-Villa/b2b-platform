# Notas de implementación

## Versiones

Usamos dos campos distintos:

- `eventVersion` indica la versión del esquema del mensaje.
- `orderVersion` es un campo nuevo que indica la revisión de negocio del pedido. Sustituye la interpretación anterior.

El dominio no guarda la versión del esquema. El DTO comprueba que `eventVersion=1` antes de mapear.

En MongoDB, la clave única (`orderId` + `orderVersion`) y el control de concurrencia usan `orderVersion`. La salida
mantiene `orderVersion`, emite `eventVersion=1` y usa `sourceEventVersion` para indicar qué esquema llegó.

No se modifica la base antigua. Si el worker detecta el formato anterior, pide una base nueva mediante
`MONGODB_DATABASE`.

## Qué cambió respecto a la propuesta

- **Outbox transaccional:** evita perder eventos entre guardar y publicar, y permite demostrar la recuperación.
- **Ventana de poll:** subió de 15 a 30 minutos por las 101 consultas secuenciales y sus reintentos. Con productos
  concurrentes mantenemos ese margen, pero ahora también cubre la espera por permisos. Si se cambia un límite, hay que
  revisar los demás.
- **Mapeo explícito:** los DTOs de entrada y de proveedores se convierten a records del dominio. El resultado se
  guarda como documentos BSON, con importes en `Decimal128`.

## Cómo lo verificamos

| Qué se probó                             | Cómo                                                                                                                         |
|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| Reglas Java, HTTP, listener y aplicación | Reportes Surefire de Maven                                                                                                   |
| Mongo y Kafka reales                     | Failsafe/Testcontainers: duplicados concurrentes, conflictos de revisión, orden de versiones, rollback y caída tras publicar |
| Go                                       | `go test ./...`, además de compilación y pruebas en el Dockerfile                                                            |
| NestJS                                   | Compila TypeScript y pasan cinco pruebas Jest; el Dockerfile ejecuta build y test                                            |
| Contratos                                | JSON Schemas, OpenAPI y ejemplos validados                                                                                   |
| Compose                                  | Arrancan cinco servicios; APIs, Kafka y Mongo quedan en healthy                                                              |

## Pendientes y límites conocidos

- El backend ya se entrega en contenedores. Faltan Flutter, caché, métricas exportadas, schema registry, pruebas de
  carga y autenticación/TLS.
- El outbox procesa por lotes sin leasing entre instancias. Puede haber duplicados o revisiones desordenadas. Los
  consumidores lo mitigan con `eventId` y `orderVersion`.
- `orders` guarda solo el último snapshot. Las revisiones nuevas incluyen identidad y snapshot completo. Las antiguas
  pueden tener solo la identidad. No hay endpoint de consulta ni replay administrativo.
- No hay retención ni limpieza automática. Antes de borrar datos de deduplicación hay que definir cuánto tiempo se
  necesita para replay. La DLT guarda el mensaje original, así que en producción necesita control de acceso.
- Los enums de proveedor son estrictos. Un valor desconocido falla y no se adivina la tasa fiscal. Suponemos que el
  proveedor ya validó sus datos.
- `attempts` cuenta los intentos de la consulta que falla, no los del pedido completo. Un error de validación
  semántica también cuenta como intento.
- Si Mongo cae antes de crear los índices, el worker no arranca. Puede hacer falta reiniciarlo a mano cuando vuelva la
  infraestructura.
- Para cada `eventId` se queda el primer resultado, aunque llegue otro mensaje con el mismo id y distinto contenido. El
  productor debe garantizar que el id no cambie. Detectar colisiones por hash queda pendiente.
- La observabilidad es básica: no hay dashboards ni alarmas. Un log anterior al commit no garantiza por sí solo que la
  operación haya terminado bien.

## Siguientes pasos

1. Añadir métricas y pruebas de carga para definir SLO y retención.
2. Después, evaluar replay y leasing/backoff si el volumen lo requiere.
3. Antes de producción, revisar dependencias, seguridad, redundancia y backups.

## Consultas de productos en concurrencia (2026-09-24)

Antes se consultaba todo en secuencia. Ahora se consulta primero el cliente y después los productos en paralelo, con
virtual threads de Java 21.

- `ConcurrentProducts` usa un semáforo por instancia del caso de uso, que es singleton.
- El límite se configura con `providers.products-concurrency` / `PRODUCTS_CONCURRENCY` (por defecto 20, debe ser
  positivo). Para cambiarlo hay que reiniciar.
- HTTP y los reintentos siguen siendo bloqueantes.
- Se mantiene el orden de entrada. Si una consulta falla, se cancelan las demás y se espera a que terminen antes de
  guardar.
- Si hay una interrupción, se propaga sin guardar nada ni confirmar offsets.
- Cada permiso cubre también el backoff y los reintentos, y se libera aunque haya una excepción.
- El límite no es distribuido ni funciona como rate limiter.

Seguimos haciendo una consulta de cliente más N de producto. La diferencia es que las N consultas ahora corren al mismo
tiempo.

## Reorganización de archivos (2026-09-25)

- `Model.java` se dividió en siete records de dominio, cada uno en su propio archivo.
- En Clients API, controller, servicio, repositorio, DTO, filtro, healthcheck y módulo tienen ahora su propio archivo.
- Se actualizaron los imports en el código de producción y en las pruebas.
- No cambian los DTOs privados del adaptador HTTP de Java ni la estructura de Go.

## Correcciones de la revisión final (2026-09-25)

- **Cancelación de productos:** `Future.cancel(true)` podía marcar el futuro como terminado antes de que su tarea
  acabara de limpiar. Ahora usamos `shutdownNow()` y mantenemos `close()`, que espera a que los hilos terminen de
  verdad y liberen sus permisos. La prueba de regresión bloquea la limpieza y comprueba que el procesamiento no
  termine antes de tiempo.
- **Contrato JSON:** había cinco entradas que se aceptaban y no debían: ID numérico, ID booleano, fecha numérica,
  contenido después del JSON y claves duplicadas. Ahora el mapper rechaza la conversión automática a texto, las
  claves duplicadas y los tokens sobrantes. El DTO externo recibe la fecha como texto y la convierte a `Instant` de
  forma explícita. Los mensajes inválidos siguen yendo a inbox/DLT y nunca se procesan como pedidos. Si falla el
  guardado de su resultado, no se confirma el offset.
- **Apagado en Go:** que `Serve` retorne no significa que `Shutdown` haya terminado de esperar las peticiones activas.
  Ahora esperamos a que termine, con un límite de cinco segundos; si se agota, se fuerza el cierre. Hay pruebas para
  una petición bloqueada durante el apagado y para un error del listener.
- **Pruebas HTTP:** las pruebas que no son de timeout tienen dos segundos de margen. Solo la prueba de timeout usa
  100 ms. Así, un runner que tarda en arrancar no se confunde con un fallo real.