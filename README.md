# Plataforma de pedidos B2B

Este proyecto procesa pedidos. Tiene tres partes:

- Un worker en Java (Spring Boot) que procesa los pedidos.
- Una API de productos en Go.
- Una API de clientes en NestJS.

Se comunican con Kafka y guardan los datos en MongoDB.

## Qué necesitas

- Docker con contenedores Linux y Docker Compose v2. Para Testcontainers, Docker Engine 25 o superior.
- Python 3.12 o superior para los scripts.
- Los puertos 3000, 8081, 9092 y 27017 libres.
- Si quieres correr las pruebas sin Docker: JDK 21, Maven 3.9+, Go 1.22+ y Node.js 22 con npm. Asegúrate de que Maven y
  tu IDE usen JDK 21.

Corre todos los comandos desde la carpeta raíz del proyecto. No necesitas credenciales ni servicios externos.

## Cómo levantarlo

```sh
docker compose up -d --build
docker compose ps
```

Esto arranca Kafka, MongoDB y los tres servicios. Para apagarlos sin perder los datos:

```sh
docker compose down
```

## Pruebas

Con Docker encendido:

```sh
python -m pip install -r scripts/requirements.txt
mvn -B -f order-processor/pom.xml clean verify
cd products-api
go test ./...
go vet ./...
cd ../clients-api
npm ci
npm run build
npm test
cd ..
python scripts/validate-contracts.py
```

Maven corre las pruebas unitarias y también las de integración, que levantan Kafka y MongoDB con Testcontainers.

Para probar todo el flujo de punta a punta, primero levanta Compose y luego corre:

```sh
python scripts/smoke.py
```

Este script usa IDs nuevos cada vez. Revisa que los pedidos se aprueben o rechacen bien, que los inválidos vayan a la
DLT, que no se procesen dos veces, que los importes cuadren y que todo se guarde y se publique en Kafka.

## Enviar pedidos de ejemplo

Con Compose encendido:

```sh
python scripts/publish.py examples/approved.json
python scripts/publish.py examples/approved-revision-2.json
python scripts/publish.py examples/rejected.json
python scripts/publish.py examples/invalid.json
```

El script usa el `orderId` como clave en Kafka. Si envías otra vez el mismo `eventId`, no pasa nada nuevo: el pedido no
se procesa dos veces.

## Revisar los resultados

```sh
docker compose exec mongo mongosh --quiet b2b --eval 'db.orders.find().toArray()'
docker compose exec mongo mongosh --quiet b2b --eval 'db.outbox.find().toArray()'
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders.processed.v1 --from-beginning --timeout-ms 10000
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders.processing.dlt --from-beginning --timeout-ms 10000
docker compose logs -f order-processor
```

El pedido aprobado de ejemplo debe quedar con estado `APPROVED` y un total de **2021.39 MXN**.

- Los pedidos aprobados y rechazados se publican en `orders.processed.v1`.
- Los mensajes inválidos y los errores técnicos van a la DLT.

El consumidor de Kafka se cierra solo después de 10 segundos sin mensajes. Eso es normal, no es un error.

## Configuración

Estos son los valores por defecto del worker cuando corre fuera de Compose:

| Variable                        | Valor                                          | Propósito                                       |
|---------------------------------|------------------------------------------------|-------------------------------------------------|
| `MONGODB_DATABASE`              | `b2b`                                          | Base de datos                                   |
| `MONGODB_URI`                   | `mongodb://localhost:27017/b2b?replicaSet=rs0` | Conexión MongoDB                                |
| `KAFKA_BOOTSTRAP_SERVERS`       | `localhost:9092`                               | Conexión Kafka                                  |
| `CLIENTS_URL`                   | `http://localhost:3000`                        | API de clientes                                 |
| `PRODUCTS_URL`                  | `http://localhost:8081`                        | API de productos                                |
| `WORKER_CONCURRENCY`            | `3`                                            | Consumidores Kafka                              |
| `PRODUCTS_CONCURRENCY`          | `20`                                           | Consultas de producto simultáneas por instancia |
| `HTTP_CONNECT_MS`               | `1000`                                         | Timeout de conexión                             |
| `HTTP_RESPONSE_MS`              | `2000`                                         | Timeout de petición                             |
| `HTTP_BACKOFF_MS`               | `200`                                          | Base de espera entre reintentos                 |
| `HTTP_RETRY_AFTER_MAX_MS`       | `2000`                                         | Tope de Retry-After                             |
| `OUTBOX_DELAY_MS`               | `1000`                                         | Pausa entre lotes de publicación                |
| `OUTBOX_RETRY_DELAY_MS`         | `5000`                                         | Espera tras un fallo de envío                   |
| `OUTBOX_MAX_PERMANENT_FAILURES` | `3`                                            | Fallos permanentes antes de aparcar una salida  |

Dentro de Compose, las direcciones de los servicios ya vienen configuradas. Puedes cambiar desde tu entorno estas
variables: `MONGODB_DATABASE`, `WORKER_CONCURRENCY`, `PRODUCTS_CONCURRENCY`, `OUTBOX_RETRY_DELAY_MS` y
`OUTBOX_MAX_PERMANENT_FAILURES`. Para cambiar cualquier otra, agrégala al bloque `environment` del servicio o usa un
archivo override de Compose.

Después de cambiar algo, reinicia el worker con `docker compose up -d --force-recreate order-processor`.

Otras notas:

- `PRODUCTS_CONCURRENCY` tiene que ser mayor que cero.
- Las APIs usan la variable `PORT`. Por defecto es 8081 en Go y 3000 en NestJS.
- Si cambias el nombre de la base de datos, cambia también `b2b` en los comandos de arriba.

## Limitaciones

- Es un entorno local: un solo broker de Kafka y un solo nodo de MongoDB. No tiene autenticación, TLS ni alta
  disponibilidad.
- Un mensaje puede publicarse más de una vez o llegar en otro orden. Quien lo consuma debe descartar repetidos por
  `eventId` y comparar `orderVersion`.
- No se puede reprocesar a mano un pedido con `TECHNICAL_FAILURE`. Para recuperarlo, el productor tiene que enviar un
  `eventId` nuevo con una versión más alta.
- No hay limpieza automática del inbox/outbox, ni métricas, pruebas de carga, caché o interfaz gráfica.
- El límite de consultas de productos es por instancia y solo se lee al arrancar. Para cambiarlo hay que reiniciar.

## Uso de IA

Durante el desarrollo se usaron herramientas de inteligencia artificial como apoyo. Esta tabla resume en qué se usó cada
una:

| Herramienta            | Uso                                                                                        |
|------------------------|--------------------------------------------------------------------------------------------|
| Codex/Astra 6          | Generación de código fuente, pruebas automatizadas, workflow de CI y scripts de prueba     |
| Claude Sonnet 5        | Generación de imágenes y diagramas para documentar el sistema, a partir de prompts propios |
| Asistente de redacción | Redacción de la documentación, corrección de estilo y gramática, y ejemplos de uso         |

### Revisión manual

Todo el código generado con IA se revisó y se adaptó para asegurar su calidad, que fuera coherente con la arquitectura y
que cumpliera los requisitos. Se revisaron con más cuidado estas partes, porque es donde es más fácil cometer errores
sutiles de negocio o de consistencia:

- **Idempotencia y concurrencia en `order-processor`:** índices únicos y condiciones atómicas en las actualizaciones.
- **Cálculo de importes:** impuestos y descuentos por cada línea del pedido.
- **Llamadas HTTP a Products API y Clients API:** manejo de errores y reintentos.

### Sugerencia rechazada:

![sugerencia-rechazada-AI.png](docs/images/sugerencia-rechazada-AI.png)