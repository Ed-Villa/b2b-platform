# Organización de clases y archivos

Este inventario describe la implementación actual. Los servicios tienen contratos compartidos, pero conservan una
organización idiomática por lenguaje. Los paths de cada tabla son relativos al directorio indicado.

## Mapa del repositorio

| Ruta                       | Responsabilidad                                                                                |
|----------------------------|------------------------------------------------------------------------------------------------|
| `order-processor/`         | Worker Java 21/Spring Boot: reglas, enriquecimiento, idempotencia, persistencia y publicación. |
| `products-api/`            | API Go de catálogo por mercado, con semillas en memoria.                                       |
| `clients-api/`             | API NestJS de clientes, con validación y repositorio sustituible.                              |
| `contracts/`               | JSON Schema de eventos y OpenAPI de proveedores.                                               |
| `examples/`                | Entradas aprobadas/rechazadas/inválidas y ejemplos de salida; incluye revisión de orden 2.     |
| `scripts/`                 | Publicación, validación de contratos, smoke y simulación de fallos HTTP.                       |
| `docs/`                    | Arquitectura, ADR, liderazgo, operación, diagramas y evidencia.                                |
| `docker-compose.yml`       | Kafka, Mongo replica set, inicialización y los tres servicios.                                 |
| `.github/workflows/ci.yml` | Compilación, tests, contratos, Testcontainers, Compose y smoke.                                |

## Worker Java

Código bajo `order-processor/src/main/java/com/apex/orders/`.

### Dominio

| Archivo en `domain/` | Tipo y responsabilidad                                                                              |
|----------------------|-----------------------------------------------------------------------------------------------------|
| `Order.java`         | Record del snapshot de pedido: identidad, revisión de negocio, mercado, moneda y líneas de entrada. |
| `Item.java`          | Record con producto, cantidad y precio unitario recibido.                                           |
| `Client.java`        | Record con los datos de cliente que usan las reglas.                                                |
| `Product.java`       | Record con los datos de catálogo que usan las reglas.                                               |
| `Line.java`          | Record de línea enriquecida y sus importes.                                                         |
| `Totals.java`        | Record de subtotal bruto, descuento, neto, impuesto y total.                                        |
| `Result.java`        | Record del estado, cliente, líneas, totales y razón; fábrica de resultados de fallo.                |
| `Rules.java`         | Validación, elegibilidad, impuestos, descuento y redondeo monetario.                                |

Cada record tiene archivo propio. El dominio usa Java estándar y `BigDecimal`; no importa Spring, Kafka, MongoDB,
clientes HTTP ni DTOs de adaptadores. `eventVersion` pertenece al contrato externo; `orderVersion` pertenece a la orden.

### Aplicación y puertos

| Archivo en `application/` | Responsabilidad                                                                                                             |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `ProcessOrder.java`       | Caso de uso: valida, comprueba duplicado conocido, consulta cliente, enriquece productos, calcula y guarda.                 |
| `ConcurrentProducts.java` | Virtual threads por pedido, semáforo compartido por instancia, orden de resultados y cancelación con espera de terminación. |
| `Ports.java`              | Agrupa los contratos que necesita la aplicación y los tipos asociados a ese límite.                                         |

Tipos contenidos en `Ports`:

- `Catalog`: consultas de cliente y producto; implementado por `HttpCatalog`.
- `Store`: consulta de evento conocido, persistencia de resultado y disposición de mensaje inválido; implementado por
  `MongoStore`.
- `Envelope`: entrada original, tópico, partición, offset y recepción; calcula la identidad de la recepción.
- `ExternalFailure`: fallo de proveedor con clasificación de inexistencia y número de intentos.

El punto de entrada de aplicación es `ProcessOrder.process`; no se añade una interfaz de entrada sin un consumidor que
la necesite. El adaptador Kafka invoca ese caso de uso. La aplicación depende de dominio y puertos, no de adaptadores.

### Adaptadores Kafka, HTTP y JSON

| Archivo en `adapters/` | Responsabilidad                                                                                                                    |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `CreatedEvent.java`    | DTO de entrada; valida el esquema, convierte fecha textual a `Instant` y mapea al dominio. Contiene `ItemDto`.                     |
| `Json.java`            | Construye el mapper común: fechas, tipos estrictos, rechazo de claves duplicadas/tokens adicionales y tolerancia de campos nuevos. |
| `OrderListener.java`   | Recibe Kafka, valida entrada, registra inválidos, invoca la aplicación y confirma offset después de persistir.                     |
| `HttpCatalog.java`     | Implementa `Catalog` con HTTP blocking, timeouts, retries, clasificación de errores y headers de correlación.                      |
| `OutboxPublisher.java` | Envía salidas pendientes y luego marca envío; aplaza errores transitorios y aparca errores permanentes.                            |

`HttpCatalog` conserva `ClientDto` y `ProductDto` como records privados porque solo se usan en ese adaptador; cada uno
mapea explícitamente al record de dominio. `OutboxPublisher.Sender` es el límite de envío sustituible en pruebas;
su implementación Kafka usa `KafkaTemplate`. El publicador depende de operaciones concretas de outbox en `MongoStore`,
no de una interfaz adicional que no se necesita en esta entrega.

### Persistencia MongoDB

| Archivo en `adapters/mongo/` | Responsabilidad                                                                                                            |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `MongoStore.java`            | Fachada pública, implementación de `Store`, compatibilidad del almacenamiento, concerns e índices. Delega las operaciones. |
| `MongoOrderStore.java`       | Transacciones de inbox, revisiones, estado actual y creación de outbox; deduplicación y resolución de versiones.           |
| `MongoOutboxStore.java`      | Inserción transaccional de salidas, lectura de pendientes, contadores, espera, aparcamiento y marcado de envío.            |
| `EventDocuments.java`        | Construye documentos de recepción, evento procesado y DLT; conserva original y metadata recuperable.                       |
| `BsonConverter.java`         | Mapea resultados a BSON y preserva importes monetarios como Decimal128.                                                    |
| `Disposition.java`           | Enum de disposiciones persistidas: PROCESSED, CONFLICT, OBSOLETE e INVALID.                                                |
| `DltCategory.java`           | Enum de categorías DLT: VALIDATION, VERSION_CONFLICT y EXTERNAL_TECHNICAL.                                                 |

Los documentos BSON son modelos internos de persistencia. Los colaboradores de la fachada permanecen dentro del
paquete; no exponen MongoDB al dominio. La separación de clases conserva el mismo `ClientSession` al escribir inbox,
revisión, pedido y outbox: no divide la transacción.

Las colecciones tienen responsabilidades diferentes: `inbox` registra identidades/disposiciones; `revisions` conserva
la revisión ganadora y su snapshot; `orders` contiene el estado vigente; `outbox` conserva salidas y su seguimiento;
`metadata` identifica el formato de almacenamiento.

### Configuración y pruebas

- `OrdersApplication.java`: arranque Spring, beans, configuración de proveedores, concurrencia, publicador y error
  handler Kafka.
- `src/main/resources/application.yml`: parámetros externalizados de Mongo, Kafka, HTTP, concurrencia y outbox.
- `pom.xml`: Java 21, dependencias, JUnit y fases de pruebas unitarias/integración.
- `Dockerfile`: construcción Maven y ejecución con JRE 21.

En `src/test/java/com/apex/orders/`:

| Archivo                       | Cobertura                                                                                      |
|-------------------------------|------------------------------------------------------------------------------------------------|
| `RulesTest.java`              | Cálculo por mercado/categoría, descuentos, exención, redondeo, elegibilidad y validaciones.    |
| `ProcessOrderTest.java`       | Orquestación del caso de uso y clasificación de resultados.                                    |
| `ConcurrentProductsTest.java` | Virtual threads, límite compartido, orden, cancelación, interrupción y liberación de permisos. |
| `HttpCatalogTest.java`        | Dobles HTTP para retries, timeout, errores definitivos y respuestas malformadas.               |
| `ListenerTest.java`           | Contrato Kafka, versiones/tipos inválidos y política de confirmación del offset.               |
| `ConsistencyIT.java`          | Mongo/Kafka reales: concurrencia, idempotencia, rollback, auditoría, outbox y flujo completo.  |

`src/test/resources/docker-java.properties` fija la API Docker usada por Testcontainers. `mvn test` ejecuta unitarias;
`mvn verify` añade `ConsistencyIT`. Los resultados vigentes están en [verification.md](verification.md).

## Products API Go

Archivos bajo `products-api/`; todos comparten `package main`. Go agrupa tipos relacionados por responsabilidad del
archivo; no requiere un archivo por struct.

| Archivo            | Tipos y funciones principales                                                                                          |
|--------------------|------------------------------------------------------------------------------------------------------------------------|
| `catalog.go`       | `Product`, `ProductRepository`, `Catalog`, `MemoryRepository`, `ErrNotFound` y `Seed`; 12 productos en tres mercados.  |
| `http.go`          | `Handler`, validación de ID/mercado, propagación de contexto y deadline, `respond`, `fail`, healthcheck y correlación. |
| `main.go`          | Configuración de puerto/timeouts, señales, listener y `serve`; espera el drenado de peticiones al apagar.              |
| `catalog_test.go`  | Repositorio, cancelación de contexto, búsqueda por mercado, handlers y errores.                                        |
| `shutdown_test.go` | Petición activa durante apagado y finalización ante fallo del listener.                                                |
| `go.mod`           | Identidad del módulo y versión mínima de Go.                                                                           |
| `Dockerfile`       | Pruebas, compilación y ejecución del binario con usuario sin privilegios.                                              |

Dependencias: handler → catálogo → interfaz de repositorio. `MemoryRepository` implementa esa interfaz y recibe
`context.Context`. Sustituirlo por almacenamiento persistente no exige cambiar el contrato HTTP.

## Clients API NestJS

Código bajo `clients-api/src/`.

| Archivo                                | Tipo o función y responsabilidad                                                               |
|----------------------------------------|------------------------------------------------------------------------------------------------|
| `main.ts`                              | `bootstrap`: crea la aplicación, aplica configuración HTTP y escucha en el puerto configurado. |
| `app.module.ts`                        | `AppModule`: registra controllers, servicio y binding de `ClientRepository` a `MemoryClients`. |
| `http.ts`                              | `configure`: middleware de correlación/logs, ValidationPipe, ErrorFilter y hooks de apagado.   |
| `clients/client.ts`                    | `Client`: contrato TypeScript de los datos que maneja el servicio de clientes.                 |
| `clients/client-params.dto.ts`         | `ClientParams`: validación explícita del parámetro `clientId`.                                 |
| `clients/client.repository.ts`         | `ClientRepository`: clase abstracta usada como contrato y token de inyección.                  |
| `clients/memory-clients.repository.ts` | `MemoryClients`: seis clientes de MX/CO/PE; devuelve una copia del registro encontrado.        |
| `clients/clients.service.ts`           | `ClientsService`: consulta el repositorio y reporta ausencia del cliente.                      |
| `clients/clients.controller.ts`        | `ClientsController`: endpoint `GET /clients/:clientId`; delega al servicio.                    |
| `health/health.controller.ts`          | `HealthController`: endpoint `GET /health`.                                                    |
| `common/error.filter.ts`               | `ErrorFilter`: errores con `code`, `message` y `correlationId`.                                |

Dependencias: controller → servicio → repositorio abstracto. Las semillas no residen en el controller. La API pequeña
usa excepciones de NestJS en el servicio; no pretende replicar la separación de dominio del worker Java.

- `test/clients.spec.ts`: consulta de servicio, recurso ausente, endpoint, parámetros inválidos, correlación y salud.
- `tsconfig.json`: compilación TypeScript estricta.
- `package.json` y `package-lock.json`: scripts, dependencias y resolución reproducible.
- `Dockerfile`: instalación desde lockfile, build, pruebas y runtime con dependencias de producción.

## Contratos, scripts e infraestructura

| Archivo o ruta                                | Responsabilidad                                                                 |
|-----------------------------------------------|---------------------------------------------------------------------------------|
| `contracts/orders.created.v1.schema.json`     | Entrada: esquema 1 y revisión positiva de un snapshot completo de orden.        |
| `contracts/orders.processed.v1.schema.json`   | Salida de aprobación/rechazo, totales, razón e identidad de origen.             |
| `contracts/orders.processing.dlt.schema.json` | Mensaje original, metadata y clasificación de fallos.                           |
| `contracts/products.openapi.json`             | API de productos, query market, errores y salud.                                |
| `contracts/clients.openapi.json`              | API de clientes, errores y salud.                                               |
| `scripts/publish.py`                          | Publica ejemplos con clave Kafka `orderId` mediante Compose.                    |
| `scripts/validate-contracts.py`               | Valida schemas, OpenAPI y ejemplos.                                             |
| `scripts/smoke.py`                            | Comprueba APIs reales, MongoDB, outbox y eventos leídos del broker.             |
| `scripts/fake-provider.py`                    | Proveedor controlable para errores, latencia y recuperación; solo pruebas.      |
| `scripts/requirements.txt`                    | Dependencias Python para validación y smoke.                                    |
| `docker-compose.yml`                          | Servicios, healthchecks, replica set, tópicos, volúmenes y configuración local. |
| `.github/workflows/ci.yml`                    | Verificación en push/PR; recoge reportes y logs y detiene Compose al finalizar. |

## Documentación y recorrido de lectura

Empezar por [architecture-proposal.md](architecture-proposal.md) y los [diagramas](diagrams.md). Para seguir una orden:
`OrderListener → CreatedEvent → ProcessOrder → HttpCatalog/ConcurrentProducts → Rules → MongoStore/MongoOrderStore`.
Después del commit transaccional, `OutboxPublisher → MongoStore/MongoOutboxStore → Kafka` completa la publicación.