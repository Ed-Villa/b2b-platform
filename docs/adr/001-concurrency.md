# ADR 001 — consumidor síncrono y productos concurrentes con virtual threads

## Contexto

Cada pedido necesita una consulta de cliente y N consultas de producto. La latencia de N consultas secuenciales se
acumula aproximadamente de forma lineal. Los productos son independientes, pero un cliente no elegible permite
rechazar el pedido sin consultarlos. El cliente HTTP y sus reintentos ya son blocking.

## Alternativas

- HTTP secuencial: limita carga, pero acumula latencia de productos independientes.
- Cliente y productos simultáneos: reduce latencia, pero consulta productos incluso para clientes no elegibles.
- Reactor: permite concurrencia, pero exige cambiar el modelo blocking sin necesidad para este caso.
- Virtual threads sin límite o con semáforo por pedido: no acotan la carga agregada hacia Products API.

## Decisión

- Mantener consumidor tradicional, secuencial por partición y con concurrencia configurable. La clave es `orderId`;
  las restricciones y transacciones MongoDB siguen siendo la garantía de consistencia.
- Consultar el cliente primero. Si es elegible, crear una tarea virtual por producto usando un executor por pedido.
- Compartir un semáforo justo en el caso de uso singleton entre todos los pedidos. Spring inyecta
  `providers.products-concurrency`, con default 20 y variable `PRODUCTS_CONCURRENCY` en Compose.
  Debe ser positivo; un valor inválido impide arrancar. Cambiar la propiedad requiere reiniciar/recrear el worker;
  no se incorpora un servicio de refresco en caliente.
- Mantener el permiso durante cada consulta lógica, incluidos hasta tres intentos y sus backoffs. Adquirirlo de
  forma interrumpible y liberarlo en `finally`. Los reintentos no crean nuevas tareas ni adquieren otro permiso.
- Recoger resultados en orden de entrada. Ante varios fallos, elegir el primero por posición, conservando causa,
  clasificación y número de intentos. Cancelar tareas restantes y cerrar el executor esperando su terminación.
- Si se interrumpe el procesamiento, preservar la interrupción y propagar el error sin persistir ni confirmar offset.
  Solo guardar cuando haya resultado completo o fallo externo clasificado; nunca importes parciales.

## Consecuencias y revisión

- Menor espera de enriquecimiento por solapamiento de I/O; siguen existiendo N consultas de producto más una de cliente,
  antes de contar retries. La concurrencia no convierte el contrato en una consulta batch.
- El semáforo limita consultas lógicas por instancia, no virtual threads en espera ni solicitudes por segundo. Varias
  réplicas multiplican el límite agregado. Retener permisos durante backoff protege al proveedor a costa de capacidad.
- El orden determinista puede esperar una consulta anterior aunque otra posterior ya haya fallado. Algunas consultas
  pueden completarse antes de que la cancelación las alcance; sus resultados no se persisten parcialmente.
- Mantener máximo de 100 líneas, un registro por poll y ventana de 30 minutos; incluir espera por permisos y retries
  al evaluar ese presupuesto. No se promete mejora cuantitativa sin pruebas de carga.
- Revisar el límite y la ventana ante latencia p95, lag, espera por permisos o saturación del proveedor. Considerar
  endpoint batch si reducir el número de solicitudes aporta más que aumentar la concurrencia.
