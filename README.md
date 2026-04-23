# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Informe

### Escalabilidad

**Clientes:** cada cliente recibe un `client_id` único (UUID) generado en el gateway al momento de conectarse. Este ID viaja en todos los mensajes internos, lo que permite que Sum, Aggregation y Joiner mantengan estado separado por cliente y procesen múltiples clientes concurrentemente sin interferencia. El joiner incluye el `client_id` en el resultado final, lo que permite al gateway entregar la respuesta al cliente correspondiente. 

**Controles:** la cantidad de instancias de Sum y Aggregation se configura únicamente a través de variables de entorno en el docker-compose. El código lee estos valores al arrancar y se adapta sin requerir modificaciones. Agregar más instancias de Sum divide el trabajo de acumulación entre más instancias. Agregar más Aggregation distribuye las frutas entre más instancias, usando hash consistente sobre la combinación de `client_id` y nombre de la fruta módulo `AGGREGATION_AMOUNT`, lo que mejora la distribución cuando hay pocas frutas distintas. 

### Coordinación entre instancias de Sum

Cada Sum mantiene 2 hilos: Thread 1 consume de la `input_queue` procesando datos y EOFs, y Thread 2 consume un `fanout exchange` compartido por todos los Sum para coordinar el envío de datos al Aggregator.

Cada Sum lleva un contador de mensajes procesados por cliente (`_msg_count`). El gateway incluye en el mensaje de EOF la cantidad total de mensajes enviados por ese cliente.

Cuando Thread 1 recibe el EOF de un cliente, actúa como **coordinador**: publica una `QUERY`en el fanout exchange preguntando a todos los Sum cuántos mensajes procesaron de ese cliente. Cada Sum (incluyendo el coordinador) responde con su conteo a través del mismo exchange. El coordinador acumula las respuestas y cuando recibe todas (`SUM_AMOUNT`), suma los conteos y compara con el total del EOF. Si coinciden, publica un `CONFIRM`; si no, reintenta la query. Cuando cada Sum recibe el `CONFIRM`, envía sus datos acumulados al Aggregator correspondiente. 

Este modelo garantiza que ningún Sum envíe datos al Aggregator hasta que todos hayan terminado de procesar los mensajes de ese cliente, sin depender de parámetros del middleware como `prefetch_count`.

#### Evolución del diseño

Una primera implementación resolvía el problema a nivel del middleware, a través de una clase adicional que permitía que la `input_queue` y el exchange de coordinación consumieran sobre un mismo canal de RabbitMQ. Al compartir canal, el consumo de ambos quedaba serializado: los callbacks se ejecutaban uno a la vez, de forma tal que cuando un Sum recibía el `EOF` no tenía mensajes de datos pendientes de procesar en ese canal. Esta sincronización requería además configurar `prefetch_count=1`, para limitar a un mensaje por vez en el buffer del consumidor.

El principal problema de esta solución era que la correctitud del sistema pasaba a depender de una configuración específica del middleware. Además, requería una clase del middleware con una semántica particular (compartir un canal entre una cola y un exchange) que no formaba parte de la interfaz original.

El protocolo de coordinación actual (`QUERY` / `RESPONSE` / `CONFIRM`) resuelve el mismo problema a nivel aplicación: la sincronización entre Sums se logra explícitamente mediante mensajes de control, comparando el total informado en el `EOF` contra la suma de los conteos reportados por cada Sum. Esto permitió volver a la interfaz original del middleware (sin canales compartidos) y que el sistema siga siendo correcto independientemente del valor de `prefetch_count`.

### Coordinación entre instancias de Aggregator

Cada Aggregator recibe datos de todas las instancias de Sum, pero solo para las frutas que le corresponden según el hash consistente (`hashlib.md5(client_id + fruta) % AGGREGATION_AMOUNT`). Esto mejora la distribución cuando hay pocas frutas distintas, ya que la misma fruta de distintos clientes puede ir a distintos Aggregators.

Para detectar cuándo todos los Sum terminaron de enviar sus datos, cada Aggregator cuenta los `EOF`s recibidos, ya que espera exactamente `SUM_AMOUNT` `EOF`s (uno por cada instancia de Sum) antes de calcular el top parcial y enviarlo al Joiner. 