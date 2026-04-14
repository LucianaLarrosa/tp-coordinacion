import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

_MESSAGE_ERRORS = (
    pika.exceptions.AMQPError,
    pika.exceptions.ChannelError,
    pika.exceptions.ReentrancyError,
)

def _make_pika_callback(on_message_callback):
    # Adapta la firma del callback de pika a la interfaz del middleware.
    def pika_callback(ch, method, properties, body):
        ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
        on_message_callback(body, ack, nack)

    return pika_callback


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.confirm_delivery()

    def start_consuming(self, on_message_callback):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_make_pika_callback(on_message_callback),
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except _MESSAGE_ERRORS:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except _MESSAGE_ERRORS:
            raise MessageMiddlewareMessageError()

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareCloseError()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="direct", durable=True
        )
        self.channel.confirm_delivery()

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=queue_name,
                    routing_key=routing_key,
                )
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=_make_pika_callback(on_message_callback),
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except _MESSAGE_ERRORS:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except _MESSAGE_ERRORS:
            raise MessageMiddlewareMessageError()

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareCloseError()
