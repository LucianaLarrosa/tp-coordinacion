import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

EXCHANGE_NAME = "sum_eof_exchange"
ROUTING_KEY = "sum_eof"
EXCHANGE_TYPE = "fanout"

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        # Para publicar el EOF recibido
        self.eof_publish_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_NAME, [ROUTING_KEY], EXCHANGE_TYPE
        )

        # Para consumir los EOFs 
        self.eof_consume_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_NAME, [ROUTING_KEY], EXCHANGE_TYPE
        )
        
        self._lock = threading.Lock()
        self.amount_by_fruit = {}  # {client_id: {fruit: FruitItem}}

        # SIGTERM handling
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        with self._lock:
            fruits_client = self.amount_by_fruit.get(
                client_id, {}
            )
            fruits_client[fruit] = fruits_client.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            self.amount_by_fruit[client_id] = fruits_client

    def _process_eof(self, client_id):
        logging.info("Sending data messages")
        for final_fruit_item in self.amount_by_fruit.get(client_id, {}).values():
            aggregator = int(hashlib.md5(final_fruit_item.fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT

            self.data_output_exchanges[aggregator].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info("Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        self.amount_by_fruit.pop(client_id, None)

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        with self._lock:
            self._process_eof(*fields)
        ack()

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self.eof_publish_exchange.send(message)
        ack()

    def start(self):
        t = threading.Thread(target=lambda: self.eof_consume_exchange.start_consuming(self.process_eof_message))
        t.start()
        self.input_queue.start_consuming(self.process_data_message)
        t.join()

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM")
        self.input_queue.stop_consuming()
        self.eof_consume_exchange.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
