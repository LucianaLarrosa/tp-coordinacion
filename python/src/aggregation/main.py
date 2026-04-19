import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}  # {client_id: {fruit: FruitItem}}
        self.eof_count = {}  # {client_id: eof_count}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top_client = self.fruit_top.get(client_id, {})
        fruit_top_client[fruit] = fruit_top_client.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.fruit_top[client_id] = fruit_top_client

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1

        if self.eof_count[client_id] < SUM_AMOUNT:
            return

        sorted_fruits = sorted(self.fruit_top.get(client_id, {}).values(), reverse=True)
        top_sorted_fruits = sorted_fruits[:TOP_SIZE]
        fruit_top = []
        for fi in top_sorted_fruits:
            fruit_top.append((fi.fruit, fi.amount))

        self.output_queue.send(
            message_protocol.internal.serialize_data([client_id, fruit_top])
        )

        self.fruit_top.pop(client_id, None)
        self.eof_count.pop(client_id, None)

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        msg_type, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.DATA:
            self._process_data(*payload)
        elif msg_type == message_protocol.internal.MsgType.EOF:
            self._process_eof(*payload)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM")
        self.input_exchange.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    # SIGTERM handling
    signal.signal(signal.SIGTERM, aggregation_filter.handle_sigterm)

    try:
        aggregation_filter.start()
    finally:
        aggregation_filter.close()
        
    return 0


if __name__ == "__main__":
    main()
