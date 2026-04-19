import os
import logging
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


class SumFilter:
    def __init__(self):
        self.input_consumer = middleware.MessageMiddlewareQueueAndExchangeRabbitMQ(
            MOM_HOST, INPUT_QUEUE, EXCHANGE_NAME
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_fruit = {}  # {client_id: {fruit: FruitItem}}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        fruits_client = self.amount_by_fruit.get(client_id, {})
        fruits_client[fruit] = fruits_client.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.amount_by_fruit[client_id] = fruits_client

    def _process_eof(self, client_id):
        logging.info("Sending data messages")
        for final_fruit_item in self.amount_by_fruit.get(client_id, {}).values():
            aggregator = (
                int(hashlib.md5(final_fruit_item.fruit.encode()).hexdigest(), 16)
                % AGGREGATION_AMOUNT
            )

            self.data_output_exchanges[aggregator].send(
                message_protocol.internal.serialize_data(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info("Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize_eof([client_id])
            )

        self.amount_by_fruit.pop(client_id, None)

    def process_eof_message(self, message, ack, nack):
        _, payload = message_protocol.internal.deserialize(message)
        self._process_eof(*payload)
        ack()

    def process_data_message(self, message, ack, nack):
        msg_type, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.DATA:
            self._process_data(*payload)
        elif msg_type == message_protocol.internal.MsgType.EOF:
            self.input_consumer.send(message)
        ack()

    def start(self):
        self.input_consumer.start_consuming(
            self.process_data_message, self.process_eof_message
        )

    def close(self):
        self.input_consumer.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM")
        self.input_consumer.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    # SIGTERM handling
    signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)

    try:
        sum_filter.start()
    finally:
        sum_filter.close()
        
    return 0


if __name__ == "__main__":
    main()
