import os
import logging
import hashlib
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

EXCHANGE_NAME = "sum_query_exchange"
ROUTING_KEY = "sum_query"
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

        # Para publicar QUERY, RESPONSE y CONFIRM
        self.msg_publish_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_NAME, [ROUTING_KEY], EXCHANGE_TYPE
        )

        # Para consumir QUERY, RESPONSE y CONFIRM
        self.msg_consume_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_NAME, [ROUTING_KEY], EXCHANGE_TYPE
        )

        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._amount_by_fruit = {}  # {client_id: {fruit: FruitItem}}
        self._msg_count = {}  # {client_id: count}
        self._total_count = {}  # {client_id: total_count}
        self._responses = {}  # {client_id: {sum_id: count}}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        with self._lock:
            fruits_client = self._amount_by_fruit.get(client_id, {})
            fruits_client[fruit] = fruits_client.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            self._msg_count[client_id] = self._msg_count.get(client_id, 0) + 1

            self._amount_by_fruit[client_id] = fruits_client

    def _process_eof(self, client_id):
        logging.info("Sending data messages")
        for final_fruit_item in self._amount_by_fruit.get(client_id, {}).values():
            aggregator = (
                int(
                    hashlib.md5(
                        (client_id + final_fruit_item.fruit).encode()
                    ).hexdigest(),
                    16,
                )
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

        self._amount_by_fruit.pop(client_id, None)

    def _handle_query(self, coordinator_id, client_id):
        with self._lock:
            count = self._msg_count.get(client_id, 0)
        with self._publish_lock:
            self.msg_publish_exchange.send(
                message_protocol.internal.serialize_response(
                    [ID, coordinator_id, client_id, count]
                )
            )

    def _handle_confirm(self, client_id):
        with self._lock:
            self._process_eof(client_id)
            self._msg_count.pop(client_id, None)
            self._total_count.pop(client_id, None)

    def _handle_response(self, sum_id, coordinator_id, client_id, count):
        if coordinator_id != ID:
            return

        should_confirm = False
        should_retry = False
        with self._lock:
            responses_client = self._responses.get(client_id, {})
            responses_client[sum_id] = count
            self._responses[client_id] = responses_client

            if len(responses_client) < SUM_AMOUNT:
                return

            total = sum(responses_client.values())
            self._responses.pop(client_id, None)
            if total == self._total_count.get(client_id, 0):
                should_confirm = True
            else:
                should_retry = True
        if should_confirm:
            with self._publish_lock:
                self.msg_publish_exchange.send(
                    message_protocol.internal.serialize_confirm([client_id])
                )
        elif should_retry:
            with self._publish_lock:
                self.msg_publish_exchange.send(
                    message_protocol.internal.serialize_query([ID, client_id])
                )

    def process_exchange_message(self, message, ack, nack):
        try:
            msg_type, payload = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.QUERY:
                coordinator_id, client_id = payload
                self._handle_query(coordinator_id, client_id)
            elif msg_type == message_protocol.internal.MsgType.CONFIRM:
                client_id = payload[0]
                self._handle_confirm(client_id)
            elif msg_type == message_protocol.internal.MsgType.RESPONSE:
                sum_id, coordinator_id, client_id, count = payload
                self._handle_response(sum_id, coordinator_id, client_id, count)
        finally:
            ack()

    def process_data_message(self, message, ack, nack):
        try:
            msg_type, payload = message_protocol.internal.deserialize(message)
            if msg_type == message_protocol.internal.MsgType.DATA:
                self._process_data(*payload)
            elif msg_type == message_protocol.internal.MsgType.EOF:
                client_id, total = payload
                self._total_count[client_id] = total
                with self._publish_lock:
                    self.msg_publish_exchange.send(
                        message_protocol.internal.serialize_query([ID, client_id])
                    )
        finally:
            ack()

    def start(self):
        t1 = threading.Thread(
            target=lambda: self.msg_consume_exchange.start_consuming(
                self.process_exchange_message
            )
        )
        t1.start()
        try:
            self.input_queue.start_consuming(self.process_data_message)
        finally:
            self.msg_consume_exchange.stop_consuming()
            t1.join()

    def close(self):
        for resource in [
            self.input_queue,
            self.msg_publish_exchange,
            self.msg_consume_exchange,
            *self.data_output_exchanges,
        ]:
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing resource")

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("Error stopping consumer on SIGTERM")

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
