import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.partial_tops = {}  # {client_id: [fruit_top]}
        self.top_count = {}  # {client_id: count}

    def process_message(self, message, ack, nack):
        logging.info("Received top")
        try:
            _, payload = message_protocol.internal.deserialize(message)
            client_id, fruit_top = payload

            self.partial_tops[client_id] = self.partial_tops.get(client_id, []) + fruit_top
            self.top_count[client_id] = self.top_count.get(client_id, 0) + 1

            if self.top_count[client_id] < AGGREGATION_AMOUNT:
                return

            self.partial_tops[client_id].sort(key=lambda x: x[1], reverse=True)
            total_top = self.partial_tops[client_id][:TOP_SIZE]
            self.output_queue.send(
                message_protocol.internal.serialize_data([client_id, total_top])
            )

            self.partial_tops.pop(client_id, None)
            self.top_count.pop(client_id, None)
        finally:
            ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def close(self):
        for resource in [self.input_queue, self.output_queue]:
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
    join_filter = JoinFilter()

    # SIGTERM handling
    signal.signal(signal.SIGTERM, join_filter.handle_sigterm)

    try:
        join_filter.start()
    finally:
        join_filter.close()

    return 0


if __name__ == "__main__":
    main()
