from common import message_protocol

import uuid


class MessageHandler:

    def __init__(self):
        self._client_id = str(uuid.uuid4())
        self._msg_count = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self._msg_count += 1
        return message_protocol.internal.serialize_data(
            [self._client_id, fruit, amount]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize_eof(
            [self._client_id, self._msg_count]
        )

    def deserialize_result_message(self, message):
        _, payload = message_protocol.internal.deserialize(message)
        client_id, fruit_top = payload
        if client_id != self._client_id:
            return None
        return fruit_top
