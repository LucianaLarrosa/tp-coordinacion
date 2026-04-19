import json


class MsgType:
    DATA = 1
    EOF = 2


def serialize_data(message):
    return json.dumps([MsgType.DATA, message]).encode("utf-8")


def serialize_eof(message):
    return json.dumps([MsgType.EOF, message]).encode("utf-8")


def deserialize(message):
    msg_type, payload = json.loads(message.decode("utf-8"))
    return msg_type, payload
