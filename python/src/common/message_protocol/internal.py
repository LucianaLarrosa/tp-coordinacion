import json


class MsgType:
    DATA = 1
    EOF = 2
    QUERY = 3
    RESPONSE = 4
    CONFIRM = 5


def serialize_data(message):
    return json.dumps([MsgType.DATA, message]).encode("utf-8")


def serialize_eof(message):
    return json.dumps([MsgType.EOF, message]).encode("utf-8")   

def serialize_query(message): # message = [coordinator_id, client_id] -> El coordinador solicita que le manden a él los datos de ese cliente
    return json.dumps([MsgType.QUERY, message]).encode("utf-8")

def serialize_response(message): # message = [sum_id, coordinator_id, client_id, msg_count]
    return json.dumps([MsgType.RESPONSE, message]).encode("utf-8")

def serialize_confirm(message): # message = [client_id]
    return json.dumps([MsgType.CONFIRM, message]).encode("utf-8")

def deserialize(message):
    msg_type, payload = json.loads(message.decode("utf-8"))
    return msg_type, payload
