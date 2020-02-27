from google.protobuf.any_pb2 import Any
from google.protobuf.json_format import MessageToJson, Parse
from pslx.core.exception import ProtobufNameNotExistException, ProtobufValueNotExistException


class ProtoUtil(object):

    @classmethod
    def check_valid_enum(cls, enum_type, value):
        return value in enum_type.values()

    @classmethod
    def get_name_by_value(cls, enum_type, value):
        try:
            return enum_type.Name(number=value)
        except ProtobufValueNotExistException as _:
            return None

    @classmethod
    def get_value_by_name(cls, enum_type, name):
        try:
            return enum_type.Value(name=name)
        except ProtobufNameNotExistException as _:
            return None

    @classmethod
    def message_to_json(cls, proto_message):
        return MessageToJson(
            message=proto_message,
            preserving_proto_field_name=True
        )

    @classmethod
    def json_to_message(cls, message_type, json_str):
        proto_message = message_type()
        return Parse(
            text=json_str,
            message=proto_message,
            ignore_unknown_fields=True
        )

    @classmethod
    def message_to_any(cls, message):
        any_message = Any()
        any_message.Pack(message)
        return any_message

    @classmethod
    def any_to_message(cls, message_type, any_message):
        proto_message = message_type()
        any_message.Unpack(proto_message)
        return proto_message
