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
    def proto_message_to_json_str(cls, proto_message):
        return MessageToJson(
            message=proto_message,
            preserving_proto_field_name=True
        )

    @classmethod
    def json_str_to_proto_message(cls, proto_type, json_str):
        proto_message = proto_type()
        return Parse(
            text=json_str,
            message=proto_message,
            ignore_unknown_fields=True
        )
