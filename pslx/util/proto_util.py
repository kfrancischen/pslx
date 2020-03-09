from google.protobuf.any_pb2 import Any
import google.protobuf.text_format as text_format
import google.protobuf.json_format as json_format
import importlib
from pslx.core.exception import ProtobufNameNotExistException, ProtobufValueNotExistException,\
    ProtobufEnumTypeNotExistException, ProtobufMessageTypeNotExistException


class ProtoUtil(object):

    ENUMS_MODULE = "pslx.schema.enums_pb2"
    MESSAGE_MODULE = [
        'pslx.schema.common_pb2',
        'pslx.schema.rpc_pb2',
        'pslx.schema.rpc_pb2_grpc',
        'pslx.schema.snapshots_pb2',
        'pslx.schema.storage_pb2',
    ]

    @classmethod
    def check_valid_enum(cls, enum_type, value):
        return value in enum_type.values()

    @classmethod
    def get_name_by_value(cls, enum_type, value):
        try:
            return enum_type.Name(number=value)
        except AttributeError as _:
            raise ProtobufValueNotExistException

    @classmethod
    def get_value_by_name(cls, enum_type, name):
        try:
            return enum_type.Value(name=name)
        except AttributeError as _:
            raise ProtobufNameNotExistException

    @classmethod
    def get_name_by_value_and_enum_name(cls, enum_type_str, value):
        module = importlib.import_module(cls.ENUMS_MODULE)
        try:
            enum_type = getattr(module, enum_type_str)
            return cls.get_name_by_value(enum_type=enum_type, value=value)
        except AttributeError as _:
            raise ProtobufEnumTypeNotExistException

    @classmethod
    def get_value_by_name_and_enum_name(cls, enum_type_str, name):
        module = importlib.import_module(cls.ENUMS_MODULE)
        try:
            enum_type = getattr(module, enum_type_str)
            return cls.get_value_by_name(enum_type=enum_type, name=name)
        except AttributeError as _:
            raise ProtobufEnumTypeNotExistException

    @classmethod
    def message_to_json(cls, proto_message):
        return json_format.MessageToJson(
            message=proto_message,
            preserving_proto_field_name=True
        )

    @classmethod
    def message_to_text(cls, proto_message):
        return text_format.MessageToString(
            message=proto_message
        )

    @classmethod
    def json_to_message(cls, message_type, json_str):
        proto_message = message_type()
        return json_format.Parse(
            text=json_str,
            message=proto_message,
            ignore_unknown_fields=True
        )

    @classmethod
    def text_to_message(cls, message_type, text_str):
        proto_message = message_type()
        return text_format.Parse(
            text=text_str,
            message=proto_message,
            allow_unknown_field=True
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

    @classmethod
    def infer_message_type_from_str(cls, message_type_str):
        for module_name in cls.MESSAGE_MODULE:
            try:
                module = importlib.import_module(module_name)
                message_type = getattr(module, message_type_str)
                return message_type
            except AttributeError as _:
                pass
        raise ProtobufMessageTypeNotExistException
