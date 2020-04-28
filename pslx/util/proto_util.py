from google.protobuf.any_pb2 import Any
import google.protobuf.text_format as text_format
import google.protobuf.json_format as json_format
import importlib
import uuid

from pslx.core.exception import ProtobufNameNotExistException, ProtobufValueNotExistException,\
    ProtobufEnumTypeNotExistException, ProtobufMessageTypeNotExistException
from pslx.schema.rpc_pb2 import GenericRPCRequest, GenericRPCResponse
from pslx.util.timezone_util import TimezoneUtil


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
            preserving_proto_field_name=True,
            including_default_value_fields=True
        )

    @classmethod
    def message_to_string(cls, proto_message):
        return proto_message.SerializeToString()

    @classmethod
    def message_to_text(cls, proto_message):
        return text_format.MessageToString(
            message=proto_message,
            print_unknown_fields=True
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
    def string_to_message(cls, message_type, string):
        proto_message = message_type()
        proto_message.ParseFromString(string)
        return proto_message

    @classmethod
    def json_to_any(cls, json_str):
        proto_message = Any()
        return json_format.Parse(
            text=json_str,
            message=proto_message,
            ignore_unknown_fields=True
        )

    @classmethod
    def text_to_any(cls, text_str):
        proto_message = Any()
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
        if any_message.Is(message_type.DESCRIPTOR):
            proto_message = message_type()
            any_message.Unpack(proto_message)
            return proto_message
        else:
            return None

    @classmethod
    def infer_message_type_from_str(cls, message_type_str, modules=None):
        if not modules:
            modules = cls.MESSAGE_MODULE
        elif not isinstance(modules, list):
            modules = [modules]
        for module_name in modules:
            try:
                module = importlib.import_module(module_name)
                message_type = getattr(module, message_type_str)
                return message_type
            except AttributeError as _:
                pass
        raise ProtobufMessageTypeNotExistException

    @classmethod
    def infer_str_from_message_type(cls, message_type):
        return message_type.__name__

    @classmethod
    def compose_generic_response(cls, response):
        generic_response = GenericRPCResponse()
        if response:
            generic_response.response_data.CopyFrom(cls.message_to_any(response))
        generic_response.timestamp = str(TimezoneUtil.cur_time_in_pst())

        return generic_response

    @classmethod
    def compose_generic_request(cls, request):
        generic_request = GenericRPCRequest()
        generic_request.request_data.CopyFrom(cls.message_to_any(message=request))
        generic_request.timestamp = str(TimezoneUtil.cur_time_in_pst())
        generic_request.uuid = str(uuid.uuid4())
        return generic_request
