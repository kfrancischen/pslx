import os
from google.protobuf.json_format import MessageToJson, Parse
from pslx.core.exception import ProtobufNameNotExistException, ProtobufValueNotExistException


def check_valid_enum(enum_type, value):
    return value in enum_type.values()


def get_name_by_value(enum_type, value):
    try:
        return enum_type.Name(number=value)
    except ProtobufValueNotExistException as _:
        return None


def get_value_by_name(enum_type, name):
    try:
        return enum_type.Value(name=name)
    except ProtobufNameNotExistException as _:
        return None


def write_proto_to_file(proto, file_name):
    dir_path = '/'.join(file_name.split('/')[:-1])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(file_name, 'wb') as outfile:
        outfile.write(proto.SerializeToString())


def read_proto_from_file(proto_type, file_name):
    proto = proto_type()
    assert os.path.exists(file_name)
    with open(file_name, 'rb') as infile:
        proto.ParseFromString(infile.read())
    return proto


def proto_message_to_json_str(proto_message):
    return MessageToJson(
        message=proto_message,
        preserving_proto_field_name=True
    )


def json_str_to_proto_message(proto_type, json_str):
    proto_message = proto_type()
    return Parse(
        text=json_str,
        message=proto_message,
        ignore_unknown_fields=True
    )
