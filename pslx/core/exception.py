from pslx.core.base import Base


class ExceptionBase(Base, Exception):
    pass


class ProtobufException(ExceptionBase):
    pass


class ProtobufNameNotExistException(ProtobufException):
    pass


class ProtobufValueNotExistException(ProtobufException):
    pass


class OperatorFailureException(ExceptionBase):
    pass
