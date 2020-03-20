import os
from pslx.core.exception import EnvNotExistException


class EnvUtil(object):

    PSLX_ENV_TO_DEFAULT_MAP = {
        'PSLX_INTERNAL_TTL': 7,
        'PSLX_INTERNAL_CACHE': 100,
        'PSLX_TEST': False,
        'PSLX_LOG': False,
        'PSLX_DATABASE': 'database/',
        'PSLX_GRPC_MAX_MESSAGE_LENGTH': 512 * 1024 * 1024,  # 512MB,
        'PSLX_BACKEND_STORAGE': 'backend/',
        'PSLX_FRONTEND_CONFIG_PROTO_PATH': '',
    }

    @classmethod
    def get_pslx_env_variable(cls, var, fallback_value=None):
        if var not in cls.PSLX_ENV_TO_DEFAULT_MAP:
            raise EnvNotExistException
        else:
            if not fallback_value:
                return os.getenv(var, cls.PSLX_ENV_TO_DEFAULT_MAP[var])
            else:
                return os.getenv(var, fallback_value)

    @classmethod
    def get_pslx_env_and_default_value(cls):
        return cls.PSLX_ENV_TO_DEFAULT_MAP
