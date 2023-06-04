import os
import importlib
import pkgutil
from pslx.core.exception import EnvNotExistException


class EnvUtil(object):

    PSLX_ENV_TO_DEFAULT_MAP = {
        'PSLX_INTERNAL_CACHE': 100,
        'PSLX_TEST': False,
        'PSLX_ENABLE_SYS_LOG': False,
        'PSLX_SNAPSHOT_DIR': "/LOCAL/PSLX_INTERNAL/ttl=7d/snapshots/",
        'PSLX_DEFAULT_LOG_DIR': '/LOCAL/ttl=7d/logs/',
        'PSLX_INTERNAL_METADATA_DIR': '/LOCAL/PSLX_INTERNAL/ttl=-1d/metadata/',
        'PSLX_GRPC_MAX_MESSAGE_LENGTH': 512 * 1024 * 1024,  # 512MB,
        'PSLX_GRPC_TIMEOUT': 1,  # 1 second
        'PSLX_QUEUE_TIMEOUT': 10,  # 10 seconds
        "PSLX_RPC_FLUSH_RATE": 1,
        'PSLX_RPC_PASSWORD': 'admin',
        'PSLX_BACKEND_CONTAINER_TTL': 7,
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
    def get_other_env_variable(cls, var, fallback_value=None):
        try:
            return os.getenv(var, fallback_value)
        except Exception as _:
            raise EnvNotExistException

    @classmethod
    def get_pslx_env_and_default_value(cls):
        return cls.PSLX_ENV_TO_DEFAULT_MAP

    @classmethod
    def get_home_path(cls):
        return os.path.expanduser('~')

    @classmethod
    def get_all_schemas(cls, schema_lookup_paths):
        modules = []
        for path in schema_lookup_paths:
            module = importlib.import_module(path)
            for sub_module in pkgutil.iter_modules(module.__path__):
                modules.append(path + '.' + sub_module.name)

        schemas = []
        for module_name in modules:
            module = importlib.import_module(module_name)
            for name in dir(module):
                cls = getattr(module, name)
                try:
                    if cls.__class__.__name__ == 'GeneratedProtocolMessageType':
                        schemas.append(module_name + '.' + name)
                except Exception as _:
                    pass
        return schemas
