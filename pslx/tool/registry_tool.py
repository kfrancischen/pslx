from pslx.core.base import Base
from pslx.core.exception import RegisteredItemAlreadyExistException


class RegistryTool(Base):

    def __init__(self):
        super().__init__()
        self._registered_items = {}

    def register(self, name):
        def decorator(func):
            if name in self._registered_items:
                raise RegisteredItemAlreadyExistException
            self._registered_items[name] = func
            return func

        return decorator

    def get_registered_items(self):
        return self._registered_items
