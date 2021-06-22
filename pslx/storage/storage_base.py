from pslx.core.base import Base
from pslx.util.dummy_util import DummyUtil


class StorageBase(Base):
    STORAGE_TYPE = None

    def __init__(self, logger=None):
        super().__init__()
        if not logger:
            self._logger = DummyUtil.dummy_logger()
        else:
            self._logger = logger

    def get_storage_type(self):
        return self.STORAGE_TYPE

    def initialize_from_file(self, file_name):
        raise NotImplementedError

    def initialize_from_dir(self, dir_name):
        raise NotImplementedError

    def read(self, params=None):
        raise NotImplementedError

    def write(self, data, params=None):
        raise NotImplementedError
