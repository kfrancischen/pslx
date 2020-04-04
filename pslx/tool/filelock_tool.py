import os
import sys
import time
import errno

from pslx.core.base import Base
from pslx.core.exception import FileLockToolException
from pslx.util.timezone_util import TimeSleepObj
from pslx.util.file_util import FileUtil


class FileLockTool(Base):

    def __init__(self, protected_file_path, timeout=None, delay=TimeSleepObj.ONE_THOUSANDTH_SECOND, read_mode=True,
                 lock_file_contents=None):
        super().__init__()
        self._is_locked = False
        self._lockfile = protected_file_path + ".lock"
        self._timeout = timeout
        self._delay = delay
        if read_mode:
            FileUtil.die_if_file_not_exist(file_name=protected_file_path)
        else:
            FileUtil.create_file_if_not_exist(file_name=protected_file_path)

        self._lock_file_contents = lock_file_contents
        if self._lock_file_contents is None:
            self._lock_file_contents = "Owning process args:\n"
            for arg in sys.argv:
                self._lock_file_contents += arg + "\n"
        self.sys_log("Lock file contents: " + self._lock_file_contents)

    def is_locked(self):
        return self._is_locked

    def is_available(self):
        return not FileUtil.does_file_exist(file_name=self._lockfile)

    def _acquire(self, blocking=True):
        start_time = time.time()
        while True:
            try:
                file_handler = os.open(self._lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                with os.fdopen(file_handler, "a") as file:
                    file.write(self._lock_file_contents)
                break
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise
                if self._timeout is not None and time.time() - start_time >= self._timeout:
                    raise FileLockToolException
                if not blocking:
                    return False
                time.sleep(self._delay)
        self._is_locked = True
        return True

    def _release(self):
        self.sys_log("File released by removing " + self._lockfile + '.')
        self._is_locked = False
        FileUtil.remove_file(self._lockfile)

    def __enter__(self):
        self._acquire()
        return self

    def __exit__(self, type, value, traceback):
        self._release()

    def __del__(self):
        if self._is_locked:
            self._release()
