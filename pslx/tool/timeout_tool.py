import signal

from pslx.core.base import Base
from pslx.core.exception import TimeoutException


class TimeoutTool(Base):
    def __init__(self, timeout=-1):
        super().__init__()
        self.sys_log("Warning: This can only be used in the main thread.")
        self._timeout = timeout

    def handle_timeout(self, signum, frame):
        raise TimeoutException

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self._timeout)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)
