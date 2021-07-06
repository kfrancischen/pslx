from functools import wraps
import multiprocessing
import signal
import sys
import time
from pslx.core.exception import TimeoutException


class ThreadedTimeout(object):
    def __init__(self, function, exception_message, time_out):
        self._limit = time_out
        self._function = function
        self._exception_message = exception_message
        self._timeout = time.time()
        self._process = multiprocessing.Process()
        self._queue = multiprocessing.Queue()

    def __call__(self, *args, **kwargs):
        self._limit = kwargs.pop('timeout', self._limit)
        self._queue = multiprocessing.Queue(1)
        args = (self._queue, self._function) + args
        self._process = multiprocessing.Process(
            target=self._target,
            args=args,
            kwargs=kwargs
        )
        self._process.daemon = True
        self._process.start()
        self._timeout = self._limit + time.time()
        while not self.ready:
            time.sleep(0.01)
        return self.value

    def cancel(self):
        if self._process.is_alive():
            self._process.terminate()
        raise TimeoutException(self._exception_message)

    @staticmethod
    def _target(queue, function, *args, **kwargs):
        try:
            queue.put((True, function(*args, **kwargs)))
        except Exception as _:
            queue.put((False, sys.exc_info()[1]))

    @property
    def ready(self):
        if self._timeout < time.time():
            self.cancel()
        return self._queue.full() and not self._queue.empty()

    @property
    def value(self):
        if self.ready is True:
            flag, load = self._queue.get()
            if flag:
                return load
            raise load


class RaterLimiter(object):
    def __init__(self, interval=0):
        self._rate_limit = interval if interval > 0 else 0
        self._tic_of_last_call = None

    def __call__(self, function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            if self._tic_of_last_call and time.time() - self._tic_of_last_call < self._rate_limit:
                time.sleep(abs(self._rate_limit - (time.time() - self._tic_of_last_call)))

            self._tic_of_last_call = time.time()
            return function(*args, **kwargs)
        return wrapper


class DecoratorUtil(object):

    @classmethod
    def run_on_condition(cls, condition_func, default_return=None, *condition_args, **condition_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if condition_func(*condition_args, **condition_kwargs):
                    return func(*args, **kwargs)
                else:
                    return default_return
            return wrapper

        return decorator

    @classmethod
    def default_timeout(cls, time_out=None, exception_message='Timeout occurs.'):
        def decorate(function):
            if not time_out or time_out <= 0:
                return function

            def handler(signum, frame):
                raise TimeoutException(exception_message)

            @wraps(function)
            def new_function(*args, **kwargs):
                new_seconds = kwargs.pop('timeout', time_out)
                if new_seconds:
                    old = signal.signal(signal.SIGALRM, handler)
                    signal.setitimer(signal.ITIMER_REAL, new_seconds)
                try:
                    return function(*args, **kwargs)
                finally:
                    if new_seconds:
                        signal.setitimer(signal.ITIMER_REAL, 0)
                        signal.signal(signal.SIGALRM, old)
            return new_function

        return decorate

    @classmethod
    def thread_safe_timeout(cls, time_out=None, exception_message='Timeout occurs.'):
        def decorate(function):
            if not time_out or time_out <= 0:
                return function

            @wraps(function)
            def new_function(*args, **kwargs):
                timeout_wrapper = ThreadedTimeout(
                    function, exception_message, time_out)
                return timeout_wrapper(*args, **kwargs)
            return new_function

        return decorate

    @classmethod
    def rate_limiter(cls, interval=0):
        return RaterLimiter(interval=interval)

    @classmethod
    def retry(cls, retry_on_exception=Exception, num_retry=-1, delay=0, max_delay=None, backoff=1):
        def decorator(func):
            def wrapper(*args, **kwargs):
                retry_val, delay_val = num_retry, delay
                while retry_val:
                    try:
                        return func(*args, **kwargs)
                    except retry_on_exception as _:
                        retry_val -= 1
                        if not retry_val:
                            raise

                        if delay_val > 0:
                            time.sleep(delay_val)
                        delay_val *= backoff

                        if max_delay:
                            delay_val = min(delay_val, max_delay)
            return wrapper

        return decorator

    @classmethod
    def exception_safe_return(cls, default_return=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as _:
                    return default_return
            return wrapper

        return decorator

    @classmethod
    def experimental(cls, func):
        def decorator(*args, **kwargs):
            print("THIS IS EXPERIMENTAL CODE. PLEASE USE WITH CAUTION!!!")
            func(*args, **kwargs)
        return decorator
