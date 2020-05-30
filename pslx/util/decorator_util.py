from pslx.util.timezone_util import TimezoneUtil


class DecoratorUtil(object):

    @classmethod
    def pst_runtime(cls, weekdays, hours=[i for i in range(24)], minutes=[i for i in range(60)]):
        def decorator(func):
            def wrapper(*args, **kwargs):
                assert isinstance(weekdays, list) and isinstance(hours, list) and isinstance(minutes, list)
                cur_time = TimezoneUtil.cur_time_in_pst()
                if cur_time.weekday() not in weekdays or cur_time.hour not in hours or cur_time.minute not in minutes:
                    return None
                else:
                    return func(*args, **kwargs)
            return wrapper

        return decorator


@DecoratorUtil.pst_runtime(weekdays=[5])
def test_func(test_string):
    return test_string


if __name__ == "__main__":
    print(test_func("test here"))
