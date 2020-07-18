import datetime
import pytz


class TimezoneObj(object):
    EASTERN_TIMEZONE = pytz.timezone('US/Eastern')
    WESTERN_TIMEZONE = pytz.timezone('US/Pacific')
    UTC_TIMEZONE = pytz.timezone('UTC')


class TimeSleepObj(object):
    ONE_THOUSANDTH_SECOND = 0.001
    ONE_HUNDREDTH_SECOND = 0.01
    ONE_TENTH_SECOND = 0.1
    HALF_SECOND = 0.5
    ONE_SECOND = 1
    FIVE_SECONDS = 5
    TEN_SECONDS = 10
    HALF_MINUTE = 30
    ONE_MINUTE = 60
    HALF_HOUR = 30 * 60
    ONE_HOUR = 60 * 60
    HALF_DAY = 60 * 60 * 12
    ONE_DAY = 60 * 60 * 24


class TimezoneUtil(object):

    @classmethod
    def naive_to_utc(cls, naive_time):
        return TimezoneObj.UTC_TIMEZONE.localize(naive_time)

    @classmethod
    def naive_to_pst(cls, naive_time):
        return TimezoneObj.WESTERN_TIMEZONE.localize(naive_time)

    @classmethod
    def naive_to_est(cls, naive_time):
        return TimezoneObj.EASTERN_TIMEZONE.localize(naive_time)

    @classmethod
    def utc_to_pst(cls, utc_time):
        if utc_time.tzinfo is None:
            utc_time = cls.naive_to_utc(naive_time=utc_time)
        return utc_time.astimezone(TimezoneObj.WESTERN_TIMEZONE)

    @classmethod
    def utc_to_est(cls, utc_time):
        if utc_time.tzinfo is None:
            utc_time = cls.naive_to_utc(naive_time=utc_time)
        return utc_time.astimezone(TimezoneObj.EASTERN_TIMEZONE)

    @classmethod
    def pst_to_est(cls, western_time):
        if western_time.tzinfo is None:
            western_time = cls.naive_to_pst(naive_time=western_time)
        return western_time.astimezone(TimezoneObj.EASTERN_TIMEZONE)

    @classmethod
    def pst_to_utc(cls, western_time):
        if western_time.tzinfo is None:
            western_time = cls.naive_to_pst(naive_time=western_time)
        return western_time.astimezone(TimezoneObj.UTC_TIMEZONE)

    @classmethod
    def est_to_pst(cls, eastern_time):
        if eastern_time.tzinfo is None:
            eastern_time = cls.naive_to_est(naive_time=eastern_time)
        return eastern_time.astimezone(TimezoneObj.WESTERN_TIMEZONE)

    @classmethod
    def est_to_utc(cls, eastern_time):
        if eastern_time.tzinfo is None:
            eastern_time = cls.naive_to_est(naive_time=eastern_time)
        return eastern_time.astimezone(TimezoneObj.UTC_TIMEZONE)

    @classmethod
    def cur_time_in_local(cls):
        return datetime.datetime.now()

    @classmethod
    def cur_time_in_utc(cls):
        return datetime.datetime.now(tz=TimezoneObj.UTC_TIMEZONE)

    @classmethod
    def cur_time_in_pst(cls):
        return datetime.datetime.now(tz=TimezoneObj.WESTERN_TIMEZONE)

    @classmethod
    def cur_time_in_est(cls):
        return datetime.datetime.now(tz=TimezoneObj.EASTERN_TIMEZONE)

    @classmethod
    def cur_time_from_str(cls, time_str):
        string_formats = [
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H",
            "%Y-%m-%d",
        ]
        for str_format in string_formats:
            try:
                return datetime.datetime.strptime(time_str, str_format)
            except Exception as _:
                pass
        return None
