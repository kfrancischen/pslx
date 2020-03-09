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
    def utc_to_pst(cls, utc_time):
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=TimezoneObj.UTC_TIMEZONE)
        return utc_time.astimezone(TimezoneObj.WESTERN_TIMEZONE)

    @classmethod
    def utc_to_est(cls, utc_time):
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=TimezoneObj.UTC_TIMEZONE)
        return utc_time.astimezone(TimezoneObj.EASTERN_TIMEZONE)

    @classmethod
    def pst_to_est(cls, western_time):
        if western_time.tzinfo is None:
            western_time = western_time.replace(tzinfo=TimezoneObj.WESTERN_TIMEZONE)
        return western_time.astimezone(TimezoneObj.EASTERN_TIMEZONE)

    @classmethod
    def est_to_pst(cls, eastern_time):
        if eastern_time.tzinfo is None:
            eastern_time = eastern_time.replace(tzinfo=TimezoneObj.EASTERN_TIMEZONE)
        return eastern_time.astimezone(TimezoneObj.WESTERN_TIMEZONE)

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
        return datetime.datetime.now(tz=TimezoneObj.WESTERN_TIMEZONE)

    @classmethod
    def cur_time_from_str(cls, time_str):
        string_formats = [
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S"
        ]
        for str_format in string_formats:
            try:
                return datetime.datetime.strptime(time_str, str_format)
            except Exception as _:
                pass
        return None
