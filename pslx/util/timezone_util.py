import datetime
from pslx.config.general_config import TimeZoneObj


class TimezoneUtil(object):

    @classmethod
    def utc_to_pst(cls, utc_time):
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=TimeZoneObj.UTC_TIMEZONE)
        return utc_time.astimezone(TimeZoneObj.WESTERN_TIMEZONE)

    @classmethod
    def utc_to_est(cls, utc_time):
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=TimeZoneObj.UTC_TIMEZONE)
        return utc_time.astimezone(TimeZoneObj.EASTERN_TIMEZONE)

    @classmethod
    def pst_to_est(cls, western_time):
        if western_time.tzinfo is None:
            western_time = western_time.replace(tzinfo=TimeZoneObj.WESTERN_TIMEZONE)
        return western_time.astimezone(TimeZoneObj.EASTERN_TIMEZONE)

    @classmethod
    def est_to_pst(cls, eastern_time):
        if eastern_time.tzinfo is None:
            eastern_time = eastern_time.replace(tzinfo=TimeZoneObj.EASTERN_TIMEZONE)
        return eastern_time.astimezone(TimeZoneObj.WESTERN_TIMEZONE)

    @classmethod
    def cur_time_in_utc(cls):
        return datetime.datetime.now(tz=TimeZoneObj.UTC_TIMEZONE)

    @classmethod
    def cur_time_in_pst(cls):
        return datetime.datetime.now(tz=TimeZoneObj.WESTERN_TIMEZONE)

    @classmethod
    def cur_time_in_est(cls):
        return datetime.datetime.now(tz=TimeZoneObj.WESTERN_TIMEZONE)
