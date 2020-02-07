import datetime
from pslx.config.general_config import TimeZoneObj


def utc_to_pst(utc_time):
    if utc_time.tzinfo is None:
        utc_time = utc_time.replace(tzinfo=TimeZoneObj.UTC_TIMEZONE)
    return utc_time.astimezone(TimeZoneObj.WESTERN_TIMEZONE)


def utc_to_est(utc_time):
    if utc_time.tzinfo is None:
        utc_time = utc_time.replace(tzinfo=TimeZoneObj.UTC_TIMEZONE)
    return utc_time.astimezone(TimeZoneObj.EASTERN_TIMEZONE)


def pst_to_est(western_time):
    if western_time.tzinfo is None:
        western_time = western_time.replace(tzinfo=TimeZoneObj.WESTERN_TIMEZONE)
    return western_time.astimezone(TimeZoneObj.EASTERN_TIMEZONE)


def est_to_pst(eastern_time):
    if eastern_time.tzinfo is None:
        eastern_time = eastern_time.replace(tzinfo=TimeZoneObj.EASTERN_TIMEZONE)
    return eastern_time.astimezone(TimeZoneObj.WESTERN_TIMEZONE)


def cur_time_in_utc():
    return datetime.datetime.now(tz=TimeZoneObj.UTC_TIMEZONE)


def cur_time_in_pst():
    return datetime.datetime.now(tz=TimeZoneObj.WESTERN_TIMEZONE)


def cur_time_in_est():
    return datetime.datetime.now(tz=TimeZoneObj.WESTERN_TIMEZONE)
