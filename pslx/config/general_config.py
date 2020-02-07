import pytz


class TimeZoneObj(object):
    EASTERN_TIMEZONE = pytz.timezone('US/Eastern')
    WESTERN_TIMEZONE = pytz.timezone('US/Pacific')
    UTC_TIMEZONE = pytz.timezone('UTC')


class SearchDirObj(object):
    TOOL_DIR = 'tool/'
    UTIL_DIR = 'util/'
    CONFIG_DIR = 'config/'
    SERVICE_DIR = 'service/'
    SCHEMA_DIR = 'schema/'
