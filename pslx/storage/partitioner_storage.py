from pslx.storage.partitioner_base import PartitionerBase
from pslx.schema.enums_pb2 import PartitionerStorageType


class YearlyPartitionerStorage(PartitionerBase):
    PARTITIONER_TYPE = PartitionerStorageType.YEARLY


class MonthlyPartitionerStorage(PartitionerBase):
    PARTITIONER_TYPE = PartitionerStorageType.MONTHLY


class DailyPartitionerStorage(PartitionerBase):
    PARTITIONER_TYPE = PartitionerStorageType.DAILY


class HourlyPartitionerStorage(PartitionerBase):
    PARTITIONER_TYPE = PartitionerStorageType.HOURLY


class MinutelyPartitionerStorage(PartitionerBase):
    PARTITIONER_TYPE = PartitionerStorageType.MINUTELY
