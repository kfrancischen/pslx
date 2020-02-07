from pslx.tool.logging_tool import LoggingTool


class DummyLoggingTool(LoggingTool):
    def __init__(self, name=None, date=None, root_dir=None, level=None,retention=0):
        super().__init__(name=None, date=date, root_dir=root_dir, level=level, retention=retention)

    def write_log(self, string):
        return
