import mysql.connector
from pslx.core.base import Base
from pslx.core.exception import SQLConnectionException, SQLNotInitializedException, SQLExecutionException
from pslx.tool.filelock_tool import FileLockTool
from pslx.util.file_util import FileUtil


class SQLTool(Base):
    def __init__(self):
        super().__init__()
        self._connector = None

    def connect_to_database(self, credential, database):
        try:
            self._connector = mysql.connector.connect(
                host=credential.others['sql_host_ip'],
                port=credential.others['sql_host_ip'],
                user=credential.user_name,
                password=credential.password,
                database=database
            )
            self.sys_log("Connection successful.")
        except mysql.connector.Error as err:
            self.sys_log("Connection exception: " + str(err))
            raise SQLConnectionException

    @staticmethod
    def _query_file_to_str(query_file):
        query_str = ''
        with FileLockTool(protected_file_path=query_file, read_mode=True):
            with open(query_file, 'r') as infile:
                for line in infile:
                    query_str += line
        return query_str

    def execute_query_str(self, query_str, modification):
        if not self._connector:
            raise SQLNotInitializedException

        try:
            cursor = self._connector.cursor()
            cursor.execute(query_str)
            if modification:
                self._connector.commit()
            result = []
            for item in cursor:
                result.append(item)

            cursor.close()
            self._connector.close()

        except mysql.connector.Error as err:
            self.sys_log("Query exception: " + str(err))
            if modification:
                self._connector.rollback()
            raise SQLExecutionException

        return result

    def execute_query_file(self, query_file, modification):
        if not FileUtil.does_file_exist(file_name=query_file):
            self.sys_log("Query file " + query_file + " does not exist")

        query_str = self._query_file_to_str(query_file=query_file)

        return self.execute_query_str(
            query_str=query_str,
            modification=modification
        )
