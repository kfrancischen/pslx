import mysql.connector
from galaxy_py import gclient_ext
from pslx.core.base import Base
from pslx.core.exception import SQLConnectionException, SQLNotInitializedException, SQLExecutionException
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
            self._SYS_LOGGER.info("Connection successful.")
        except mysql.connector.Error as err:
            self._SYS_LOGGER.info("Connection exception: " + str(err) + '.')
            raise SQLConnectionException("Connection exception: " + str(err) + '.')

    @staticmethod
    def _query_file_to_str(query_file):
        query_str = gclient_ext.read_txt(path=query_file)
        return query_str

    def execute_query_str(self, query_str, modification):
        if not self._connector:
            raise SQLNotInitializedException("SQl client not initialized.")

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
            self._SYS_LOGGER.info("Query exception: " + str(err) + '.')
            if modification:
                self._connector.rollback()
            raise SQLExecutionException("Query exception: " + str(err) + '.')

        return result

    def execute_query_file(self, query_file, modification):
        if not FileUtil.does_file_exist(file_name=query_file):
            self._SYS_LOGGER.info("Query file " + query_file + " does not exist")

        query_str = self._query_file_to_str(query_file=query_file)

        return self.execute_query_str(
            query_str=query_str,
            modification=modification
        )
