from pymongo import MongoClient
from pslx.core.base import Base
from pslx.core.exception import MongodbConnectionException, MongodbExecutionException, MongodbNotInitializedException


class MongodbTool(Base):
    def __init__(self):
        super().__init__()
        self._client = None

    def connect_to_database(self, credential):
        connection_str = "mongodb://{}:{}@{}:{}".format(
            credential.user_name, credential.password, credential.others['mongodb_host_ip'],
            credential.others['mongodb_port'])
        try:
            self._client = MongoClient(connection_str)
        except Exception as err:
            self.sys_log("Connection exeception: " + str(err) + '.')
            raise MongodbConnectionException("Connection exeception: " + str(err) + '.')

    def list_databases(self):
        if not self._client:
            raise MongodbNotInitializedException("Mongodb client no initialized.")
        return self._client.list_database_names()

    def list_collections(self, database_name):
        try:
            db = self._client[database_name]
            return db.list_collection_names()
        except Exception as err:
            raise MongodbExecutionException("Failed to list collection names with error " + str(err) + '.')

    def get_database(self, database_name):
        return self._client[database_name]

    def get_collection(self, database_name, collection_name):
        db = self.get_database(database_name=database_name)
        return db[collection_name]
