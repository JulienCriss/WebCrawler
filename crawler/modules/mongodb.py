# coding=utf-8
"""
    MongoDB Module
"""
import traceback

from pymongo import MongoClient


class MongoDB(object):
    """
        MongoDB handler
    """

    def __init__(self, host=None, port=None, user=None, password=None):
        """
        Constructor
        :param host: The host name of  database
        :param port: Port number where to listen
        :param user: User name
        :param password: Password
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password

        self._connection = None
        self._database = None
        self._collection = None

    @property
    def connection(self):
        """
        Get connection instance
        :return: 
        """
        return self._connection

    @property
    def database(self):
        """
        Get database instance
        :return: 
        """
        return self._database

    @property
    def collection(self):
        """
        Get collection instance
        :return: 
        """
        return self._collection

    def connect(self, host=None, port=None, database=None, collection=None):
        """
        Connect to MongoDB database
        :param host: Host
        :param port: Port
        :param database: Database name
        :param collection: Collection/View name
        :type host: str
        :type port: int
        :type database: str
        :type collection: str
        :return: 
        """

        if host and port:
            self._connection = MongoClient("mongodb://{}:{}/".format(host, port))
        else:
            self._connection = MongoClient("mongodb://{}:{}/".format(self._host, self._port))

        self._database = self._connection[database]
        self._collection = self._database[collection]

    def insert_document(self, document_entry, collection_name=None):
        """
        Insert a document in collection
        :param document_entry: 
        :param collection_name:
        :type document_entry: dict
        :return: The id of the new entry
        """
        try:
            if self.connection:

                if collection_name:
                    _id = self._database[collection_name].insert(document_entry, check_keys=False)
                    return _id

                else:
                    _id = self.collection.insert(document_entry, check_keys=False)
                    return _id
        except Exception as exc:
            print traceback.print_exc(exc)

    def select(self, query=None, collection_name=None):
        """
        Select documents from collection
        :param query: 
        :param collection_name:
        :type query: dict
        :type collection_name: str
        :return: 
        """
        results = []
        if self.connection:

            if query:

                if collection_name:

                    for result in self._database[collection_name].find(query, {"_id": 0}):
                        results.append(result)

                    return results
                else:

                    for result in self.collection.find(query, {"_id": 0}):
                        results.append(result)

                    return results
            else:

                for result in self.collection.find({}, {"_id": 0}):
                    results.append(result)

                return results
        else:
            return None

    def clear(self, query=None, collection_name=None):
        """
        Remove items from database
        :param query: 
        :param collection_name: 
        :return: 
        """
        if self.connection:
            if query:

                if collection_name:
                    try:
                        self._database[collection_name].delete_many(query)
                    except Exception as exc:
                        print traceback.format_exc(exc)
                else:
                    try:
                        self.collection.delete_many(query)
                    except Exception as exc:
                        print traceback.format_exc(exc)
            else:
                try:
                    self.collection.delete_many({})
                except Exception as exc:
                    print traceback.format_exc(exc)

    def disconnect(self):
        """
            Terminate MongoDB session
        """
        if self._connection:
            self._connection.close()
