# coding=utf-8
"""
    Utils functions
"""

from mongodb import MongoDB


def clear_db_tables(host, port, database, collections):
    """
    Clear some tables firstly : TF_DICT, etc
    :param host: 
    :param port: 
    :param database: 
    :param collections: A list of collection that you want to delete
    :return: 
    """
    mongo_session = MongoDB()

    for c in collections:
        mongo_session.connect(host=host, port=port, database=database, collection=c)
        mongo_session.clear({}, c)
        mongo_session.disconnect()
