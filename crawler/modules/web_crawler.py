# coding=utf-8
"""
    Web Crawler
"""
import json
import os
import traceback
from collections import OrderedDict
from datetime import datetime

import math

from .document_parser import DocumentParse
from mongodb import MongoDB


class WebCrawler(DocumentParse):
    """
        Web Crawler Class
    """

    def __init__(self, path_to_stop_words, path_to_exception_words, dir_location, extension, direct_index_dir, reverse_index_dir):
        """
        Constructor
        :param path_to_stop_words: Path to stop words document
        :param path_to_exception_words: Path to exception words document
        :param dir_location: Root location of site
        """
        super(self.__class__, self).__init__(path_to_stop_words, path_to_exception_words, extension, dir_location, direct_index_dir,
                                             reverse_index_dir)

        self._map_index_files = OrderedDict()
        self._reverse_index = {}
        self._interrogation = None
        self._list_of_words = None

        self._create_direct_index()
        self._create_reverse_index()
        self._compute_idf()

    def _create_direct_index(self):
        """
        Create direct index and save the hash map for each file processed
        :return:
        """
        try:
            map_response = self.map_phase()
            for item in map_response:
                self._map_index_files[item[0]] = item[1]

        except Exception as exc:
            print traceback.print_exc(exc)

    def _create_reverse_index(self):
        """
        Create reverse index
        :return:
        """
        map_response = []

        try:
            for _idx, _file in self._map_index_files.items():
                result = self.reduce_phase(_idx, _file)
                map_response.append(result)

            for item in map_response:
                for key in item:
                    temp_file_name = key.replace('@', '.')
                    dictionary = item[key]

                    for word, value in dictionary.iteritems():
                        if word in self._reverse_index:
                            self._reverse_index[word].append((temp_file_name, int(value)))
                        else:
                            self._reverse_index[word] = [(temp_file_name, int(value))]

            reverse_index_file = r"{}\{}_reverse_index.idx".format(self._reverse_index_dir, datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
            os.environ["LAST_REVERSE_INDEX_FILE"] = reverse_index_file

            with open(reverse_index_file, 'w') as file_handler:
                file_handler.write(json.dumps(self._reverse_index, sort_keys=True))

        except Exception as exc:
            traceback.print_exc(exc)

    def _compute_idf(self):
        """
        Compute idf
        :return: 
        """
        temp_dict = {}

        self._mongo_session = MongoDB()
        self._mongo_session.connect(host="localhost", port=27017, database="crawler", collection="tf_dict")
        db_tf_results = self._mongo_session.select({})

        for result in db_tf_results:

            for _file, words_dict in result.items():

                if _file not in temp_dict:
                    temp_dict[_file] = {}

                doc_norm = 0

                for word, tf in words_dict.items():

                    if word in self._reverse_index:
                        idf = math.log(self._number_of_docs + 0.1 / float(len(self._reverse_index[word])), 10)
                        idf = float("{0:.6f}".format(idf))

                        doc_norm += math.pow(tf * idf, 2)

                        temp_dict[_file][word] = {"tf": tf, "idf": idf, "doc": float("{0:.6f}".format(tf * idf))}

                temp_dict[_file]['|doc|'] = float("{0:.6f}".format(math.sqrt(doc_norm)))

        self._mongo_session.connect(host="localhost", port=27017, database="crawler", collection="tf_idf_dict")
        self._mongo_session.insert_document(temp_dict, "tf_idf_dict")
        self._mongo_session.disconnect()

    @staticmethod
    def _compute_cosinus(doc_norm, query_norm, d, q):
        return (d * q) / float(doc_norm * query_norm)

    @classmethod
    def make_interrogation(cls, interrogation, reverse_index_file):
        """
        Make an interrogation and search for files which contains the words from interrogation
        :param reverse_index_file: The file name who contains the reverse index dictionary 
        :param interrogation: A list with words from interrogation
        :type interrogation: list
        :type reverse_index_file: str
        :return:
        """
        reverse_index = {}
        query = {}
        results = []
        item_used = []
        doc_norm = 0
        doc_results = {}

        mongo_session = MongoDB()
        mongo_session.connect(host="localhost", port=27017, database="crawler", collection="tf_idf_dict")
        db_tf_idf_results = mongo_session.select({})

        with open(reverse_index_file) as file_handler:
            for line in file_handler:
                reverse_index = json.loads(line)

        for query_word in interrogation:
            tf = 1 / float(len(interrogation))
            tf = float("{0:.6f}".format(tf))
            idf = 1
            doc_norm += math.pow(tf * idf, 2)
            query[query_word] = {"tf": tf, "idf": idf, 'query': float("{0:.6f}".format(tf * idf))}

        query['|query|'] = float("{0:.6f}".format(math.sqrt(doc_norm)))

        for word, list_of_files in reverse_index.items():
            if word in interrogation:

                for _file_item in list_of_files:
                    _file = _file_item[0]

                    if _file in db_tf_idf_results[0]:
                        doc_norm = db_tf_idf_results[0][_file]['|doc|']
                        query_norm = query['|query|']
                        d = db_tf_idf_results[0][_file][word]['doc']
                        q = query[word]['query']
                        cosinus = cls._compute_cosinus(doc_norm, query_norm, d, q)
                        doc_results[cosinus] = _file

        doc_results = OrderedDict(sorted(doc_results.items()))
        for cosinus, _file in doc_results.items():

            if _file not in item_used:
                item_used.append(_file)
                temp_dict = {'id': _file, 'label': _file, 'value': _file}
                results.append(temp_dict)

        if len(results) == 0:
            temp_dict = {'id': 1, 'label': 'no results', 'value': 'no results'}
            results.append(temp_dict)

        return results
