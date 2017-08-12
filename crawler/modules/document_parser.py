# coding=utf-8
"""
    Module to parse a document
"""
import codecs
import copy_reg
import ntpath
import os
import uuid

import types
import urllib2
import multiprocessing
import traceback
import string
from nltk import PorterStemmer
from multiprocessing import Manager, Process
from bson.objectid import ObjectId

from BeautifulSoup import BeautifulSoup
from .mongodb import MongoDB


def _pickle_method(m):
    """
    Method to pickle class method for processes
    :param m:
    :return:
    """
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copy_reg.pickle(types.MethodType, _pickle_method)

PUNCTUATION_TRANS = string.maketrans(string.punctuation, ' ' * len(string.punctuation))


class DocumentParse(object):
    """
        Parse a document an create an hash map which will be saved on disk
        The resulting file will be the direct index for parsed document
        e.g. {name_of_file: {word1: value1, word2: value2 ...}}
    """

    def __init__(self, path_to_stop_words, path_to_exception_words, file_extension, working_directory, direct_index_dir, reverse_index_dir):
        """
        Constructor
        :param path_to_stop_words: Path to stop words file
        :param path_to_exception_words: Path to exception words file
        :param file_extension:  Parse only documents with the extension that user provides
        :param working_directory: Working directory where are all files to parse
        :type path_to_stop_words: str
        :type path_to_exception_words: str
        :type file_extension: str
        :type working_directory: str
        :type direct_index_dir: str
        :type reverse_index_dir: str
        """
        self._path_to_stop_words = path_to_stop_words
        self._path_to_exception_words = path_to_exception_words
        self._extension = file_extension
        self._working_directory = working_directory
        self._direct_index_dir = direct_index_dir
        self._reverse_index_dir = reverse_index_dir

        self._stop_words_list = []
        self._exception_words_list = []
        self._document_content = []
        self._list_of_files_to_parse = None
        self._hash_map = {}
        self._mongo_session = None

        self._pool_workers = multiprocessing.Pool(processes=8)

        self._load_stop_words()
        self._load_exception_words()
        self._read_directory_content()

        self._number_of_docs = len(self._list_of_files_to_parse)

    @property
    def hash_map(self):
        """
        Return the hash map dictionary
        :rtype: dict
        """
        return self._hash_map

    @staticmethod
    def _generate_mapper_file(filename, extension):
        """
        Method to generate a direct index file
        :param filename: Name of file that you want to change extension
        :param extension: The new extension e.g .txt or .idx etc
        :return:
        """

        if '.html' in filename:
            filename = '{}_{}'.format(uuid.uuid4().hex, filename)
            return filename.replace('.html', extension)

        elif '.htm' in filename:
            filename = '{}_{}'.format(uuid.uuid4().hex, filename)
            return filename.replace('.htm', extension)

        elif '.txt' in filename:
            filename = '{}_{}'.format(uuid.uuid4().hex, filename)
            return filename.replace('.txt', extension)
        else:
            filename = '{}_{}'.format(uuid.uuid4().hex, filename)
            return "{}{}".format(filename, extension)

    def _is_with_extension(self, filename):
        """
        Method to check if  a file has the extension that user specified
        :param filename: Name of file
        :return: Return true or false if the file is a file with extension .html or .htm
        :rtype: bool
        """
        return self._extension in filename

    def _read_directory_content(self):
        """
        Method who iterates a set of directories and search for files with a specific extension
        :return:
        """
        self._list_of_files_to_parse = []

        for subdir, dirs, files in os.walk(self._working_directory):
            for item_file in files:
                file_path = subdir + os.sep + item_file

                if self._is_with_extension(file_path):
                    self._list_of_files_to_parse.append(file_path)

    def _load_stop_words(self):
        """
        Method to load stop words in a list
        Later we will check if a word from parsed document is a stop word or not
        :return: 
        """
        with open(self._path_to_stop_words) as file_handler:
            for line in file_handler:
                self._stop_words_list.append(line.strip())

    def _load_exception_words(self):
        """
        Method to load exception words in a list
        Later we will check if a word from parsed document is a stop word or not
        :return: 
        """
        with open(self._path_to_exception_words) as file_handler:
            for line in file_handler:
                self._exception_words_list.append(line.strip())

    def _is_stop_word(self, word):
        """
        Method to check if a word is a stop word or not
        :param word: The word do you want to check
        :type word: str
        :return: True or False
        :rtype: bool
        """
        return word in self._stop_words_list

    def _is_exception_word(self, word):
        """
        Method to check if a word is an exception word or not
        :param word: The word do you want to check
        :type word: str
        :return: True or False
        :rtype: bool
        """
        return word in self._exception_words_list

    def _create_hash_file(self, filename):
        """
        Save hash map for parsed document on disk
        :param filename: Current file that is processed
        :type filename: str
        :return: The file name where is saved the hash
        :rtype: str
        """
        filename = ntpath.basename(filename)
        idx_file_name = self._generate_mapper_file(filename, '.idx')

        # save the files in direct index directory
        idx_file_name = '{}\{}'.format(self._direct_index_dir, idx_file_name)

        with open(idx_file_name, "w") as file_handler:
            for key, value in self._hash_map.iteritems():
                file_handler.write("{}:{}\n".format(key, value))

        return idx_file_name

    def _compute_tf(self, filename):
        """
        Compute tf number for each word and save the result in MongoDB database
        :param filename: Current file that is processed
        :type filename: str
        """
        total_terms = len(self._hash_map.keys())

        tf_dict = {filename: {}}

        for key, value in self._hash_map.iteritems():
            tf = value / float(total_terms)
            tf_dict[filename][key] = float("{0:.6f}".format(tf))

        # save tf_dict on database
        self._mongo_session = MongoDB()
        self._mongo_session.connect(host="localhost", port=27017, database="crawler", collection="tf_dict")
        self._mongo_session.insert_document(tf_dict)
        self._mongo_session.disconnect()

    def _save_hash_on_db(self, filename, hash_map):
        """
        Save the hash map on database
        :param filename: The file that is processed and generates the hash map
        :return: The key id from database after insert
        """
        self._mongo_session = MongoDB()
        self._mongo_session.connect(host="localhost", port=27017, database="crawler", collection="index_direct")
        temp_id = 0

        # mongodb does not support . in keys
        filename = filename.replace('.', '@')
        new_entry = {filename: hash_map}

        try:
            temp_id = self._mongo_session.insert_document(document_entry=new_entry)
        except Exception as exc:
            print traceback.format_exc(exc)

        self._mongo_session.disconnect()

        return ObjectId(temp_id)

    def generate_hash_map(self):
        """
        Method who creates a hash map with all word from current file parsed
        All words are firstly parsed to Porter Algorithm and then inserted to hash map
        :return:
        """

        # clear the hash map
        self._hash_map.clear()

        for line in self._document_content:

            line = line.encode('utf-8')

            line = str(line).translate(PUNCTUATION_TRANS)
            words = line.split()

            for word in words:

                word = word.decode('utf-8-sig')
                word = PorterStemmer().stem(word)
                word = word.lower()

                if word.isalpha():
                    if not self._is_stop_word(word):

                        # if the word is not in hash
                        if word not in self._hash_map:
                            self._hash_map[word] = 1
                        else:
                            self._hash_map[word] += 1

    def parse_document(self, file_name, map_result, protocol="file:///"):
        """
        Method to parse a document and extract information such as title, meta content, anchors hrefs, body text, etc. if is a HTML file
        or extract words if is other file
        :param map_result: Map the results between processes
        :param protocol: The protocol you want to open the file
        :param file_name: Name of file that you want to parse
        :type file_name: str
        :type map_result: list
        :return A tuple with the file parsed and the new file create witch contains the direct index
        """

        # init the document content
        self._document_content = []

        if self._extension == ".html" or self._extension == '.htm':
            # open the file with a specific protocol
            html_doc = urllib2.urlopen("{}{}".format(protocol, file_name))

            # create soup object to parse html files
            soup_object = BeautifulSoup(html_doc)

            # iterate the tags of document
            for tag in soup_object.findAll():

                if tag.name == 'meta':
                    continue

                elif tag.name == 'title':
                    self._document_content.append(str(tag.text))

                elif tag.name == 'a':
                    self._document_content.append(str(tag.attrs[0][1]))

                elif tag.name == 'body':
                    self._document_content.append(str(tag.getText(separator=" ")))

        elif self._extension == '.txt':
            with codecs.open(file_name, 'r', encoding='utf-8', errors='ignore') as file_handler:
                for line in file_handler:
                    line = line.strip()
                    if len(line) > 0:
                        self._document_content.append(line)

        # create the hash map
        self.generate_hash_map()

        # create the hash file and save it on disk
        new_file = self._create_hash_file(file_name)
        # return new_file, file_name

        self._compute_tf(file_name)

        # map the result
        map_result.append((new_file, file_name))

    def map_phase(self):
        """
        This method will iterate a list of files collected from directories and try to parse them and create the hash map for each of them 
        """
        try:
            # threading
            # map_result = self._pool_workers.map(self.parse_document, self._list_of_files_to_parse, chunksize=1)

            manager = Manager()  # []
            map_result = manager.list()
            processes = []
            for _file in self._list_of_files_to_parse:
                p = Process(target=self.parse_document, args=(_file, map_result,))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()
                p.terminate()

                # result = self.parse_document(_file)
                # map_result.append(result)

            return map_result

        except Exception as exc:
            print traceback.print_exc(exc)

    @staticmethod
    def reduce_phase(idx_id, filename):
        """
        Get saved hash from disk for a given file      
        :param filename: The  original file name who was parsed
        :param idx_id: The hash map correspondent for the file parsed
        :rtype: dict
        """
        temp_dict = {filename: {}}

        with open(idx_id) as file_handler:
            for line in file_handler:
                line = line.strip()
                word, value = line.split(":")

                temp_dict[filename][word] = value

        return temp_dict

    def __getstate__(self):
        """
        Get process state
        :return:
        """
        self_dict = self.__dict__.copy()
        del self_dict['_pool_workers']
        return self_dict

    def __setstate__(self, state):
        """
        Set process state
        :param state: 
        :return: 
        """
        self.__dict__.update(state)
