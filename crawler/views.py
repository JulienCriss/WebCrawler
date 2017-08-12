# coding=utf-8
"""
    Create views here
"""
import json
import os
import traceback

import time
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

from .modules.web_crawler import WebCrawler
from .modules.utils import clear_db_tables


def crawler_search_engine(request):
    """
    Make an interrogation on crawler
    :param request:
    :return:
    """
    data = 'fail'

    if request.is_ajax():
        try:
            start_time = time.time()
            reverse_index_file = os.environ["LAST_REVERSE_INDEX_FILE"]
            interrogation = request.GET.get('term', '')
            interrogation = interrogation.split(" ")

            results = WebCrawler.make_interrogation(interrogation, reverse_index_file)

            data = json.dumps(results)
            print("--- Query results in %s seconds ---" % (time.time() - start_time))
        except Exception as exc:
            print traceback.print_exc(exc)
    else:
        results = [{'id': 'idUnique', 'label': 'no match', 'value': 'no match'}]
        data = json.dumps(results)

    mime_type = 'application/json'
    return HttpResponse(data, mime_type)


def document_parsing(request):
    """
    Set the crawler and parse all documents from the directory which user sets
    :param request: 
    :return: 
    """

    if request.method == "POST":
        start_time = time.time()
        extension = request.POST.get('extension', "")
        dir_location = request.POST.get('workdirPath', "")
        path_to_stop_words = request.POST.get('stopWordsFile', "")
        path_to_exception_words = request.POST.get('excpWordsFile', "")
        direct_index_dir = request.POST.get('dirIndex', "")
        reverse_index_dir = request.POST.get('revIndex', "")

        clear_db_tables("localhost", 27017, "crawler", ["tf_dict", "reverse_dict", "tf_idf_dict"])

        WebCrawler(path_to_stop_words, path_to_exception_words, dir_location, extension, direct_index_dir, reverse_index_dir)

        print("--- Parsing documents in %s seconds ---" % (time.time() - start_time))
        return HttpResponseRedirect("/")

    if request.method == "GET":
        root_folder = os.path.abspath(os.path.dirname(__name__))
        work_dir = r'{}\workdir'.format(root_folder)
        stop_words_dir = r'{}\doc_words\stop_words.txt'.format(root_folder)
        exception_words_dir = r'{}\doc_words\exceptions.txt'.format(root_folder)
        direct_index_dir = r'{}\direct_index'.format(root_folder)
        reverse_index_dir = r'{}\reverse_index'.format(root_folder)

        return render(request, "settings.html", {'work_dir': work_dir, 'stop_words_dir': stop_words_dir, 'exception_words_dir': exception_words_dir,
                                                 'direct_index_dir': direct_index_dir, 'reverse_index_dir': reverse_index_dir})
