# coding=utf-8
"""
    Register Tables
"""
from django.contrib import admin
from models import WebCrawlerDirectIndex


# Register your models here.

class TableDirectIndex(admin.ModelAdmin):
    """
        Register Table
    """
    list_display = ('document_name', 'data_created')


admin.site.register(WebCrawlerDirectIndex, TableDirectIndex)
