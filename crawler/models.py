from django.db import models


# Create your models here.

class WebCrawlerDirectIndex(models.Model):
    """
        Class to index
    """
    document_name = models.TextField()
    document_hash = models.TextField()
    data_created = models.DateField(auto_now_add=True)
