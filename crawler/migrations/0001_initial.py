# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='WebCrawlerDirectIndex',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('document_name', models.TextField()),
                ('document_hash', models.TextField()),
                ('data_created', models.DateField(auto_now_add=True)),
            ],
        ),
    ]
