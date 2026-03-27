# Скопируй этот файл в PythonAnywhere:
# /var/www/zakcheks_pythonanywhere_com_wsgi.py
# Укажи virtualenv во вкладке Web — тогда этот файл только добавляет путь и импортирует app.

import sys

project_home = '/home/zakcheks/vk_public_rewriter'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

from app import app as application
