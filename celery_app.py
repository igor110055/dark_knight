import logging

from celery import Celery
from celery.utils.log import get_task_logger
import sys
from os.path import abspath, dirname

root_dir = dirname(abspath(__file__))
sys.path.append(root_dir)


logging.basicConfig(level=logging.INFO)

logger = get_task_logger(__name__)


app = Celery('Storm', broker='redis://localhost:6379/1',
             backend='redis://localhost:6379/2')




app.autodiscover_tasks(packages=['tasks.order_book_task'])
# from tasks.order_book_task import *