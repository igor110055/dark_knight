import logging

from celery import Celery
from celery.utils.log import get_task_logger
import sys
from os.path import abspath, dirname

root_dir = dirname(abspath(__file__))
sys.path.append(root_dir)


logging.basicConfig(level=logging.INFO)

logger = get_task_logger(__name__)


app = Celery('Storm')
default_config = 'celery_config'
app.config_from_object(default_config)


# @app.on_after_configure.connect
# def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    # sender.add_periodic_task(10.0, test.s('hello'), name='add every 10')

    # # Calls test('world') every 30 seconds
    # sender.add_periodic_task(30.0, test.s('world'), expires=10)

    # # Executes every Monday morning at 7:30 a.m.
    # sender.add_periodic_task(
    #     crontab(hour=7, minute=30, day_of_week=1),
    #     test.s('Happy Mondays!'),
    # )
