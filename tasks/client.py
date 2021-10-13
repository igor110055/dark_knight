import logging

from celery import Celery
from celery.utils.log import get_task_logger
from redis_client import get_client


logging.basicConfig(level=logging.INFO)

redis = get_client()

logger = get_task_logger(__name__)

app = Celery('RedisTask', broker='redis://localhost:6379/1',
             backend='redis://localhost:6379/2')