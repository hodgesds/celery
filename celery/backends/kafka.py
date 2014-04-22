from __future__ import absolute_import

try:
    from kafka.client import KafkaClient
except ImportError:
    KafkaClient = None

import time

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.fix import monotonic
from celery.utils.log import get_logger
from celery.utils.timeutiles import maybe_timedelta, timedelta_seconds
from .base import BaseBackend

__all__ = ['KafkaBackend']

logger = get_logger(__name__)

class KafkaBackend(BaseBackend):
    """ 
        Apache Kafka backend
    """
    servers = []
    keyspace = None
    column_family = None
    detailed_mode = False
    _retry_timeout = 300
    _retry_wait = 3
    supports_autoexpire = True

    def __init__(self, servers=None, keyspace=None, column_family=None,
            kafka_options=None, detailed_mode=False, **kwargs):
        super(KafkaBackend, self).__init__(**kwargs)
        self.expires = kwargs.get('expires') or maybe_timedelta(
          self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        if not KafkaClient:
            raise ImproperlyConfigured(
            'You need to install kafka-python library to use the Kafka backend'
            'See https://github.com/mumrah/kafka-python')
        
        conf = self.app.conf
        self.server = ( servers or 
                      conf.get('KAFKA_SERVERS') or
                      self.servers)
        self.keyspace = (keyspace or 
                      conf.get('KAFKA_KEYSPACE') or
                      self.keyspace)
        
