# -*- coding: utf-8 -*-
"""
    celery.backends.kafka
    ~~~~~~~~~~~~~~~~~~~~~

    kafka result store backend.

"""
from __future__ import absolute_import

try:
    from kafka.client import KafkaClient
    from kafka.consumer import SimpleConsumer
    from kafka.producer import KeyedProducer
    from kafka.queue import KafkaQueue
except ImportError:
    KafkaClient = None

import time
from kombu.utils import cached_property, retry_over_time
from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.five import monotonic
from celery.utils.log import get_logger
from celery.utils.timeutils import maybe_timedelta, timedelta_seconds
from .base import BaseBackend

__all__ = ['KafkaBackend']

logger = get_logger(__name__)

class KafkaBackend(BaseBackend):
    """ 
        Apache Kafka backend
    """
    servers = []
    topic = None
    detailed_mode = False
    _retry_timeout = 300
    _retry_wait = 3
    supports_autoexpire = True
    block = False

    def __init__(self, servers=None, topic=None, block=None, timeout=None,
            kafka_options=None, async=True, **kwargs):
        super(KafkaBackend, self).__init__(**kwargs)
        self.expires = kwargs.get('expires') or maybe_timedelta(
          self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        self.timeout = timeout or self._retry_timeout
        self.block = block or self.block
        if not KafkaClient:
            raise ImproperlyConfigured(
            'You need to install kafka-python library to use the Kafka backend'
            'See https://github.com/mumrah/kafka-python')
        
        conf = self.app.conf
        self.server = ( servers or 
                      conf.get('KAFKA_SERVERS') or
                      self.servers)
        self.topic = (topic or
                      conf.get('KAFKA_TOPIC') or
                      self.topic)
        self.kafka_options = dict(conf.get('KAFKA_OPTIONS') or {},
            **kafka_options or {})

        if not self.servers or not self.topic:
            raise ImproperlyConfigured(
                'KAFKA backend not properly configured.')

        self.kafka = KafkaClient(self.servers)

    @cached_property
    def client(self):
        return self.kafka

    @cached_property
    def producer(self):
        return KeyedProducer(self.client)

    @cached_property
    def consumer(self):
        return SimpleConsumer(self.client)

    @cached_property
    def queue(self):
        return KafkaQueue(self.client, self.topic,
            self.partitions, producer_config=self.producer_config, consumer_config=self.consumer_config)


    def close_queue(self):
        self.queue.close()

    def get(self):
        return self.queue.get(block=self.block, timeout=self.timeout)