#!/usr/bin/python
# coding=utf-8
##############################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from test import run_only
from mock import patch

from diamond.collector import Collector
from celeryflower import CeleryFlowerCollector

import json
import urlparse

##############################################################################


def run_only_if_requests_is_available(func):
    try:
        import requests
        has_requests = True
    except ImportError:
        has_requests = False

    return run_only(func, lambda: has_requests)


class TestCeleryFlowerCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('CeleryFlowerCollector', {
            'host': 'localhost:5555',
            'user': 'guest',
            'password': 'password',
        })
        self.collector = CeleryFlowerCollector(config, None)

    def test_import(self):
        self.assertTrue(CeleryFlowerCollector)

    @run_only_if_requests_is_available
    @patch('celeryflower.requests.get')
    @patch.object(Collector, 'publish')
    def test_should_publish_nested_keys(self, publish_mock, requests_get):
        import requests
        worker_data = requests.Response()
        worker_data.status_code = 200
        worker_data._content = json.dumps({
            'celery@worker-1': {
                'completed_tasks': 205224,
                'concurrency': None,
                'queues': ['do_something_more', 'celery'],
                'running_tasks': 0,
                'status': True
            },
            'celery@worker-2': {
                'completed_tasks': 1023,
                'concurrency': None,
                'queues': ['do_something'],
                'running_tasks': 22,
                'status': True
            },
            'celery@worker-3': {
                'completed_tasks': 0,
                'concurrency': None,
                'queues': ['do_something_more', 'celery'],
                'running_tasks': 0,
                'status': False
            }
        })

        requests_get.return_value = worker_data
        self.collector.collect()
        url = urlparse.urljoin(self.collector.config['url'], '/api/workers')
        requests_get.assert_called_once_with(url, timeout=5)

        metrics = {
            'worker.worker-1.status': 1,
            'worker.worker-1.running_tasks': 0,
            'worker.worker-1.completed_tasks': 205224,
            'worker.worker-1.handled_queues': 2,
            'worker.worker-2.status': 1,
            'worker.worker-2.running_tasks': 22,
            'worker.worker-2.completed_tasks': 1023,
            'worker.worker-2.handled_queues': 1,
            'worker.worker-3.status': 0,
            'worker.worker-3.running_tasks': 0,
            'worker.worker-3.completed_tasks': 0,
            'worker.worker-3.handled_queues': 2,
            'workers.count': 3,
            'workers.active': 2,
            'workers.inactive': 1,
        }

        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['path'])
        self.assertPublishedMany(publish_mock, metrics)


##############################################################################
if __name__ == "__main__":
    unittest.main()
