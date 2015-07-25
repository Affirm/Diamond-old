# coding=utf-8

"""
Collects worker data from celery flower using the API

Configuration
* url - Celery flower url (default: http://127.0.0.1:5555)
* user - Username for http basic auth
* pass - Password for http basic auth

#### Dependencies

 * requests

"""

import diamond.collector
import requests
import urlparse


class CeleryFlowerCollector(diamond.collector.Collector):
    def get_default_config_help(self):
        config_help = super(
            CeleryFlowerCollector, self).get_default_config_help()
        config_help.update({
            'url':      'Celery flower URI',
            'user':     'Username for connecting to flower',
            'passwd':   'Password for connecting to flower',
            'timeout':  'Timeout (secs) for connecting to flower',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(CeleryFlowerCollector, self).get_default_config()
        config.update({
            'url':          'http://localhost:5555',
            'user':         None,
            'passwd':       None,
            'path':         'flower',
            'timeout':      5,
        })
        return config

    def collect(self):
        # Get the json
        url = urlparse.urljoin(self.config['url'], '/api/workers')

        if self.config['user'] is not None and \
                self.config['passwd'] is not None:
            auth = (self.config['user'], self.config['passwd'])
        else:
            auth = None

        response = requests.get(url, auth=auth, timeout=self.config['timeout'])

        try:
            response.raise_for_status()
        except:
            self.log.exception('Error collecting flower metrics')
            return {}

        workers = response.json()
        active = 0
        inactive = 0

        for worker, status in workers.iteritems():
            worker_id = worker.split('@')[1]

            for metric in ('running_tasks', 'completed_tasks', 'status'):
                key = 'worker.%s.%s' % (worker_id, metric)
                self.publish(key, int(status[metric]))

            self.publish('worker.%s.handled_queues' % (worker_id),
                         len(status['queues']))

            if status['status']:
                active += 1
            else:
                inactive += 1

        self.publish('workers.count', len(workers))
        self.publish('workers.active', active)
        self.publish('workers.inactive', inactive)
