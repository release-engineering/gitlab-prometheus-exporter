#!/usr/bin/env python3
""" A simple prometheus exporter for gitlab.

Scrapes gitlab on an interval and exposes metrics about pipelines.
"""

import logging
import os
import datetime
import time
import urllib

import requests

from prometheus_client.core import REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server


# Required inputs
GITLAB_URL = os.environ['GITLAB_URL']
PROJECTS = [p.strip() for p in os.environ['GITLAB_PROJECTS'].split(',')]
TOKEN = os.environ['GITLAB_TOKEN']

session = requests.Session()
session.headers = {'Authorization': f'Bearer {TOKEN}'}

LABELS = ['organization', 'project', 'branch', 'status']
BRANCH = 'master'  # Only support the master branch for now
# START = datetime.datetime.utcnow().isoformat()
START = (datetime.datetime.utcnow() - datetime.timedelta(days=3)).isoformat()
metrics = {}


def get_gitlab_pipelines(project):
    slug = urllib.parse.quote_plus(project)
    url = f"{GITLAB_URL}/api/v4/projects/{slug}/pipelines"
    params = dict(ref=BRANCH, updated_after=START, order_by='updated_at', sort='asc')
    page = 1
    while True:
        params['page'] = page
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not data:
            break

        yield from data
        page = page + 1


def gitlab_pipelines_total():
    counts = {}
    for project in PROJECTS:
        organization, _ = project.split('/', 1)
        pipelines = list(get_gitlab_pipelines(project))

        counts[organization] = counts.get(organization, {})
        counts[organization][project] = counts[organization].get(project, {})
        for pipeline in pipelines:
            status = pipeline['status']
            counts[organization][project][status] = counts[organization][project].get(
                status, 0
            )
            counts[organization][project][status] += 1

    for organization in counts:
        for project in counts[organization]:
            for status in counts[organization][project]:
                yield counts[organization][project][status], [
                    organization,
                    project,
                    BRANCH,
                    status,
                ]


def scrape():
    family = CounterMetricFamily('gitlab_pipelines_total', 'Help text', labels=LABELS)
    for value, labels in gitlab_pipelines_total():
        family.add_metric(labels, value)
    # Replace this in one atomic operation to avoid race condition to the collector
    metrics['gitlab_pipelines_total'] = family


class Expositor(object):
    """ Responsible for exposing metrics to prometheus """

    def collect(self):
        logging.info("Serving prometheus data")
        for key in sorted(metrics):
            yield metrics[key]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    start_http_server(8000)
    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)
    REGISTRY.register(Expositor())
    while True:
        scrape()
        time.sleep(int(os.environ.get('GITLAB_POLL_INTERVAL', '3')))
