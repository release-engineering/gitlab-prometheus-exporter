#!/usr/bin/env python3
""" A simple prometheus exporter for gitlab.

Scrapes gitlab on an interval and exposes metrics about pipelines.
"""

import logging
import os
import datetime
import time
import urllib

import arrow
import requests

from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)
from prometheus_client import start_http_server


# Required inputs
GITLAB_URL = os.environ['GITLAB_URL']
PROJECTS = [p.strip() for p in os.environ['GITLAB_PROJECTS'].split(',')]
TOKEN = os.environ['GITLAB_TOKEN']

session = requests.Session()
session.headers = {'Authorization': f'Bearer {TOKEN}'}

LABELS = ['project', 'branch']
BRANCH = 'master'  # Only support the master branch for now
START = datetime.datetime.utcnow().isoformat()
metrics = {}

# In seconds
DURATION_BUCKETS = [180, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700]


error_status = "failed"


class IncompletePipeline(Exception):
    """ Error raised when a gitlab pipeline is not complete. """

    pass


def get_gitlab_pipelines(project, **kwargs):
    slug = urllib.parse.quote_plus(project)
    url = f"{GITLAB_URL}/api/v4/projects/{slug}/pipelines"
    params = dict(ref=BRANCH, order_by='updated_at', sort='asc')
    params.update(kwargs)
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


def gitlab_pipelines_total(data):
    counts = {}

    for project, pipelines in data.items():
        counts[project] = counts.get(project, 0)
        for pipeline in pipelines:
            counts[project] += 1

    for project in counts:
        yield counts[project], [project, BRANCH]


def retrieve_gitlab_pipelines(**kwargs):
    result = {}
    for project in PROJECTS:
        result[project] = list(get_gitlab_pipelines(project, **kwargs))
    return result


def calculate_duration(pipeline):
    if pipeline['status'] != 'success':
        # Duration is undefined.
        # Failed pipelines can be restarted an arbitrary number of times.  A pipeline isn't done
        # until it succeeds, which makes it hard to handle in a prometheus Counter.
        raise IncompletePipeline(
            "Pipeline is not yet complete.  Duration is undefined."
        )
    return (
        arrow.get(pipeline['updated_at']) - arrow.get(pipeline['created_at'])
    ).total_seconds()


def find_applicable_buckets(duration):
    buckets = DURATION_BUCKETS + ["+Inf"]
    for bucket in buckets:
        if duration < float(bucket):
            yield bucket


def gitlab_pipeline_duration_seconds(data):
    counts = {}
    duration_buckets = DURATION_BUCKETS + ["+Inf"]

    for project, pipelines in data.items():
        for pipeline in pipelines:

            try:
                duration = calculate_duration(pipeline)
            except IncompletePipeline:
                continue

            # Initialize structure
            counts[project] = counts.get(project, {})
            for bucket in duration_buckets:
                counts[project][bucket] = counts[project].get(bucket, 0)

            # Increment applicable bucket counts
            for bucket in find_applicable_buckets(duration):
                counts[project][bucket] += 1

    for project in counts:
        buckets = [
            (str(bucket), counts[project][bucket]) for bucket in duration_buckets
        ]
        yield buckets, [project, BRANCH]


def only(data, status):
    result = {}
    for project, pipelines in data.items():
        result[project] = [p for p in pipelines if p['status'] == status]
    return result


def scrape():
    pipelines = retrieve_gitlab_pipelines(updated_after=START)

    gitlab_pipelines_total_family = CounterMetricFamily(
        'gitlab_pipelines_total', 'Count of all gitlab pipelines', labels=LABELS
    )
    for value, labels in gitlab_pipelines_total(pipelines):
        gitlab_pipelines_total_family.add_metric(labels, value)

    # raise NotImplementedError("Need special handling for retried error pipelines.")
    # gitlab_pipeline_errors_total_family = CounterMetricFamily(
    #     'gitlab_pipeline_errors_total', 'Count of all gitlab pipeline errors', labels=LABELS
    # )
    # error_pipelines = only(pipelines, status=error_status)
    # for value, labels in gitlab_pipelines_total(error_pipelines):
    #     gitlab_pipeline_errors_total_family.add_metric(labels, value)

    gitlab_in_progress_pipelines_family = GaugeMetricFamily(
        'gitlab_in_progress_pipelines',
        'Count of all in-progress gitlab pipelines',
        labels=LABELS,
    )
    in_progress_pipelines = retrieve_gitlab_pipelines(status="running")
    for value, labels in gitlab_pipelines_total(in_progress_pipelines):
        gitlab_in_progress_pipelines_family.add_metric(labels, value)

    gitlab_pipeline_duration_seconds_family = HistogramMetricFamily(
        'gitlab_pipeline_duration_seconds',
        'Histogram of gitlab pipeline durations',
        labels=LABELS,
    )
    for buckets, labels in gitlab_pipeline_duration_seconds(pipelines):
        gitlab_pipeline_duration_seconds_family.add_metric(
            labels, buckets, sum_value=None
        )

    # Replace this in one atomic operation to avoid race condition to the Expositor
    metrics.update(
        {
            'gitlab_pipelines_total': gitlab_pipelines_total_family,
            # 'gitlab_pipeline_errors_total': gitlab_pipeline_errors_total_family,
            'gitlab_in_progress_pipelines': gitlab_in_progress_pipelines_family,
            'gitlab_pipeline_duration_seconds': gitlab_pipeline_duration_seconds_family,
        }
    )


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
