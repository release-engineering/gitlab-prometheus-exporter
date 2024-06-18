This is an experimental prometheus exporter for gitlab, with an emphasis on exposing *counters*
about pipelines.

[mvisonneau/gitlab-ci-pipelines-exporter](https://github.com/mvisonneau/gitlab-ci-pipelines-exporter) is another good alternative.

This polls a gitlab instance for a configured list of projects and exposes prometheus metrics about
the ci pipeline runs for each project. It does not support authentication.
