# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Interface to serialize benchmark resource objects."""


import robcore.view.hateoas as hateoas
import robcore.view.labels as labels


class BenchmarkSerializer(object):
    """Serializer for benchmark resource objects. Defines the methods that are
    used to serialize benchmark descriptors and handles.
    """
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: robcore.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def benchmark_descriptor(self, benchmark):
        """Get dictionary serialization containing the descriptor of a
        benchmark resource.

        Parameters
        ----------
        benchmark: robcore.model.template.benchmark.BenchmarkHandle
            Benchmark handle

        Returns
        -------
        dict
        """
        b_id = benchmark.identifier
        leaderboard_url = self.urls.get_leaderboard(b_id)
        rel_submission_create = hateoas.action(
            hateoas.CREATE,
            resource=hateoas.SUBMISSIONS
        )
        obj = {
            labels.ID: b_id,
            labels.NAME: benchmark.name,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.get_benchmark(b_id),
                hateoas.LEADERBOARD: leaderboard_url,
                rel_submission_create: self.urls.create_submission(b_id)
            })
        }
        if benchmark.has_description():
            obj[labels.DESCRIPTION] = benchmark.description
        if benchmark.has_instructions():
            obj[labels.INSTRUCTIONS] = benchmark.instructions
        return obj

    def benchmark_handle(self, benchmark):
        """Get dictionary serialization containing the handle of a
        benchmark resource.

        Parameters
        ----------
        benchmark: robcore.model.template.benchmark.BenchmarkHandle
            Benchmark handle

        Returns
        -------
        dict
        """
        obj = self.benchmark_descriptor(benchmark)
        # Add parameter declarations to the serialized benchmark descriptor
        parameters = benchmark.template.parameters.values()
        obj[labels.PARAMETERS] = [p.to_dict() for p in parameters]
        # Add module definitions if given
        modules = benchmark.template.modules
        if modules is not None:
            obj[labels.MODULES] = [
                {
                    labels.ID: m.identifier,
                    labels.NAME: m.name,
                    labels.INDEX: m.index
                } for m in modules]
        modules
        return obj

    def benchmark_leaderboard(self, benchmark, ranking):
        """Get dictionary serialization for a benchmark leaderboard.

        Parameters
        ----------
        benchmark: robcore.model.template.benchmark.BenchmarkHandle
            Benchmark handle
        leaderboard: robcore.model.ranking.ResultRanking
            List of entries in the benchmark leaderboard

        Returns
        -------
        dict
        """
        b_id = benchmark.identifier
        # Serialize ranking entries
        entries = list()
        for run in ranking.entries:
            results = list()
            for key in run.values:
                results.append({labels.ID: key, labels.VALUE: run.values[key]})
            entries.append({
                labels.RUN: {
                    labels.ID: run.run_id,
                    labels.CREATED_AT: run.created_at.isoformat(),
                    labels.STARTED_AT: run.started_at.isoformat(),
                    labels.FINISHED_AT: run.finished_at.isoformat()
                },
                labels.SUBMISSION: {
                    labels.ID: run.submission_id,
                    labels.NAME: run.submission_name
                },
                labels.RESULTS: results
            })
        # Serialize available benchmark post-processing resources
        resources = list()
        current_resources = benchmark.get_resources()
        if current_resources is not None:
            result_id = current_resources.result_id
            if result_id is not None:
                for r in current_resources:
                    url = self.urls.download_benchmark_resource(
                        benchmark_id=b_id,
                        result_id=result_id,
                        resource_id=r.identifier
                    )
                    resources.append({
                        labels.ID: r.identifier,
                        labels.NAME: r.name,
                        labels.CAPTION: r.caption,
                        labels.LINKS: hateoas.serialize({hateoas.SELF: url})
                    })
                resources.sort(key=lambda r: r[labels.ID], reverse=True)
        return {
            labels.SCHEMA: [{
                    labels.ID: c.identifier,
                    labels.NAME: c.name,
                    labels.DATA_TYPE: c.data_type
                } for c in ranking.columns
            ],
            labels.RANKING: entries,
            labels.RESOURCES: resources,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.get_leaderboard(b_id),
                hateoas.BENCHMARK: self.urls.get_benchmark(b_id),
                hateoas.RESOURCES: self.urls.download_benchmark_resources(
                    benchmark_id=b_id,
                    result_id=result_id
                )
            })
        }

    def benchmark_listing(self, benchmarks):
        """Get dictionary serialization of a benchmark listing.

        Parameters
        ----------
        benchmarks: list(robcore.model.template.benchmark.BenchmarkHandle)
            List of benchmark descriptors

        Returns
        -------
        dict
        """
        return {
            labels.BENCHMARKS: [self.benchmark_descriptor(b) for b in benchmarks],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_benchmarks()
            })
        }
