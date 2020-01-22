# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The benchmark API component provides methods to list and access benchmarks
and benchmark leader boards.
"""

from flowserv.view.benchmark import BenchmarkSerializer
from flowserv.view.route import UrlFactory


class BenchmarkService(object):
    """API component that provides methods to access benchmarks and benchmark
    leader boards.
    """
    def __init__(self, repo, urls=None, serializer=None):
        """Initialize the internal reference to the benchmark repository and
        the route factory.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        repo: flowserv.model.template.repo.benchmark.BenchmarkRepository
            Repository to access registered benchmarks
        urls: flowserv.view.route.UrlFactory
            Factory for API resource Urls
        serializer: flowserv.view.benchmark.BenchmarkSerializer, optional
            Override the default serializer
        """
        self.repo = repo
        self.urls = urls if urls is not None else UrlFactory()
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = BenchmarkSerializer(self.urls)

    def get_benchmark(self, benchmark_id):
        """Get serialization of the handle for the given benchmark.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        """
        benchmark = self.repo.get_benchmark(benchmark_id)
        return self.serialize.benchmark_handle(benchmark)

    def get_benchmark_archive(self, benchmark_id, result_id=None):
        """Get in-memory compressed tar-file containing all resource files for
        a benchmark that have been generated by the post-processing step. If
        the result identifier is None the latest resource results are returned.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        result_id: string, optional
            Unique identifier of the result run that generated the resources

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        flowserv.core.error.UnknownResourceError
        """
        return self.repo.get_benchmark_archive(
            benchmark_id=benchmark_id,
            result_id=result_id
        )

    def get_benchmark_resource(self, benchmark_id, resource_id, result_id=None):
        """Get file handle for a benchmark resource that has been generated
        by the post-processing step. If the result identifier is None the
        latest resource results are returned.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        resource_id: string
            Unique resource identifier
        result_id: string, optional
            Unique identifier of the post-processing result set

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        flowserv.core.error.UnknownResourceError
        """
        return self.repo.get_benchmark_resource(
            benchmark_id=benchmark_id,
            resource_id=resource_id,
            result_id=result_id
        )

    def get_leaderboard(self, benchmark_id, order_by=None, include_all=False):
        """Get serialization of the leader board for the given benchmark.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the
            schema default attribute is used
        include_all: bool, optional
            Include at most one entry per submission in the result if False

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        """
        # Get list with run results. This will raise an unknown benchmark error
        # if the given identifier does not reference an existing benchmark.
        results = self.repo.get_leaderboard(
            benchmark_id=benchmark_id,
            order_by=order_by,
            include_all=include_all
        )
        return self.serialize.benchmark_leaderboard(
            benchmark=self.repo.get_benchmark(benchmark_id),
            ranking=results
        )

    def list_benchmarks(self):
        """Get serialized listing of all benchmarks in the repository.

        Parameters
        ----------
        access_token: string, optional
            User access token

        Returns
        -------
        dict
        """
        benchmarks = self.repo.list_benchmarks()
        return self.serialize.benchmark_listing(benchmarks)