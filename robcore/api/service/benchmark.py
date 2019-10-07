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

from robapi.serialize.benchmark import BenchmarkSerializer
from robapi.service.route import UrlFactory


class BenchmarkService(object):
    """API component that provides methods to access benchmarks and benchmark
    leader boards.
    """
    def __init__(self, repo, urls=None, serializer=None):
        """Initialize the internal reference to the benchmark repository and
        the route factory.

        Parameters
        ----------
        repo: robcore.model.template.repo.benchmark.BenchmarkRepository
            Repository to access registered benchmarks
        urls: robapi.service.route.UrlFactory
            Factory for API resource Urls
        serializer: robapi.serialize.benchmark.BenchmarkSerializer, optional
            Override the default serializer
        """
        self.repo = repo
        self.urls = urls if not urls is None else UrlFactory()
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
        robcore.error.UnknownBenchmarkError
        """
        benchmark = self.repo.get_benchmark(benchmark_id)
        return self.serialize.benchmark_handle(benchmark)

    def get_leaderboard(self, benchmark_id, include_all=False):
        """Get serialization of the handle for the given benchmark.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        include_all: bool, optional
            Include at most one entry per submission in the result if False

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.UnknownBenchmarkError
        """
        # Return serialized benchmark handle
        ranking = self.repo.get_leaderboard(
            benchmark_id=benchmark_id,
            include_all=include_all
        )
        return self.serialize.benchmark_leaderboard(
            benchmark_id=benchmark_id,
            ranking=ranking
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
