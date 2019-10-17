# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test API methods for benchmark resources."""

import os

import robcore.api.serialize.hateoas as hateoas
import robcore.api.serialize.labels as labels
import robcore.model.template.parameter.declaration as pd
import robcore.tests.api as api
import robcore.tests.serialize as serialize
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default benchmark user
USER_1 = util.get_unique_identifier()

# Mandatory HATEOAS relationships in benchmark handles and descriptors
RELS = [hateoas.SELF, hateoas.LEADERBOARD]


class TestBenchmarkApi(object):
    """Test API methods that access and list benchmarks and leader boards."""
    @staticmethod
    def init(base_dir):
        """Initialize the database and the benchmark repository. Loads three
        copies of the same benchmark. Returns a tripple that contains the
        benchmark reposiroty, the list of the benchmark handles, and the open
        database connection.
        """
        repo, submissions, users, _, _ = api.init_api(base_dir)
        # Create one new user
        users.register_user(USER_1, USER_1)
        # Create three benchmarks
        benchmarks = list()
        benchmarks.append(
            repo.repo.add_benchmark(
                name='A',
                src_dir=TEMPLATE_DIR
            )
        )
        benchmarks.append(
            repo.repo.add_benchmark(
                name='B',
                description='desc',
                src_dir=TEMPLATE_DIR
            )
        )
        benchmarks.append(
            repo.repo.add_benchmark(
                name='C',
                description='desc',
                instructions='inst',
                src_dir=TEMPLATE_DIR
            )
        )
        return repo, submissions, benchmarks

    def test_get_benchmark(self, tmpdir):
        """Test get benchmark handle."""
        repo, _, benchmarks = TestBenchmarkApi.init(str(tmpdir))
        r = repo.get_benchmark(benchmarks[0].identifier)
        util.validate_doc(
            doc=r,
            mandatory_labels=[
                labels.ID,
                labels.NAME,
                labels.LINKS,
                labels.PARAMETERS
            ]
        )
        serialize.validate_links(r, RELS)
        assert len(r[labels.PARAMETERS]) == 3
        for para in r[labels.PARAMETERS]:
            util.validate_doc(
                doc=para,
                mandatory_labels=[
                    pd.LABEL_ID,
                    pd.LABEL_NAME,
                    pd.LABEL_DATATYPE,
                    pd.LABEL_DESCRIPTION,
                    pd.LABEL_INDEX,
                    pd.LABEL_REQUIRED
                ],
                optional_labels=[
                    pd.LABEL_DEFAULT,
                    pd.LABEL_AS
                ]
            )
        r = repo.get_benchmark(benchmarks[1].identifier)
        util.validate_doc(
            doc=r,
            mandatory_labels=[
                labels.ID,
                labels.NAME,
                labels.DESCRIPTION,
                labels.LINKS,
                labels.PARAMETERS
            ]
        )
        serialize.validate_links(r, RELS)
        r = repo.get_benchmark(benchmarks[2].identifier)
        util.validate_doc(
            doc=r,
            mandatory_labels=[
                labels.ID,
                labels.NAME,
                labels.DESCRIPTION,
                labels.INSTRUCTIONS,
                labels.LINKS,
                labels.PARAMETERS
            ]
        )
        serialize.validate_links(r, RELS)

    def test_get_leaderboard(self, tmpdir):
        """Test get benchmark leaderboard."""
        repo, submissions, benchmarks = TestBenchmarkApi.init(str(tmpdir))
        # Create one submission and add three results for the first benchmark
        controller = submissions.manager.engine.backend
        # Create two submissions for the first benchmark.
        bm = benchmarks[0]
        template = bm.get_template()
        s1 = submissions.manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        s2 = submissions.manager.create_submission(
            benchmark_id=bm.identifier,
            name='B',
            user_id=USER_1
        )
        # Create two successful runs for the first submission and one run for
        # the second submission.
        controller.success({'avg_count': 12, 'max_len': 12.4})
        submissions.manager.start_run(s1.identifier, dict(), template)
        controller.success({'avg_count': 10, 'max_len': 12.4})
        submissions.manager.start_run(s1.identifier, dict(), template)
        controller.success({'avg_count': 11, 'max_len': 12.4})
        submissions.manager.start_run(s2.identifier, dict(), template)
        # Get the benchmark leaderboard
        r = repo.get_leaderboard(bm.identifier, include_all=False)
        util.validate_doc(
            doc=r,
            mandatory_labels=[labels.RANKING, labels.SCHEMA, labels.LINKS]
        )
        serialize.validate_links(r, [hateoas.SELF, hateoas.BENCHMARK])
        assert len(r[labels.RANKING]) == 2
        r = repo.get_leaderboard(bm.identifier, include_all=True)
        util.validate_doc(
            doc=r,
            mandatory_labels=[labels.RANKING, labels.SCHEMA, labels.LINKS]
        )
        serialize.validate_links(r, [hateoas.SELF, hateoas.BENCHMARK])
        ranking = r[labels.RANKING]
        assert len(ranking) == 3
        for run in ranking:
            util.validate_doc(
                doc=run,
                mandatory_labels=[labels.RUN, labels.SUBMISSION, labels.RESULTS]
            )
            util.validate_doc(
                doc=run[labels.RUN],
                mandatory_labels=[
                    labels.ID,
                    labels.CREATED_AT,
                    labels.STARTED_AT,
                    labels.FINISHED_AT
                ]
            )
            util.validate_doc(
                doc=run[labels.SUBMISSION],
                mandatory_labels=[labels.ID, labels.NAME]
            )
            for res in run[labels.RESULTS]:
                util.validate_doc(
                    doc=res,
                    mandatory_labels=[labels.ID, labels.VALUE]
                )

    def test_list_benchmarks(self, tmpdir):
        """Test list benchmark descriptors."""
        repo, _, benchmarks = TestBenchmarkApi.init(str(tmpdir))
        r = repo.list_benchmarks()
        util.validate_doc(
            doc=r,
            mandatory_labels=[
                labels.BENCHMARKS,
                labels.LINKS
            ]
        )
        # Create a dictionary of benchmark handles keyed by their name
        b_index = dict()
        for bm in r[labels.BENCHMARKS]:
            serialize.validate_links(bm, RELS)
            b_index[bm[labels.NAME]] = bm
        # Validate the descriptors for the three different benchmarks
        util.validate_doc(
            doc=b_index['A'],
            mandatory_labels=[
                labels.ID,
                labels.NAME,
                labels.LINKS
            ]
        )
        util.validate_doc(
            doc=b_index['B'],
            mandatory_labels=[
                labels.ID,
                labels.NAME,
                labels.DESCRIPTION,
                labels.LINKS
            ]
        )
        util.validate_doc(
            doc=b_index['C'],
            mandatory_labels=[
                labels.ID,
                labels.NAME,
                labels.DESCRIPTION,
                labels.INSTRUCTIONS,
                labels.LINKS
            ]
        )
