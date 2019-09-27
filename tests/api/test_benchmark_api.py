# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test API methods for benchmark resources."""

import os

from passlib.hash import pbkdf2_sha256

from robapi.model.benchmark.engine import BenchmarkEngine
from robcore.model.submission import SubmissionManager
from robcore.model.template.benchmark.repo import BenchmarkRepository
from robapi.service.benchmark import BenchmarkService
from robcore.tests.benchmark import StateEngine
from robcore.model.template.repo.fs import TemplateFSRepository

import robapi.serialize.hateoas as hateoas
import robapi.serialize.labels as labels
import robcore.tests.benchmark as wf
import robcore.tests.db as db
import robcore.tests.serialize as serialize
import robcore.model.template.parameter.declaration as pd
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default benchmark user
USER_1 = util.get_unique_identifier()

# Mandatory HATEOAS relationships in benchmark handles and descriptors
RELS = [hateoas.SELF, hateoas.LEADERBOARD]


class TestBenchmarkApi(object):
    """Test API methods that access and list benchmarks and leader boards."""
    def init(self, base_dir):
        """Initialize the database and the benchmark repository. Loads three
        copies of the same benchmark and returns a list of the benchmark
        handles.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        con.execute(sql, (USER_1, USER_1, pbkdf2_sha256.hash(USER_1), 1))
        repo = BenchmarkRepository(
            con=con,
            template_repo=TemplateFSRepository(base_dir=base_dir)
        )
        benchmarks = list()
        benchmarks.append(repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR))
        benchmarks.append(
            repo.add_benchmark(
                name='B',
                description='desc',
                src_dir=TEMPLATE_DIR
            )
        )
        benchmarks.append(
            repo.add_benchmark(
                name='C',
                description='desc',
                instructions='inst',
                src_dir=TEMPLATE_DIR
            )
        )
        return BenchmarkService(repo=repo), benchmarks, con

    def test_get_benchmark(self, tmpdir):
        """Test get benchmark handle."""
        repo, benchmarks, _ = self.init(str(tmpdir))
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
        repo, benchmarks, con = self.init(str(tmpdir))
        # Create one submission and add three results for the first benchmark
        engine = BenchmarkEngine(con=con, backend=StateEngine())
        submissions = SubmissionManager(con=con, directory=str(tmpdir))
        # Add two benchmarks and create two submissions for the first benchmark
        # and one submission for the second
        bm = benchmarks[0]
        s1 = submissions.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        wf.run_workflow(
            engine=engine,
            template=bm.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 10, 'avg_count': 11.1, 'max_line': 'L1'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 11, 'avg_count': 12.1, 'max_line': 'L2'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 12, 'avg_count': 13.1, 'max_line': 'L3'}
        )
        r = repo.get_leaderboard(bm.identifier, include_all=False)
        util.validate_doc(
            doc=r,
            mandatory_labels=[labels.RANKING, labels.SCHEMA, labels.LINKS]
        )
        serialize.validate_links(r, [hateoas.SELF, hateoas.BENCHMARK])
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
        repo, benchmarks, _ = self.init(str(tmpdir))
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
