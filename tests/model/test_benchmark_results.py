# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality that retrieves benchmark result rankings."""

import os
import pytest

from passlib.hash import pbkdf2_sha256

from robapi.model.benchmark.engine import BenchmarkEngine
from robcore.model.template.benchmark.repo import BenchmarkRepository
from robapi.model.submission import SubmissionManager
from robcore.tests.benchmark import StateEngine
from robcore.model.template.schema import SortColumn
from robcore.model.template.repo.fs import TemplateFSRepository

import robcore.error as err
import robcore.tests.benchmark as wf
import robcore.tests.db as db
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')
TEMPLATE_WITHOUT_SCHEMA = os.path.join(DIR, '../.files/templates/template.json')


USER_1 = util.get_unique_identifier()


class TestBenchmarkResultRanking(object):
    """Unit tests for getting and setting run states. Uses a fake backend to
    simulate workflow execution.
    """
    def init(self, base_dir):
        """Create a fresh database with three users and return an open
        connection to the database. Returns instances of the benchmark
        repository, benchmark engine and the submission manager.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        con.execute(sql, (USER_1, USER_1, pbkdf2_sha256.hash(USER_1), 1))
        con.commit()
        repo = BenchmarkRepository(
            con=con,
            template_repo=TemplateFSRepository(base_dir=base_dir)
        )
        engine = BenchmarkEngine(con=con, backend=StateEngine())
        submissions = SubmissionManager(con=con, directory=base_dir)
        return repo, engine, submissions

    def test_get_results(self, tmpdir):
        """Test get result ranking for different submissions."""
        # Initialize the repository, benchmark engine and submission manager
        repo, engine, submissions = self.init(str(tmpdir))
        # Add two benchmarks and create two submissions for the first benchmark
        # and one submission for the second
        bm1 = repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        bm2 = repo.add_benchmark(name='B', src_dir=TEMPLATE_DIR)
        s1 = submissions.create_submission(
            benchmark_id=bm1.identifier,
            name='A',
            user_id=USER_1
        )
        s2 = submissions.create_submission(
            benchmark_id=bm1.identifier,
            name='B',
            user_id=USER_1
        )
        s3 = submissions.create_submission(
            benchmark_id=bm2.identifier,
            name='A',
            user_id=USER_1
        )
        # Insert three run results for the first submission and one for the
        # second submission
        wf.run_workflow(
            engine=engine,
            template=bm1.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 10, 'avg_count': 11.1, 'max_line': 'L3'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm1.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 5, 'avg_count': 21.1, 'max_line': 'L1'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm1.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 8, 'avg_count': 28.3, 'max_line': 'L1'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm1.get_template(),
            submission_id=s2.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 25, 'avg_count': 25.1, 'max_line': 'L4'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm2.get_template(),
            submission_id=s3.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 35, 'avg_count': 30.0, 'max_line': 'L1'}
        )
        assert s1.get_results().size() == 3
        assert s2.get_results().size() == 1
        assert s3.get_results().size() == 1
        s1 = submissions.get_submission(submission_id=s1.identifier)
        results = s1.get_results(order_by=[SortColumn('max_len')])
        assert results.size() == 3
        assert results.get(0).get('max_len') == 10
        assert results.get(1).get('max_len') == 8
        assert results.get(2).get('max_len') == 5
        results = s1.get_results(
            order_by=[SortColumn('max_len', sort_desc=False)]
        )
        assert results.size() == 3
        assert results.get(0).get('max_len') == 5
        assert results.get(1).get('max_len') == 8
        assert results.get(2).get('max_len') == 10
        results = s1.get_results(
            order_by=[
                SortColumn('max_line', sort_desc=False),
                SortColumn('avg_count')
            ]
        )
        assert results.size() == 3
        assert results.get(0).get('max_len') == 8
        assert results.get(1).get('max_len') == 5
        assert results.get(2).get('max_len') == 10
        # Leader board
        assert repo.get_leaderboard(bm1.identifier).size() == 2
        assert repo.get_leaderboard(bm1.identifier, include_all=True).size() == 4
        assert repo.get_leaderboard(bm2.identifier).size() == 1
        leaderboard = bm1.get_leaderboard()
        assert leaderboard.size() == 2
        assert leaderboard.get(0).get('max_len') == 8
        assert leaderboard.get(1).get('max_len') == 25

    def test_results_for_empty_schema(self, tmpdir):
        """Test get result ranking for template without result schema."""
        # Initialize the repository, benchmark engine and submission manager
        repo, engine, submissions = self.init(str(tmpdir))
        # Add benchmark and create two submissions
        benchmark = repo.add_benchmark(
            name='A',
            src_dir=TEMPLATE_DIR,
            spec_file=TEMPLATE_WITHOUT_SCHEMA
        )
        s1 = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user_id=USER_1
        )
        s2 = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='B',
            user_id=USER_1
        )
        # Insert three run results for the first submission and one for the
        # second submission
        wf.run_workflow(
            engine=engine,
            template=benchmark.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir)
        )
        wf.run_workflow(
            engine=engine,
            template=benchmark.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir)
        )
        wf.run_workflow(
            engine=engine,
            template=benchmark.get_template(),
            submission_id=s1.identifier,
            base_dir=str(tmpdir)
        )
        wf.run_workflow(
            engine=engine,
            template=benchmark.get_template(),
            submission_id=s2.identifier,
            base_dir=str(tmpdir)
        )
        assert s1.get_results().size() == 3
        assert s2.get_results().size() == 1
        s1 = submissions.get_submission(submission_id=s1.identifier)
        results = s1.get_results(order_by=[SortColumn('max_len')])
        assert results.size() == 3
        for e in results.entries:
            assert len(e.values) == 0
