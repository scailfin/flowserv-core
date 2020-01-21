# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality for the module that maintains and retrieves benchmark
result rankings.
"""

import json
import os
import pytest

from passlib.hash import pbkdf2_sha256

from flowserv.model.template.repo.benchmark import BenchmarkRepository
from flowserv.model.template.store import TemplateRepository
from flowserv.model.template.schema import SortColumn
from flowserv.model.workflow.resource import FSObject

import flowserv.controller.run as runstore
import flowserv.core.error as err
import flowserv.model.ranking as ranking
import flowserv.model.workflow.state as wfstate
import flowserv.tests.benchmark as bm
import flowserv.tests.db as db
import flowserv.core.util as util


BENCHMARK_1 = util.get_unique_identifier()
BENCHMARK_2 = util.get_unique_identifier()
SUBMISSION_1 = util.get_unique_identifier()
SUBMISSION_2 = util.get_unique_identifier()
SUBMISSION_3 = util.get_unique_identifier()
USER_ID = util.get_unique_identifier()


class TestBenchmarkResultRanking(object):
    """Unit tests for inserting benchmark results and for retrieving result
    rankings.
    """
    @staticmethod
    def init(basedir):
        """Create a fresh database with one user, two benchmark, and three
        submissions. The first benchmark has a schema and two submissions while
        the second benchmark has no schema and one submission.
        """
        con = db.init_db(basedir).connect()
        sql = (
            'INSERT INTO api_user(user_id, name, secret, active) '
            'VALUES(?, ?, ?, ?)'
        )
        con.execute(sql, (USER_ID, USER_ID, pbkdf2_sha256.hash(USER_ID), 1))
        sql = 'INSERT INTO benchmark(benchmark_id, name, result_schema) '
        sql += 'VALUES(?, ?, ?)'
        schema = json.dumps(bm.BENCHMARK_SCHEMA.to_dict())
        con.execute(sql, (BENCHMARK_1, BENCHMARK_1, schema))
        sql = 'INSERT INTO benchmark(benchmark_id, name) VALUES(?, ?)'
        con.execute(sql, (BENCHMARK_2, BENCHMARK_2))
        sql = (
            'INSERT INTO benchmark_submission(submission_id, name, '
            'benchmark_id, owner_id, parameters, workflow_spec'
            ') VALUES(?, ?, ?, ?, ?, ?)'
        )
        params = (SUBMISSION_1, SUBMISSION_1, BENCHMARK_1, USER_ID, '[]', '{}')
        con.execute(sql, params)
        params = (SUBMISSION_2, SUBMISSION_2, BENCHMARK_1, USER_ID, '[]', '{}')
        con.execute(sql, params)
        params = (SUBMISSION_3, SUBMISSION_3, BENCHMARK_2, USER_ID, '[]', '{}')
        con.execute(sql, params)
        ranking.create_result_table(
            con=con,
            benchmark_id=BENCHMARK_1,
            schema=bm.BENCHMARK_SCHEMA,
            commit_changes=False
        )
        con.commit()
        repo = BenchmarkRepository(
            con=con,
            template_repo=TemplateRepository(basedir=basedir),
            resource_basedir=os.path.join(basedir, 'resources')
        )
        return con, repo

    def create_run(self, con, submission_id, values, basedir):
        """Create a successful run for the given submission with the given
        result values.
        """
        run = runstore.create_run(
            con=con,
            submission_id=submission_id,
            arguments=dict(),
            commit_changes=True
        )
        if values is not None:
            filename = os.path.join(basedir, 'results.json')
            util.write_object(obj=values, filename=filename)
            files = [
                FSObject(
                    resource_id='0',
                    resource_name=bm.RESULT_FILE_ID,
                    file_path=filename
                )
            ]
        else:
            files = dict()
        runstore.update_run(
            con=con,
            run_id=run.identifier,
            state=wfstate.StatePending().start().success(files=files),
            commit_changes=True
        )

    def test_get_leaderboard(self, tmpdir):
        """Test inserting results for submission runs and retrieving benchmark
        leaderboards.
        """
        con, repo = TestBenchmarkResultRanking.init(str(tmpdir))
        tmp_dir = str(tmpdir)
        self.create_run(con, SUBMISSION_1, {'col1': 1, 'col2': 10.7}, tmp_dir)
        self.create_run(con, SUBMISSION_1, {'col1': 5, 'col2': 1.3}, tmp_dir)
        self.create_run(con, SUBMISSION_1, {'col1': 10, 'col2': 1.3}, tmp_dir)
        self.create_run(con, SUBMISSION_2, {'col1': 7, 'col2': 12.7}, tmp_dir)
        self.create_run(con, SUBMISSION_2, {'col1': 3, 'col2': 8.3}, tmp_dir)
        sql = 'SELECT COUNT(*) FROM ' + ranking.RESULT_TABLE(BENCHMARK_1)
        r = con.execute(sql).fetchone()
        assert r[0] == 5
        # Sort by the default column col1 in descending order. Expects two
        # entries:
        # 1. SUBMISSION_1['col1'] = 10
        # 2. SUBMISSION_2['col1'] =  7
        results = repo.get_leaderboard(
            benchmark_id=BENCHMARK_1,
            order_by=None,
            include_all=False
        )
        assert results.size() == 2
        run_1 = results.get(0)
        assert run_1.submission_id == SUBMISSION_1
        assert run_1.get('col1') == 10
        run_2 = results.get(1)
        assert run_2.submission_id == SUBMISSION_2
        assert run_2.get('col1') == 7
        # Ensure that the result schema contains the three columns col1, col2,
        # and col3.
        names = results.names()
        assert len(names) == 3
        for col in ['col1', 'col2', 'col3']:
            assert col in names
        # Include all results. Expects 5 entries
        results = repo.get_leaderboard(
            benchmark_id=BENCHMARK_1,
            order_by=None,
            include_all=True
        )
        assert results.size() == 5
        # Sort by the col2 and col1 in ascending order. Expects two
        # entries:
        # 1. SUBMISSION_1['col1'] = 5
        # 2. SUBMISSION_2['col1'] = 3
        results = repo.get_leaderboard(
            benchmark_id=BENCHMARK_1,
            order_by=[SortColumn('col2', False), SortColumn('col1', False)],
            include_all=False
        )
        assert results.size() == 2
        run_1 = results.get(0)
        assert run_1.submission_id == SUBMISSION_1
        assert run_1.get('col1') == 5
        run_2 = results.get(1)
        assert run_2.submission_id == SUBMISSION_2
        assert run_2.get('col1') == 3
        # Sort by the col2 and col1 in ascending order. Include all results.
        # Expects five entries with the first two belonging to SUBMISSION 1:
        # 1. SUBMISSION_1['col1'] = 5
        # 2. SUBMISSION_1['col1'] = 10
        results = repo.get_leaderboard(
            benchmark_id=BENCHMARK_1,
            order_by=[SortColumn('col2', False), SortColumn('col1', False)],
            include_all=True
        )
        assert results.size() == 5
        run_1 = results.get(0)
        assert run_1.submission_id == SUBMISSION_1
        assert run_1.get('col1') == 5
        run_2 = results.get(1)
        assert run_2.submission_id == SUBMISSION_1
        assert run_2.get('col1') == 10
        # Error for queries that specify an unknown sort column
        with pytest.raises(err.InvalidSortColumnError):
            repo.get_leaderboard(
                benchmark_id=BENCHMARK_1,
                order_by=[SortColumn('col2', False), SortColumn('abc', False)]
            )

    def test_insert_results(self, tmpdir):
        """Test inserting results for submission runs."""
        con, _ = TestBenchmarkResultRanking.init(str(tmpdir))
        tmp_dir = str(tmpdir)
        self.create_run(con, SUBMISSION_1, {'col1': 1, 'col2': 10.7}, tmp_dir)
        self.create_run(con, SUBMISSION_1, {'col1': 5, 'col2': 5.5}, tmp_dir)
        self.create_run(con, SUBMISSION_1, None, tmp_dir)
        self.create_run(con, SUBMISSION_2, {'col1': 7, 'col2': 12.7}, tmp_dir)
        self.create_run(con, SUBMISSION_2, {'col1': 3, 'col2': 8.3}, tmp_dir)
        self.create_run(con, SUBMISSION_3, {'col1': 6, 'col2': 6.3}, tmp_dir)
        sql = 'SELECT COUNT(*) FROM ' + ranking.RESULT_TABLE(BENCHMARK_1)
        r = con.execute(sql).fetchone()
        assert r[0] == 4
