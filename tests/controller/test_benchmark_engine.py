# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the benchmark engine."""

import json
import os
import pytest

from robcore.model.template.base import WorkflowTemplate
from robcore.controller.engine import BenchmarkEngine

import robcore.core.error as err
import robcore.model.ranking as ranking
import robcore.tests.benchmark as bm
import robcore.tests.db as db
import robcore.core.util as util


SUBMISSION_1 = util.get_unique_identifier()
SUBMISSION_2 = util.get_unique_identifier()


class TestBenchmarkEngine(object):
    """Unit tests for getting and setting run states. Uses a fake backend to
    simulate workflow execution.
    """
    @staticmethod
    def init(base_dir):
        """Create a fresh database with a single user, single benchmark, and
        two submissions. Returns an instance of the benchmark engine and the
        created template handle.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) '
        sql += 'VALUES(?, ?, ?, ?)'
        USER_ID = util.get_unique_identifier()
        con.execute(sql, (USER_ID, USER_ID, USER_ID, 1))
        BID = util.get_unique_identifier()
        sql = 'INSERT INTO benchmark(benchmark_id, name, result_schema) '
        sql += 'VALUES(?, ?, ?)'
        schema = json.dumps(bm.BENCHMARK_SCHEMA.to_dict())
        con.execute(sql, (BID, BID, schema))
        sql = (
            'INSERT INTO benchmark_submission(submission_id, name, '
            'benchmark_id, owner_id, parameters, workflow_spec'
            ') VALUES(?, ?, ?, ?, ?, ?)'
        )
        params = (SUBMISSION_1, SUBMISSION_1, BID, USER_ID, '[]', '{}')
        con.execute(sql, params)
        params = (SUBMISSION_2, SUBMISSION_2, BID, USER_ID, '[]', '{}')
        con.execute(sql, params)
        ranking.create_result_table(
            con=con,
            benchmark_id=BID,
            schema=bm.BENCHMARK_SCHEMA,
            commit_changes=False
        )
        con.commit()
        template = WorkflowTemplate(
            workflow_spec=dict(),
            source_dir='dev/null',
            result_schema=bm.BENCHMARK_SCHEMA
        )
        engine = BenchmarkEngine(
            con=con,
            backend=bm.StateEngine(base_dir=base_dir)
        )
        return engine, template

    def test_cancel_and_delete_run(self, tmpdir):
        """Test deleting runs."""
        # Initialize the database and benchmark engine
        engine, template = TestBenchmarkEngine.init(str(tmpdir))
        # Start a new run for each of the two submissions
        run1 = engine.start_run(
            submission_id=SUBMISSION_1,
            arguments=dict(),
            template=template
        )
        assert len(run1.list_resources()) == 0
        assert run1.get_resource(bm.RESULT_FILE_ID) is None
        assert engine.exists_run(run1.identifier)
        engine.start_run(
            submission_id=SUBMISSION_2,
            arguments=dict(),
            template=template
        )
        assert len(engine.list_runs(SUBMISSION_1)) == 1
        assert len(engine.list_runs(SUBMISSION_2)) == 1
        # Errors when trying to delete an active run
        with pytest.raises(err.InvalidRunStateError):
            engine.delete_run(run1.identifier)
        # Cancel the run
        run1 = engine.cancel_run(run1.identifier)
        assert run1.is_canceled()
        assert engine.exists_run(run1.identifier)
        # Error when trying to cancel an inactive run
        with pytest.raises(err.InvalidRunStateError):
            engine.cancel_run(run1.identifier)
        # Delete the run
        engine.delete_run(run1.identifier)
        assert len(engine.list_runs(SUBMISSION_1)) == 0
        assert len(engine.list_runs(SUBMISSION_2)) == 1
        assert not engine.exists_run(run1.identifier)
        # Error when deleting a non-existing run
        with pytest.raises(err.UnknownRunError):
            engine.delete_run(run1.identifier)
        # Error when getting a non-existing run
        with pytest.raises(err.UnknownRunError):
            engine.get_run(run1.identifier)
        # Delete a successful run that created result files
        engine.backend.success()
        run = engine.start_run(
            submission_id=SUBMISSION_1,
            arguments=dict(),
            template=template
        )
        assert len(run.list_resources()) == 1
        assert not run.get_resource(bm.RESULT_FILE_ID) is None
        assert len(engine.list_runs(SUBMISSION_1)) == 1
        assert len(engine.list_runs(SUBMISSION_2)) == 1
        assert run.is_success()
        result_file = run.state.get_resource(bm.RESULT_FILE_ID).filename
        assert os.path.isfile(result_file)
        engine.delete_run(run.identifier)
        assert not os.path.isfile(result_file)
        assert len(engine.list_runs(SUBMISSION_1)) == 0
        assert len(engine.list_runs(SUBMISSION_2)) == 1

    def test_run_error(self, tmpdir):
        """Test state transitions when running a workflow that ends in an
        error state.
        """
        # Initialize the database and benchmark engine
        engine, template = TestBenchmarkEngine.init(str(tmpdir))
        # Create a run that is in error state
        messages = ['there', 'was', 'an', 'error']
        engine.backend.error(messages=messages)
        run = engine.start_run(
            submission_id=SUBMISSION_1,
            arguments=dict(),
            template=template
        )
        assert run.state.is_error()
        assert run.state.messages == messages
        run = engine.get_run(run.identifier)
        assert run.state.is_error()
        assert run.state.messages == messages
        # Errors for illegal state transitions
        with pytest.raises(err.InvalidRunStateError):
            engine.cancel_run(run_id=run.identifier)
        run = engine.delete_run(run.identifier)
        assert run.state.is_error()
        # Error when getting a non-existing run
        with pytest.raises(err.UnknownRunError):
            engine.get_run(run.identifier)

    def test_run_results(self, tmpdir):
        """Test successful runs with different results."""
        # Initialize the database and benchmark engine
        engine, template = TestBenchmarkEngine.init(str(tmpdir))
        # Run workflow for different result sets. All value sets are valid
        # and should not raise an exception.
        values = [
            {'col1': 1, 'col2': 1.1, 'col3': 'R0'},
            {'col1': 2, 'col2': 2.1},
        ]
        for vals in values:
            engine.backend.success(values=vals)
            engine.start_run(
                submission_id=SUBMISSION_1,
                arguments=dict(),
                template=template
            )
        # Run workflow for different result sets. All value sets are invalid
        # and should raise an exception.
        values = [
            {'col1': 1, 'col3': 'R0'},
            {'col2': 2.1},
        ]
        for vals in values:
            engine.backend.success(values=vals)
            with pytest.raises(err.ConstraintViolationError):
                engine.start_run(
                    submission_id=SUBMISSION_1,
                    arguments=dict(),
                    template=template
                )
