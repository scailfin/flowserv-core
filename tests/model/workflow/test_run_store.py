# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test store that maintains run information in a database."""

import os
import json
import pytest

from robcore.io.files import FileHandle, InputFile
from robcore.model.template.parameter.base import TemplateParameter
from robcore.model.template.schema import ResultColumn, ResultSchema
from robcore.model.workflow.resource import FileResource

import robcore.error as err
import robcore.model.ranking as ranking
import robcore.model.template.parameter.declaration as pd
import robcore.model.template.parameter.value as pv
import robcore.model.workflow.run as store
import robcore.tests.db as db
import robcore.util as util


"""Unique identifier for benchmarks."""
BID_SCHEMA = 'BID_SCHEMA'
BID_NOSCHEMA = 'BID_NOSCHEMA'


"""Unique identifier for submissions."""
SID_SCHEMA = 'SID_1'
SID_NOSCHEMA = 'SID_2'


"""Benchmark parameters and arguments."""
PARAMETERS={
    'A': TemplateParameter(pd.parameter_declaration('A', data_type=pd.DT_INTEGER)),
    'B': TemplateParameter(pd.parameter_declaration('B', data_type=pd.DT_BOOL)),
    'C': TemplateParameter(pd.parameter_declaration('C', data_type=pd.DT_FILE))
}
ARGS = pv.parse_arguments(
    arguments={'A': 10, 'B': True, 'C': FileHandle('dev/null')},
    parameters=PARAMETERS
)


"""Result schema for the default benchmark."""
RESULT_FILE_ID = 'results.json'
BENCHMARK_SCHEMA = ResultSchema(
    result_file_id=RESULT_FILE_ID,
    columns=[
        ResultColumn('col1', 'col1', pd.DT_INTEGER),
        ResultColumn('col2', 'col2', pd.DT_INTEGER)
    ]
)


class TestRunStore(object):
    """Unit tests for the run store methods and the run handle."""
    def init(self, base_dir):
        """Create an instance of the database with a single default user, two
        benchmarks and a submission for each benchmark.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) '
        sql += 'VALUES(?, ?, ?, ?)'
        USER_ID = 'ABC'
        con.execute(sql, (USER_ID, USER_ID, USER_ID, 1))
        sql = 'INSERT INTO benchmark(benchmark_id, name, result_schema) '
        sql += 'VALUES(?, ?, ?)'
        schema = json.dumps(BENCHMARK_SCHEMA.to_dict())
        con.execute(sql, (BID_SCHEMA, BID_SCHEMA, schema))
        ranking.create_result_table(
            con=con,
            benchmark_id=BID_SCHEMA,
            schema=BENCHMARK_SCHEMA,
            commit_changes=False
        )
        con.execute(sql, (BID_NOSCHEMA, BID_NOSCHEMA, None))
        sql = 'INSERT INTO benchmark_submission(submission_id, name, benchmark_id, owner_id) '
        sql += ' VALUES(?, ?, ?, ?)'
        con.execute(sql, (SID_SCHEMA, SID_SCHEMA, BID_SCHEMA, USER_ID))
        con.execute(sql, (SID_NOSCHEMA, SID_NOSCHEMA, BID_NOSCHEMA, USER_ID))
        con.commit()
        return con

    def test_cancel_run(self, tmpdir):
        """Test creating, running, and canceling a run."""
        con = self.init(str(tmpdir))
        # Create pending run for submission with result schema
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=ARGS
        )
        assert run.is_pending()
        run_id = run.identifier
        # Start run and then cancel it
        store.update_run(con=con, run_id=run_id, state=run.state.start())
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_running()
        # Cancel run without error message
        store.update_run(con=con, run_id=run_id, state=run.state.cancel())
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_canceled()
        assert len(run.state.messages) > 0
        # Cancel second run with error messages
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=ARGS
        )
        run_id = run.identifier
        store.update_run(
            con=con,
            run_id=run_id,
            state=run.state.cancel(['canceled', 'by', 'user'])
        )
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_canceled()
        assert run.state.messages == ['canceled', 'by', 'user']

    def test_create_run(self, tmpdir):
        """Test creating a new run that is in pending state."""
        con = self.init(str(tmpdir))
        # Create pending run for submission with result schema
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=ARGS
        )
        assert run.is_pending()
        util.validate_doc(
            doc=run.arguments['C'],
            mandatory_labels=['fileHandle', 'targetPath']
        )
        # All information should be commited by default
        con.rollback()
        run = store.get_run(con=con, run_id=run.identifier)
        assert run.is_pending()
        util.validate_doc(
            doc=run.arguments['C'],
            mandatory_labels=['fileHandle', 'targetPath']
        )
        # Create another run for the second submission. This time results can
        # be rolled back
        run = store.create_run(
            con=con,
            submission_id=SID_NOSCHEMA,
            arguments=ARGS,
            commit_changes=False
        )
        assert run.is_pending()
        util.validate_doc(
            doc=run.arguments['C'],
            mandatory_labels=['fileHandle', 'targetPath']
        )
        # All information should be commited by default
        con.rollback()
        with pytest.raises(err.UnknownRunError):
            run = store.get_run(con=con, run_id=run.identifier)

    def test_delete_run(self, tmpdir):
        """Test creating and deleting a workflow run."""
        con = self.init(str(tmpdir))
        # Create pending run for submission with result schema
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=ARGS
        )
        # Delete without commit will allow to roll back
        store.delete_run(con=con, run_id=run.identifier, commit_changes=False)
        with pytest.raises(err.UnknownRunError):
            run = store.get_run(con=con, run_id=run.identifier)
        con.rollback()
        run = store.get_run(con=con, run_id=run.identifier)
        assert run.is_pending()
        # By default changes are commited
        store.delete_run(con=con, run_id=run.identifier)
        with pytest.raises(err.UnknownRunError):
            run = store.get_run(con=con, run_id=run.identifier)
        con.rollback()
        with pytest.raises(err.UnknownRunError):
            run = store.get_run(con=con, run_id=run.identifier)

    def test_error_run(self, tmpdir):
        """Test creating and updating a run that ends in an error state."""
        con = self.init(str(tmpdir))
        # Create pending run for submission with result schema
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=ARGS
        )
        assert run.is_pending()
        run_id = run.identifier
        # Start run
        store.update_run(con=con, run_id=run_id, state=run.state.start())
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_running()
        # Set run into error state
        store.update_run(
            con=con,
            run_id=run_id,
            state=run.state.error(['there', 'was', 'an', 'error'])
        )
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_error()
        assert run.state.messages == ['there', 'was', 'an', 'error']

    def test_rollback_update(self, tmpdir):
        """Test update run state without commit."""
        con = self.init(str(tmpdir))
        # Create pending run for submission with result schema
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=pv.parse_arguments(
                arguments={'A': 10, 'B': True, 'C': FileHandle('dev/null')},
                parameters=PARAMETERS
            )
        )
        run_id = run.identifier
        # Update run without committing the changes
        store.update_run(
            con=con,
            run_id=run_id,
            state=run.state.start(),
            commit_changes=False
        )
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_running()
        con.rollback()
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_pending()

    def test_success_run(self, tmpdir):
        """Test setting runs into success state."""
        con = self.init(str(tmpdir))
        # Create a result file
        filename = os.path.join(str(tmpdir), 'data.json')
        util.write_object(obj={'col1': 10, 'col2': 20}, filename=filename)
        files = [
            FileResource(identifier=RESULT_FILE_ID, filename=filename),
            FileResource(identifier='X', filename='/dev/null')
        ]
        # Test runs for a submission with a result schema
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=pv.parse_arguments(
                arguments={'A': 10, 'B': True, 'C': FileHandle('dev/null')},
                parameters=PARAMETERS
            )
        )
        run_id = run.identifier
        # Update run to sucess state with a result file
        store.update_run(
            con=con,
            run_id=run_id,
            state=run.state.start().success(files=files),
            commit_changes=False
        )
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_success()
        assert len(run.state.files) == 2
        results = ranking.query(
            con=con,
            benchmark_id=BID_SCHEMA,
            schema=BENCHMARK_SCHEMA
        )
        assert results.size() == 1
        # Second run, this time without result files
        run = store.create_run(
            con=con,
            submission_id=SID_SCHEMA,
            arguments=pv.parse_arguments(
                arguments={'A': 10, 'B': True, 'C': FileHandle('dev/null')},
                parameters=PARAMETERS
            )
        )
        run_id = run.identifier
        # Update run to sucess state with a result file
        store.update_run(
            con=con,
            run_id=run_id,
            state=run.state.start().success(),
            commit_changes=False
        )
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_success()
        assert len(run.state.files) == 0
        results = ranking.query(
            con=con,
            benchmark_id=BID_SCHEMA,
            schema=BENCHMARK_SCHEMA,
            include_all=True
        )
        assert results.size() == 1
        # Create a run for the benchmark that does not have a result schema
        run = store.create_run(
            con=con,
            submission_id=SID_NOSCHEMA,
            arguments=pv.parse_arguments(
                arguments={'A': 10, 'B': True, 'C': FileHandle('dev/null')},
                parameters=PARAMETERS
            )
        )
        run_id = run.identifier
        # Update run to sucess state with a result file
        store.update_run(
            con=con,
            run_id=run_id,
            state=run.state.start().success(files=files),
            commit_changes=False
        )
        run = store.get_run(con=con, run_id=run_id)
        assert run.is_success()
        assert len(run.state.files) == 2
        results = ranking.query(
            con=con,
            benchmark_id=BID_SCHEMA,
            schema=BENCHMARK_SCHEMA,
            include_all=True
        )
        assert results.size() == 1
