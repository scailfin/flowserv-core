# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the benchmark engine."""

import os
import pytest

from passlib.hash import pbkdf2_sha256

from robapi.model.benchmark.engine import BenchmarkEngine
from robcore.model.template.benchmark.repo import BenchmarkRepository
from robapi.model.submission import SubmissionManager
from robcore.tests.benchmark import StateEngine
from robcore.io.files import FileHandle, InputFile
from robcore.model.template.parameter.base import TemplateParameter
from robcore.model.template.parameter.value import TemplateArgument
from robcore.model.template.schema import SortColumn
from robcore.model.template.repo.fs import TemplateFSRepository
from robcore.model.workflow.resource import FileResource
from robcore.model.workflow.state.base import StatePending, StateRunning

import robcore.error as err
import robcore.tests.benchmark as wf
import robcore.tests.db as db
import robcore.model.template.parameter.declaration as pd
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()
USER_3 = util.get_unique_identifier()


class TestBenchmarkEngine(object):
    """Unit tests for getting and setting run states. Uses a fake backend to
    simulate workflow execution.
    """
    def init(self, base_dir):
        """Create a fresh database with three users. Returns instances of the
        benchmark repository, benchmark engine and the submission manager.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        con.execute(sql, (USER_1, USER_1, pbkdf2_sha256.hash(USER_1), 1))
        con.execute(sql, (USER_2, USER_2, pbkdf2_sha256.hash(USER_2), 1))
        con.execute(sql, (USER_3, USER_3, pbkdf2_sha256.hash(USER_3), 0))
        con.commit()
        repo = BenchmarkRepository(
            con=con,
            template_repo=TemplateFSRepository(base_dir=base_dir)
        )
        engine = BenchmarkEngine(con=con, backend=StateEngine())
        submissions = SubmissionManager(con=con, directory=base_dir)
        return repo, engine, submissions

    def test_cancel_and_delete_run(self, tmpdir):
        """Test deleting runs."""
        # Initialize the repository, the benchmark engine, and the submission
        # manager
        repo, engine, submissions = self.init(str(tmpdir))
        # Add benchmark and create submission
        bm_1 = repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        submission = submissions.create_submission(
            benchmark_id=bm_1.identifier,
            name='A',
            user_id=USER_1
        )
        # Start a new run
        run = engine.start_run(
            submission_id=submission.identifier,
            template=bm_1.get_template(),
            source_dir='/dev/null',
            arguments=dict()
        )
        # Errors when trying to delete an active run
        with pytest.raises(err.InvalidRunStateError):
            engine.delete_run(run.identifier)
        # Cancel the run
        run = engine.cancel_run(run.identifier)
        assert run.is_canceled()
        # Error when trying to cancel an inactive run
        with pytest.raises(err.InvalidRunStateError):
            engine.cancel_run(run.identifier)
        # Delete the run
        engine.delete_run(run.identifier)
        # Error when deleting a non-existing run
        with pytest.raises(err.UnknownRunError):
            engine.delete_run(run.identifier)
        # Delete a successful run that created result files
        run = engine.start_run(
            submission_id=submission.identifier,
            template=bm_1.get_template(),
            source_dir='/dev/null',
            arguments=dict()
        )
        run = engine.update_run(run_id=run.identifier, state=run.state.start())
        result_file = os.path.join(str(tmpdir), 'run_result.json')
        util.write_object(
            filename=result_file,
            obj={'max_len': 1, 'avg_count': 2.1, 'max_line': 'R0'}
        )
        file_id = bm_1.get_template().get_schema().result_file_id
        files = {
            file_id:
            FileResource(identifier=file_id, filename=result_file)
        }
        state = run.state.success(files=files)
        engine.update_run(run_id=run.identifier, state=state)
        assert os.path.isfile(result_file)
        engine.delete_run(run.identifier)
        assert not os.path.isfile(result_file)

    def test_run_error(self, tmpdir):
        """Test state transitions when running a workflow that ends in an
        error state.
        """
        # Initialize the repository, the benchmark engine, and the submission
        # manager
        repo, engine, submissions = self.init(str(tmpdir))
        # Add benchmark and create submission
        bm_1 = repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        submission = submissions.create_submission(
            benchmark_id=bm_1.identifier,
            name='A',
            user_id=USER_1
        )
        # Run workflow using fake engine
        run = engine.start_run(
            submission_id=submission.identifier,
            template=bm_1.get_template(),
            source_dir='/dev/null',
            arguments=dict()
        )
        run_id = run.identifier
        # Start the run
        engine.update_run(run_id=run_id, state=run.state.start())
        run = engine.get_run(run_id=run_id)
        assert run.state.is_running()
        # Errors for illegal state transitions
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending())
        # Set run into error state
        messages = ['there', 'was', 'an', 'error']
        engine.update_run(
            run_id=run_id,
            state=run.state.error(messages=messages)
        )
        run = engine.get_run(run_id=run_id)
        assert run.state.is_error()
        assert run.state.messages == messages
        # Errors for illegal state transitions
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending())
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending().start())
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending().start().error())
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending().start().success())

    def test_run_results(self, tmpdir):
        """Test loading run results into the respective result table."""
        # Initialize the repository, the benchmark engine, and the submission
        # manager
        repo, engine, submissions = self.init(str(tmpdir))
        # Add benchmark and create submission
        bm_1 = repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        submission = submissions.create_submission(
            benchmark_id=bm_1.identifier,
            name='A',
            user_id=USER_1
        )
        # Run workflow for different result sets
        values = [
            {'max_len': 1, 'avg_count': 1.1, 'max_line': 'R0'},
            {'max_len': 2, 'avg_count': 2.1},
        ]
        for vals in values:
            wf.run_workflow(
                engine=engine,
                template=bm_1.get_template(),
                submission_id=submission.identifier,
                base_dir=str(tmpdir),
                values=vals
            )
        values = [
            {'max_len': 1, 'max_line': 'R0'},
            {'avg_count': 2.1},
        ]
        with pytest.raises(err.ConstraintViolationError):
            for vals in values:
                wf.run_workflow(
                    engine=engine,
                    template=bm_1.get_template(),
                    submission_id=submission.identifier,
                    base_dir=str(tmpdir),
                    values=vals
                )

    def test_run_success(self, tmpdir):
        """Test state transitions when running a workflow that ends in a
        success state.
        """
        # Initialize the repository, the benchmark engine, and the submission
        # manager
        repo, engine, submissions = self.init(str(tmpdir))
        # Add benchmark and create submission
        bm_1 = repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        submission = submissions.create_submission(
            benchmark_id=bm_1.identifier,
            name='A',
            user_id=USER_1
        )
        # Run workflow using fake engine
        f1 = InputFile(
            f_handle=FileHandle(
                filepath='source/file1.txt',
                identifier='F1',
                file_name='myfile.txt'
            ),
            target_path='target/file1.txt'
        )
        p_name = TemplateParameter(pd.parameter_declaration(identifier='name'))
        p_file = TemplateParameter(
            pd.parameter_declaration(identifier='file', data_type=pd.DT_FILE)
        )
        run = engine.start_run(
            submission_id=submission.identifier,
            template=bm_1.get_template(),
            source_dir='/dev/null',
            arguments={
                'name': TemplateArgument(parameter=p_name, value='MyName'),
                'file': TemplateArgument(parameter=p_file, value=f1)
            }
        )
        run_id = run.identifier
        assert run.state.is_pending()
        util.validate_doc(run.arguments, mandatory_labels=['name', 'file'])
        util.validate_doc(
            run.arguments['file'],
            mandatory_labels=['fileHandle', 'targetPath']
        )
        util.validate_doc(
            run.arguments['file']['fileHandle'],
            mandatory_labels=['filepath', 'identifier', 'filename']
        )
        run = engine.get_run(run_id=run_id)
        util.validate_doc(run.arguments, mandatory_labels=['name', 'file'])
        util.validate_doc(
            run.arguments['file'],
            mandatory_labels=['fileHandle', 'targetPath']
        )
        util.validate_doc(
            run.arguments['file']['fileHandle'],
            mandatory_labels=['filepath', 'identifier', 'filename']
        )
        assert run.state.is_pending()
        engine.update_run(run_id=run_id, state=run.state.start())
        run = engine.get_run(run_id=run_id)
        assert run.state.is_running()
        result_file = os.path.join(str(tmpdir), 'run_result.json')
        util.write_object(
            filename=result_file,
            obj={'max_len': 1, 'avg_count': 2.1, 'max_line': 'R0'}
        )
        file_id = bm_1.get_template().get_schema().result_file_id
        files = {
            file_id:
            FileResource(identifier=file_id, filename=result_file)
        }
        state = run.state.success(files=files)
        engine.update_run(run_id=run_id, state=state)
        run = engine.get_run(run_id=run_id)
        assert run.state.is_success()
        assert 'results/analytics.json' in run.state.files
        # Errors for illegal state transitions
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending())
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending().start())
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending().start().error())
        with pytest.raises(err.InvalidRunStateError):
            engine.update_run(run_id=run_id, state=StatePending().start().success())
