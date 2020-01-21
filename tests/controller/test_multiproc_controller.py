# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the asynchronous multiprocess workflow controller."""

import json
import os
import time

from flowserv.core.files import FileHandle
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.parameter.value import TemplateArgument
from flowserv.controller.backend.multiproc import MultiProcessWorkflowEngine

from flowserv.controller.engine import BenchmarkEngine

import flowserv.model.ranking as ranking
import flowserv.tests.db as db
import flowserv.core.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')
# Workflow templates
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './benchmark.yaml')
# Input files
NAMES_TXT = '../../template/inputs/short-names.txt'
NAMES_FILE = os.path.join(TEMPLATE_DIR, NAMES_TXT)
UNKNOWN_FILE = os.path.join(TEMPLATE_DIR, './tmp/no/file/here')


SUBMISSION_ID = util.get_unique_identifier()


class TestMultiProcessWorkflowEngine(object):
    """Unit test for the asynchronous workflow controler."""
    @staticmethod
    def init(base_dir):
        """Create a fresh database with a single user, single benchmark, and
        a single submission. The benchmark is the 'Hello World' example.
        Returns an instance of the benchmark engine with a multi-process
        backend controller and the created template handle.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) '
        sql += 'VALUES(?, ?, ?, ?)'
        USER_ID = util.get_unique_identifier()
        con.execute(sql, (USER_ID, USER_ID, USER_ID, 1))
        BENCHMARK_ID = util.get_unique_identifier()
        sql = 'INSERT INTO benchmark(benchmark_id, name, result_schema) '
        sql += 'VALUES(?, ?, ?)'
        doc = util.read_object(filename=TEMPLATE_HELLOWORLD)
        template = WorkflowTemplate.from_dict(doc, source_dir=TEMPLATE_DIR)
        schema = json.dumps(template.get_schema().to_dict())
        con.execute(sql, (BENCHMARK_ID, BENCHMARK_ID, schema))
        sql = (
            'INSERT INTO benchmark_submission(submission_id, name, '
            'benchmark_id, owner_id, parameters, workflow_spec'
            ') VALUES(?, ?, ?, ?, ?, ?)'
        )
        params = [p.to_dict() for p in template.parameters.values()]
        con.execute(
            sql,
            (
                SUBMISSION_ID,
                SUBMISSION_ID,
                BENCHMARK_ID,
                USER_ID,
                json.dumps(params),
                json.dumps(template.workflow_spec)
            )
        )
        ranking.create_result_table(
            con=con,
            benchmark_id=BENCHMARK_ID,
            schema=template.get_schema(),
            commit_changes=False
        )
        con.commit()
        engine = BenchmarkEngine(
            con=con,
            backend=MultiProcessWorkflowEngine(base_dir=base_dir, verbose=True)
        )
        return engine, template

    def test_cancel_and_delete_run(self, tmpdir):
        """Execute the helloworld example."""
        # Read the workflow template
        engine, template = TestMultiProcessWorkflowEngine.init(str(tmpdir))
        # Set the template argument values
        arguments = {
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(NAMES_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=30
            )
        }
        # Run the workflow
        run = engine.start_run(
            submission_id=SUBMISSION_ID,
            template=template,
            arguments=arguments
        )
        # Sleep and pool once
        while run.is_active():
            time.sleep(1)
            run = engine.get_run(run.identifier)
            break
        assert run.is_active()
        run = engine.cancel_run(run.identifier, reason='done testing')
        assert run.is_canceled()
        assert run.state.messages[0] == 'done testing'
        run = engine.get_run(run.identifier)
        assert run.is_canceled()
        assert run.state.messages[0] == 'done testing'
        engine.delete_run(run.identifier)

    def test_run_helloworld(self, tmpdir):
        """Execute the helloworld example."""
        # Read the workflow template
        engine, template = TestMultiProcessWorkflowEngine.init(str(tmpdir))
        # Set the template argument values
        arguments = {
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(NAMES_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=3
            ),
            'greeting': TemplateArgument(
                parameter=template.get_parameter('greeting'),
                value='Hi'
            )
        }
        # Run the workflow
        run = engine.start_run(
            submission_id=SUBMISSION_ID,
            template=template,
            arguments=arguments
        )
        # Poll workflow state every second.
        while run.is_active():
            time.sleep(1)
            run = engine.get_run(run.identifier)
        assert run.is_success()
        with open(run.get_resource('results/greetings.txt').filename) as f:
            greetings = f.read()
            assert 'Hi Alice' in greetings
            assert 'Hi Bob' in greetings
        result_file = run.get_resource('results/analytics.json').filename
        assert os.path.isfile(result_file)

    def test_run_with_missing_file(self, tmpdir):
        """Execute the helloworld example with a missing file that will case
        an error when copying the input files for the workflow run.
        """
        # Read the workflow template
        engine, template = TestMultiProcessWorkflowEngine.init(str(tmpdir))
        # Set the template argument values
        arguments = {
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(UNKNOWN_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=3
            )
        }
        # Run the workflow
        run = engine.start_run(
            submission_id=SUBMISSION_ID,
            template=template,
            arguments=arguments
        )
        while run.is_active():
            time.sleep(1)
            run = engine.get_run(run.identifier)
        assert run.is_error()
        assert 'No such file or directory' in run.state.messages[0]
