# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test API methods for benchmark runs."""

import os
import pytest

from passlib.hash import pbkdf2_sha256

from robcore.model.user.auth import DefaultAuthPolicy
from robapi.model.submission import SubmissionManager
from robapi.model.benchmark.engine import BenchmarkEngine
from robcore.model.template.benchmark.repo import BenchmarkRepository
from robcore.model.user.base import UserManager
from robapi.service.benchmark import BenchmarkService
from robapi.service.run import RunService
from robapi.service.submission import SubmissionService
from robcore.tests.benchmark import StateEngine
from robcore.tests.io import FakeStream
from robcore.model.template.repo.fs import TemplateFSRepository
from robcore.model.workflow.state.base import StatePending
from robcore.model.workflow.resource import FileResource

import robcore.error as err
import robapi.serialize.hateoas as hateoas
import robapi.serialize.labels as labels
import robcore.tests.db as db
import robcore.tests.serialize as serialize
import robcore.util as util
import robcore.model.workflow.state.base as wf


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default benchmark users
USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()

# Mandatory labels for run handles
RUN_LABELS = [labels.ID, labels.STATE, labels.CREATED_AT, labels.LINKS]
RUN_HANDLE = RUN_LABELS + [labels.ARGUMENTS]
RUN_PENDING = RUN_HANDLE
RUN_RUNNING = RUN_PENDING + [labels.STARTED_AT]
RUN_ERROR = RUN_RUNNING + [labels.FINISHED_AT, labels.MESSAGES]
RUN_SUCCESS = RUN_RUNNING + [labels.FINISHED_AT]
RUN_LISTING = [labels.RUNS, labels.LINKS]

# Mandatory HATEOAS relationships in run descriptors
RELS_ACTIVE = [hateoas.SELF, hateoas.action(hateoas.CANCEL)]
RELS_INACTIVE = [hateoas.SELF, hateoas.action(hateoas.DELETE)]
RELS_LISTING = [hateoas.SELF, hateoas.SUBMIT]


class TestRunApi(object):
    """Test API methods that execute, access and manipulate benchmark runs."""
    def init(self, base_dir):
        """Initialize the database, benchmark repository, submission manager,
        and the benchmark engine. Load one benchmark.

        Returns the run service, submission service, user service, the handle
        for the created benchmark.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        for user_id in [USER_1, USER_2]:
            con.execute(sql, (user_id, user_id, pbkdf2_sha256.hash(user_id), 1))
        repo = BenchmarkRepository(
            con=con,
            template_repo=TemplateFSRepository(base_dir=base_dir)
        )
        bm = repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        auth = DefaultAuthPolicy(con=con)
        submissions = SubmissionManager(con=con, directory=base_dir)
        return (
             RunService(
                engine=BenchmarkEngine(con=con, backend=StateEngine()),
                submissions=submissions,
                repo=repo,
                auth=auth
            ),
            SubmissionService(manager=submissions, auth=auth),
            UserManager(con=con),
            bm
        )

    def test_cancel_and_delete_runs(self, tmpdir):
        """Test cancel and delete for submission runs."""
        runs, submissions, users, benchmark = self.init(str(tmpdir))
        # Get handle for USER_1
        user = users.login_user(USER_1, USER_1)
        # Create new submission with a single member
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        r = runs.start_run(submission_id, dict(), user)
        run_id = r[labels.ID]
        # Cancel the pending run
        r = runs.cancel_run(run_id=run_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_ERROR)
        serialize.validate_links(r, RELS_INACTIVE)
        assert r[labels.STATE] == wf.STATE_CANCELED
        # Error when trying to delete a run without being a submission member
        user2 = users.login_user(USER_2, USER_2)
        with pytest.raises(err.UnauthorizedAccessError):
            runs.delete_run(run_id=run_id, user=user2)
        # Delete the run
        r = runs.delete_run(run_id=run_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_LISTING)
        serialize.validate_links(r, RELS_LISTING)
        assert len(r[labels.RUNS]) == 0

    def test_execute_run(self, tmpdir):
        """Test starting new runs for a submission."""
        runs, submissions, users, benchmark = self.init(str(tmpdir))
        # Get handle for USER_1
        user = users.login_user(USER_1, USER_1)
        # Create new submission with a single member
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        r = runs.start_run(submission_id, dict(), user)
        util.validate_doc(doc=r, mandatory_labels=RUN_PENDING)
        serialize.validate_links(r, RELS_ACTIVE)
        # Start a new run in running state.
        runs.engine.backend.state = StatePending().start()
        r = runs.start_run(submission_id, dict(), user)
        util.validate_doc(doc=r, mandatory_labels=RUN_RUNNING)
        serialize.validate_links(r, RELS_ACTIVE)
        # Start a new run in error.
        runs.engine.backend.state = StatePending().start().error(['Error'])
        r = runs.start_run(submission_id, dict(), user)
        util.validate_doc(doc=r, mandatory_labels=RUN_ERROR)
        serialize.validate_links(r, RELS_INACTIVE)
        # Start a new run in running.
        result_file = os.path.join(str(tmpdir), 'run_result.json')
        values = {'max_len': 10, 'avg_count': 11.1}
        util.write_object(filename=result_file, obj=values)
        file_id = benchmark.template.get_schema().result_file_id
        files = {file_id: FileResource(identifier=file_id, filename=result_file)}
        runs.engine.backend.state = StatePending().start().success(files=files)
        r = runs.start_run(submission_id, dict(), user)
        util.validate_doc(doc=r, mandatory_labels=RUN_SUCCESS)
        serialize.validate_links(r, RELS_INACTIVE)
        # Error when trying to start a run without being a submission member
        user2 = users.login_user(USER_2, USER_2)
        with pytest.raises(err.UnauthorizedAccessError):
            runs.start_run(submission_id, dict(), user2)

    def test_get_run(self, tmpdir):
        """Test retrieving a run."""
        runs, submissions, users, benchmark = self.init(str(tmpdir))
        # Get handle for USER_1
        user = users.login_user(USER_1, USER_1)
        # Create new submission with a single member
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        r = runs.start_run(submission_id, dict(), user)
        util.validate_doc(doc=r, mandatory_labels=RUN_PENDING)
        serialize.validate_links(r, RELS_ACTIVE)
        run_id = r[labels.ID]
        r = runs.get_run(run_id=run_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_PENDING)
        serialize.validate_links(r, RELS_ACTIVE)
        # Error when trying to access a run without being a submission member
        user2 = users.login_user(USER_2, USER_2)
        with pytest.raises(err.UnauthorizedAccessError):
            runs.get_run(run_id=run_id, user=user2)

    def test_list_runs(self, tmpdir):
        """Test retrieving a list of runs for a submission."""
        runs, submissions, users, benchmark = self.init(str(tmpdir))
        # Get handle for USER_1
        user = users.login_user(USER_1, USER_1)
        # Create new submission with a single member
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        runs.start_run(submission_id, dict(), user)
        r = runs.list_runs(submission_id=submission_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_LISTING)
        serialize.validate_links(r, RELS_LISTING)
        assert len(r[labels.RUNS]) == 1
        util.validate_doc(doc=r[labels.RUNS][0], mandatory_labels=RUN_LABELS)
        serialize.validate_links(r[labels.RUNS][0], RELS_ACTIVE)
        # Start a new run in running state.
        runs.engine.backend.state = StatePending().start()
        runs.start_run(submission_id, dict(), user)
        r = runs.list_runs(submission_id=submission_id, user=user)
        assert len(r[labels.RUNS]) == 2
        # Start a new run in error.
        runs.engine.backend.state = StatePending().start().error(['Error'])
        runs.start_run(submission_id, dict(), user)
        r = runs.list_runs(submission_id=submission_id, user=user)
        assert len(r[labels.RUNS]) == 3
        # Error when trying to list runs without being a submission member
        user2 = users.login_user(USER_2, USER_2)
        with pytest.raises(err.UnauthorizedAccessError):
            runs.list_runs(submission_id=submission_id, user=user2)
