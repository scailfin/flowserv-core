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

from robcore.model.workflow.state import StatePending
from robcore.model.workflow.resource import FileResource
from robcore.tests.io import FakeStream

import robcore.error as err
import robcore.view.hateoas as hateoas
import robcore.view.labels as labels
import robcore.tests.api as api
import robcore.tests.serialize as serialize
import robcore.util as util
import robcore.model.workflow.state as wf


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Mandatory labels for run handles
RUN_LABELS = [labels.ID, labels.STATE, labels.CREATED_AT, labels.LINKS]
RUN_HANDLE = RUN_LABELS + [labels.ARGUMENTS, labels.PARAMETERS]
RUN_PENDING = RUN_HANDLE
RUN_RUNNING = RUN_PENDING + [labels.STARTED_AT]
RUN_ERROR = RUN_RUNNING + [labels.FINISHED_AT, labels.MESSAGES]
RUN_SUCCESS = RUN_RUNNING + [labels.FINISHED_AT, labels.RESOURCES]
RUN_LISTING = [labels.RUNS, labels.LINKS]

# Mandatory HATEOAS relationships in run descriptors
RELS_ACTIVE = [hateoas.SELF, hateoas.action(hateoas.CANCEL)]
RELS_INACTIVE = [hateoas.SELF, hateoas.action(hateoas.DELETE)]
RELS_LISTING = [hateoas.SELF, hateoas.SUBMIT]


class TestRunView(object):
    """Test API methods that execute, access and manipulate benchmark runs."""
    @staticmethod
    def init(base_dir):
        """Initialize the database, benchmark repository, submission manager,
        and the benchmark engine. Load one benchmark.

        Returns the run service, submission service, two user handles, and the
        handle for the created benchmark.
        """
        repository, submissions, user_service, runs, _ = api.init_api(base_dir)
        users = list()
        for i in range(2):
            user_id = util.get_unique_identifier()
            users.append(user_service.manager.register_user(user_id, user_id))
        bm = repository.repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        return runs, submissions, users, bm

    def test_cancel_and_delete_runs(self, tmpdir):
        """Test cancel and delete for submission runs."""
        runs, submissions, users, benchmark = TestRunView.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member and upload the names
        # input file file submission runs
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        f_id = submissions.upload_file(
            submission_id=submission_id,
            file=FakeStream(),
            file_name='names.txt',
            user=user
        )[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        run_id = r[labels.ID]
        # Cancel the pending run
        r = runs.cancel_run(run_id=run_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_ERROR)
        serialize.validate_links(r, RELS_INACTIVE)
        assert r[labels.STATE] == wf.STATE_CANCELED
        # Error when trying to delete a run without being a submission member
        user2 = users[1]
        with pytest.raises(err.UnauthorizedAccessError):
            runs.delete_run(run_id=run_id, user=user2)
        # Delete the run
        runs.delete_run(run_id=run_id, user=user)
        r = runs.list_runs(submission_id=submission_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_LISTING)
        serialize.validate_links(r, RELS_LISTING)
        assert len(r[labels.RUNS]) == 0

    def test_execute_run(self, tmpdir):
        """Test starting new runs for a submission."""
        runs, submissions, users, benchmark = TestRunView.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member and upload the names
        # input file file submission runs
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        f_id = submissions.upload_file(
            submission_id=submission_id,
            file=FakeStream(),
            file_name='names.txt',
            user=user
        )[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        util.validate_doc(doc=r, mandatory_labels=RUN_PENDING)
        serialize.validate_links(r, RELS_ACTIVE)
        # Start a new run in running state.
        runs.engine.backend.start()
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        util.validate_doc(doc=r, mandatory_labels=RUN_RUNNING)
        serialize.validate_links(r, RELS_ACTIVE)
        # Start a new run in error.
        runs.engine.backend.error(['Error'])
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        util.validate_doc(doc=r, mandatory_labels=RUN_ERROR)
        serialize.validate_links(r, RELS_INACTIVE)
        # Start a new run in running.
        values = {'max_len': 10, 'avg_count': 11.1}
        runs.engine.backend.success(values=values)
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        util.validate_doc(doc=r, mandatory_labels=RUN_SUCCESS)
        serialize.validate_links(r, RELS_INACTIVE)
        resources = r[labels.RESOURCES]
        assert len(resources) == 1
        res = resources[0]
        util.validate_doc(doc=res, mandatory_labels=[labels.ID, labels.NAME, labels.LINKS])
        assert res[labels.NAME] == 'results/analytics.json'
        serialize.validate_links(res, [hateoas.SELF])
        # Error when trying to start a run without being a submission member
        user2 = users[1]
        with pytest.raises(err.UnauthorizedAccessError):
            runs.start_run(
                submission_id,
                [{labels.ID: 'names', labels.VALUE: f_id}],
                user2
            )

    def test_get_run(self, tmpdir):
        """Test retrieving a run."""
        runs, submissions, users, benchmark = TestRunView.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member and upload the names
        # input file file submission runs
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        f_id = submissions.upload_file(
            submission_id=submission_id,
            file=FakeStream(),
            file_name='names.txt',
            user=user
        )[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        util.validate_doc(doc=r, mandatory_labels=RUN_PENDING)
        serialize.validate_links(r, RELS_ACTIVE)
        run_id = r[labels.ID]
        r = runs.get_run(run_id=run_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_PENDING)
        serialize.validate_links(r, RELS_ACTIVE)
        # Error when trying to access a run without being a submission member
        user2 = users[1]
        with pytest.raises(err.UnauthorizedAccessError):
            runs.get_run(run_id=run_id, user=user2)

    def test_list_runs(self, tmpdir):
        """Test retrieving a list of runs for a submission."""
        runs, submissions, users, benchmark = TestRunView.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member and upload the names
        # input file file submission runs
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        f_id = submissions.upload_file(
            submission_id=submission_id,
            file=FakeStream(),
            file_name='names.txt',
            user=user
        )[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state.
        runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        r = runs.list_runs(submission_id=submission_id, user=user)
        util.validate_doc(doc=r, mandatory_labels=RUN_LISTING)
        serialize.validate_links(r, RELS_LISTING)
        assert len(r[labels.RUNS]) == 1
        util.validate_doc(doc=r[labels.RUNS][0], mandatory_labels=RUN_LABELS)
        serialize.validate_links(r[labels.RUNS][0], RELS_ACTIVE)
        # Start a new run in running state.
        runs.engine.backend.start()
        runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        r = runs.list_runs(submission_id=submission_id, user=user)
        assert len(r[labels.RUNS]) == 2
        # Start a new run in error.
        runs.engine.backend.error(['Error'])
        runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id}],
            user
        )
        r = runs.list_runs(submission_id=submission_id, user=user)
        assert len(r[labels.RUNS]) == 3
        # Error when trying to list runs without being a submission member
        user2 = users[1]
        with pytest.raises(err.UnauthorizedAccessError):
            runs.list_runs(submission_id=submission_id, user=user2)

    def test_run_arguments(self, tmpdir):
        """Test converting run arguments for a submission."""
        runs, submissions, users, benchmark = TestRunView.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member and upload the names
        # input file file submission runs
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        submission_id = s[labels.ID]
        f_id = submissions.upload_file(
            submission_id=submission_id,
            file=FakeStream(),
            file_name='names.txt',
            user=user
        )[labels.ID]
        # Start a run where target path for the file parameter is given
        r = runs.start_run(
            submission_id,
            [{labels.ID: 'names', labels.VALUE: f_id, labels.AS: 'mynames.txt'}],
            user
        )
        assert r[labels.ARGUMENTS][0][labels.VALUE]['targetPath'] == 'mynames.txt'
        # Error conditions for lists of serialized run arguments
        # 1. Missing argument
        with pytest.raises(err.MissingArgumentError):
            runs.start_run(submission_id, list(), user)
        # 2. Invalid argument serialization
        with pytest.raises(err.InvalidArgumentError):
            runs.start_run(submission_id, list({'no': 'go'}), user)
        # 3. Duplicate argument
        with pytest.raises(err.DuplicateArgumentError):
            runs.start_run(
                submission_id,
                arguments=[
                    {labels.ID: 'names', labels.VALUE: f_id},
                    {labels.ID: 'names', labels.VALUE: f_id}
                ],
                user=user
            )
        # 4. Unknown parameter
        with pytest.raises(err.UnknownParameterError):
            runs.start_run(
                submission_id,
                arguments=[
                    {labels.ID: 'names', labels.VALUE: f_id},
                    {labels.ID: 'inputs', labels.VALUE: f_id}
                ],
                user=user
            )
