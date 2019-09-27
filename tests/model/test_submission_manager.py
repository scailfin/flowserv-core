# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the submission manager."""

import os
import pytest

from passlib.hash import pbkdf2_sha256

from robcore.model.submission import SubmissionManager
from robcore.model.template.benchmark.repo import BenchmarkRepository
from robcore.model.template.repo.fs import TemplateFSRepository
from robcore.model.user.base import UserHandle
from robcore.model.workflow.engine import BenchmarkEngine
from robcore.model.workflow.state import StatePending
from robcore.tests.benchmark import StateEngine

import robcore.error as err
import robcore.tests.benchmark as wf
import robcore.tests.db as db
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')
TEMPLATE_WITHOUT_SCHEMA = os.path.join(DIR, '../.files/templates/template.json')


USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()
USER_3 = util.get_unique_identifier()


class TestSubmissionManager(object):
    """Unit tests for managing submissions and team members through the
    submission manager.
    """
    def init(self, base_dir):
        """Create a fresh database with three users and a template repository
        with a single entry. Returns an tuple containing an instance of the
        submission manager and the handle for the created benchmark.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        con.execute(sql, (USER_1, USER_1, pbkdf2_sha256.hash(USER_1), 1))
        con.execute(sql, (USER_2, USER_2, pbkdf2_sha256.hash(USER_2), 1))
        con.execute(sql, (USER_3, USER_3, pbkdf2_sha256.hash(USER_3), 0))
        con.commit()
        bm = BenchmarkRepository(
            con=con,
            template_repo=TemplateFSRepository(base_dir=str(base_dir))
        ).add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        submissions = SubmissionManager(
            con=con,
            directory=base_dir,
            engine=BenchmarkEngine(con=con, backend=StateEngine()
        )
        return submissions, bm

    def test_create_submission(self, tmpdir):
        """Test creating a submission."""
        # Initialize the repository and the benchmark
        manager, bm, _ = self.init(str(tmpdir))
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        assert submission.name == 'A'
        assert submission.owner_id == USER_1
        assert len(submission.members) == 1
        assert submission.has_member(USER_1)
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='B',
            user_id=USER_2,
            members=[USER_1, USER_3]
        )
        assert submission.owner_id == USER_2
        assert len(submission.members) == 3
        for user_id in [USER_1, USER_2, USER_3]:
            assert submission.has_member(user_id)
        # Duplicate users in member list
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='C',
            user_id=USER_3,
            members=[USER_1, USER_3, USER_1, USER_3]
        )
        for user_id in [USER_1, USER_3]:
            assert submission.has_member(user_id)
        assert not submission.has_member(USER_2)
        # Error conditions
        # - Unknown benchmark
        with pytest.raises(err.UnknownBenchmarkError):
            manager.create_submission(
                benchmark_id='UNK',
                name='C',
                user_id=USER_1
            )
        # - Invalid name
        with pytest.raises(err.ConstraintViolationError):
            manager.create_submission(
                benchmark_id=bm.identifier,
                name='A' * 513,
                user_id=USER_1
            )

    def test_delete_submission(self, tmpdir):
        """Test creating and deleting submissions."""
        # Initialize the repository and the benchmark
        manager, bm = self.init(str(tmpdir))
        # Create a new submission with two run results
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        wf.run_workflow(
            engine=engine,
            template=bm.get_template(),
            submission_id=submission.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 1, 'avg_count': 1.1, 'max_line': 'R0'}
        )
        wf.run_workflow(
            engine=engine,
            template=bm.get_template(),
            submission_id=submission.identifier,
            base_dir=str(tmpdir),
            values={'max_len': 1, 'avg_count': 1.1, 'max_line': 'R0'}
        )
        submission = manager.get_submission(submission.identifier)
        filenames = list()
        for run in submission.get_runs():
            for fh in run.get_files():
                filenames.append(fh.filename)
                assert os.path.isfile(fh.filename)
        manager.delete_submission(submission.identifier)
        for f in filenames:
            assert not os.path.isfile(f)
        with pytest.raises(err.UnknownSubmissionError):
            manager.get_submission(submission.identifier)
        with pytest.raises(err.UnknownSubmissionError):
            manager.delete_submission(submission.identifier)

    def test_get_runs(self, tmpdir):
        """Test retrieving list of submission runs."""
        # Initialize the repository and the benchmark
        manager, bm, engine = self.init(str(tmpdir))
        # Create two submissions
        s1 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        s2 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='B',
            user_id=USER_1
        )
        # Add two runs to submission 1 and set them into running state
        engine.backend.state = StatePending().start()
        engine.start_run(
            submission_id=s1.identifier,
            template=bm.get_template(),
            source_dir='/dev/null',
            arguments=dict()
        )
        engine.start_run(
            submission_id=s1.identifier,
            template=bm.get_template(),
            source_dir='/dev/null',
            arguments=dict()
        )
        # Add one run to submission 2 that is in pending state
        engine.backend.state = StatePending()
        engine.start_run(
            submission_id=s2.identifier,
            template=bm.get_template(),
            source_dir='/dev/null',
            arguments=dict()
        )
        # Ensure that submission 1 has two runs in running state
        submission = manager.get_submission(s1.identifier)
        runs = submission.get_runs()
        assert len(runs) == 2
        for run in runs:
            assert run.is_running()
        # Ensure that submission 2 has one run in pending state
        submission = manager.get_submission(s2.identifier)
        runs = submission.get_runs()
        assert len(runs) == 1
        for run in runs:
            assert run.is_pending()
        # Error when accessing an unknown submission
        with pytest.raises(err.UnknownSubmissionError):
            manager.get_runs('UNK')

    def test_get_submission(self, tmpdir):
        """Test creating and retrieving submissions."""
        # Initialize the repository and the benchmark
        manager, bm, _ = self.init(str(tmpdir))
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        submission = manager.get_submission(submission.identifier)
        assert not submission.members is None
        assert submission.name == 'A'
        assert submission.owner_id == USER_1
        assert len(submission.members) == 1
        assert submission.has_member(USER_1)
        # If the load members flag is false submission members have to be
        # loaded on demand
        submission = manager.get_submission(
            submission.identifier,
            load_members=False
        )
        assert submission.members is None
        assert not submission.get_members() is None
        assert not submission.members is None
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='B',
            user_id=USER_2,
            members=[USER_1, USER_3]
        )
        submission = manager.get_submission(submission.identifier)
        assert submission.name == 'B'
        assert submission.owner_id == USER_2
        assert len(submission.members) == 3
        for user_id in [USER_1, USER_2, USER_3]:
            assert submission.has_member(user_id)
        # Error when accessing an unknown submission
        with pytest.raises(err.UnknownSubmissionError):
            manager.get_submission('UNK')

    def test_list_submission(self, tmpdir):
        """Test listing submissions."""
        # Initialize the repository and the benchmark
        manager, bm, _ = self.init(str(tmpdir))
        # Create three submissions. USER_1 is member of all three submissions.
        # USER_2 and USER_3 are member of one submissions
        s1 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        s2 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='B',
            user_id=USER_1,
            members=[USER_2]
        )
        s3 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='C',
            user_id=USER_3,
            members=[USER_1]
        )
        user_1 = UserHandle(identifier=USER_1, name=USER_1)
        user_2 = UserHandle(identifier=USER_2, name=USER_2)
        user_3 = UserHandle(identifier=USER_3, name=USER_3)
        assert len(manager.list_submissions()) == 3
        assert len(manager.list_submissions(user=user_1)) == 3
        assert len(manager.list_submissions(user=user_2)) == 1
        submissions3 = manager.list_submissions(user=user_3)
        assert len(submissions3) == 1
        members3 = [m.identifier for m in submissions3[0].get_members()]
        assert USER_1 in members3
        assert not USER_2 in members3
        assert USER_3 in members3

    def test_submission_membership(self, tmpdir):
        """Test adding and removing submission members."""
        # Initialize the repository and the benchmark
        manager, bm, _ = self.init(str(tmpdir))
        submission = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        s_id = submission.identifier
        submission = manager.get_submission(s_id)
        assert len(submission.members) == 1
        manager.add_member(submission_id=s_id, user_id=USER_2)
        submission = manager.get_submission(s_id)
        assert len(submission.members) == 2
        with pytest.raises(err.ConstraintViolationError):
            manager.add_member(submission_id=s_id, user_id=USER_2)
        assert manager.remove_member(submission_id=s_id, user_id=USER_2)
        manager.add_member(submission_id=s_id, user_id=USER_2)
        submission = manager.get_submission(s_id)
        assert len(submission.members) == 2
        for user_id in [USER_1, USER_2]:
            assert submission.has_member(user_id)
        assert not submission.has_member(USER_3)
        assert manager.remove_member(submission_id=s_id, user_id=USER_1)
        assert not manager.remove_member(submission_id=s_id, user_id=USER_1)
        submission = manager.get_submission(s_id)
        assert len(submission.members) == 1
        assert submission.has_member(USER_2)
        assert manager.remove_member(submission_id=s_id, user_id=USER_2)
        # Cannot access a submission without a member
        submission = manager.get_submission(s_id)
        assert len(submission.members) == 0
        with pytest.raises(err.UnknownSubmissionError):
            manager.list_members(s_id, raise_error=True)

    def test_update_submission(self, tmpdir):
        """Test updating submission name and member list."""
        # Initialize the repository and the benchmark
        manager, bm, _ = self.init(str(tmpdir))
        # Add two submissions
        s1 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user_id=USER_1
        )
        s2 = manager.create_submission(
            benchmark_id=bm.identifier,
            name='B',
            user_id=USER_2,
            members=[USER_1]
        )
        # Update submission name only
        s1 = manager.update_submission(s1.identifier, name='C')
        assert s1.name == 'C'
        assert len(s1.get_members()) == 1
        assert s1.has_member(USER_1)
        # Cannot change name to an existing one
        with pytest.raises(err.ConstraintViolationError):
            manager.update_submission(s1.identifier, name='B')
        # Update without any changes
        s1 = manager.update_submission(s1.identifier, name='C')
        assert s1.name == 'C'
        assert len(s1.get_members()) == 1
        s1 = manager.update_submission(s1.identifier)
        assert s1.name == 'C'
        assert len(s1.get_members()) == 1
        # Update submission members
        s2 = manager.update_submission(s2.identifier, members=[USER_3])
        assert s2.name == 'B'
        assert len(s2.get_members()) == 1
        assert s2.has_member(USER_3)
        # Update name and members
        s2 = manager.update_submission(
            s2.identifier,
            name='D',
            members=[USER_1, USER_3]
        )
        assert s2.name == 'D'
        assert len(s2.get_members()) == 2
        assert s2.has_member(USER_1)
        assert s2.has_member(USER_3)
