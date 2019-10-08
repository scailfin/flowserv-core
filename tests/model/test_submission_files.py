# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test file store functionality of the submission manager."""

import json
import os
import pytest

from passlib.hash import pbkdf2_sha256

from robcore.model.submission import SubmissionManager
from robcore.model.workflow.engine import BenchmarkEngine
from robcore.tests.benchmark import StateEngine
from robcore.tests.io import FakeStream

import robcore.error as err
import robcore.model.ranking as ranking
import robcore.tests.benchmark as bm
import robcore.tests.db as db
import robcore.util as util


BENCHMARK_1 = util.get_unique_identifier()
USER_1 = util.get_unique_identifier()


class TestSubmissionManagerFilestore(object):
    """Unit tests for functionality of the submission manager that maintains
    files that are uploaded by the user.
    """
    @staticmethod
    def init(base_dir):
        """Create a fresh database with one user and one benchmark. Creates two
        submissions for the benchmark. Returns a triple containing an instance
        of the submission manager and the two submission handles.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        con.execute(sql, (USER_1, USER_1, pbkdf2_sha256.hash(USER_1), 1))
        sql = 'INSERT INTO benchmark(benchmark_id, name, result_schema) '
        sql += 'VALUES(?, ?, ?)'
        schema = json.dumps(bm.BENCHMARK_SCHEMA.to_dict())
        con.execute(sql, (BENCHMARK_1, BENCHMARK_1, schema))
        ranking.create_result_table(
            con=con,
            benchmark_id=BENCHMARK_1,
            schema=bm.BENCHMARK_SCHEMA,
            commit_changes=False
        )
        con.commit()
        manager = SubmissionManager(
            con=con,
            directory=base_dir,
            engine=BenchmarkEngine(
                con=con,
                backend=StateEngine()
            )
        )
        # Create two submissions
        s1 = manager.create_submission(
            benchmark_id=BENCHMARK_1,
            name='A',
            user_id=USER_1
        )
        s2 = manager.create_submission(
            benchmark_id=BENCHMARK_1,
            name='B',
            user_id=USER_1
        )
        return manager, s1, s2

    def test_delete_file(self, tmpdir):
        """Test deleting an uploaded file."""
        # Initialize database. Get submission manager and submission handles
        manager, s1, s2 = TestSubmissionManagerFilestore.init(str(tmpdir))
        # Upload one file for submission 1 and one file for submission 2
        fh1 = manager.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        fh2 = manager.upload_file(
            submission_id=s2.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        fh1 = manager.get_file(s1.identifier, fh1.identifier)
        assert os.path.isfile(fh1.filepath)
        fh2 = manager.get_file(s2.identifier, fh2.identifier)
        assert os.path.isfile(fh2.filepath)
        assert fh1.filepath != fh2.filepath
        # Deleting a submission will delete all associated files
        manager.delete_submission(s2.identifier)
        assert os.path.isfile(fh1.filepath)
        assert not os.path.isfile(fh2.filepath)
        # Delete the remaining file for submission 1
        manager.delete_file(s1.identifier, fh1.identifier)
        assert not os.path.isfile(fh1.filepath)
        with pytest.raises(err.UnknownFileError):
            manager.get_file(s1.identifier, fh1.identifier)
        with pytest.raises(err.UnknownFileError):
            manager.delete_file(s1.identifier, fh1.identifier)

    def test_get_file(self, tmpdir):
        """Test accessing uploaded files."""
        # Initialize database. Get submission manager and submission handles
        manager, s1, s2 = TestSubmissionManagerFilestore.init(str(tmpdir))
        # Upload one file for submission 1
        fh = manager.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        file_id = fh.identifier
        fh = manager.get_file(s1.identifier, file_id)
        assert fh.identifier == file_id
        assert fh.name == 'A.json'
        assert util.read_object(filename=fh.filepath) == {'A': 1}
        # Error situations
        # - File handle is unknown for s2
        with pytest.raises(err.UnknownFileError):
            manager.get_file(s2.identifier, file_id)
        # - Access file with unknown file identifier
        with pytest.raises(err.UnknownFileError):
            manager.get_file('UNK', file_id)

    def test_list_files(self, tmpdir):
        """Test listing uploaded files."""
        # Initialize database. Get submission manager and submission handles
        manager, s1, s2 = TestSubmissionManagerFilestore.init(str(tmpdir))
        # Upload two files for s1 and one file for s2
        fh1 = manager.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        fh2 = manager.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='B.json'
        )
        fh3 = manager.upload_file(
            submission_id=s2.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        assert fh1.identifier != fh3.identifier
        # The file listing for s1 should contain A.json and B.json
        files = manager.list_files(s1.identifier)
        assert len(files) == 2
        names = [f.file_name for f in files]
        assert 'A.json' in names
        assert 'B.json' in names
        ids = [f.identifier for f in files]
        assert fh1.identifier in ids
        assert fh2.identifier in ids
        # The file listing for s2 should only contain A.json
        files = manager.list_files(s2.identifier)
        assert len(files) == 1
        fh = files[0]
        assert fh.file_name == 'A.json'
        assert fh.identifier == fh3.identifier

    def test_upload_file(self, tmpdir):
        """Test uploading files."""
        # Initialize database. Get submission manager and submission handles
        manager, s1, s2 = TestSubmissionManagerFilestore.init(str(tmpdir))
        # Upload a new file for the submission
        fh1 = manager.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        assert fh1.name == 'A.json'
        assert fh1.size > 0
        assert os.path.isfile(fh1.filepath)
        assert util.read_object(filename=fh1.filepath) == {'A': 1}
        # Can have multiple files with the same name
        fh2 = manager.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        assert fh2.name == 'A.json'
        assert fh1.identifier != fh2.identifier
        # Test error cases
        # - Invalid file name
        with pytest.raises(err.ConstraintViolationError):
            manager.upload_file(
                submission_id=s1.identifier,
                file=FakeStream(data={'A': 1}),
                file_name=' '
            )
        # - Unknown submission
        with pytest.raises(err.UnknownSubmissionError):
            manager.upload_file(
                submission_id='UNK',
                file=FakeStream(data={'A': 1}),
                file_name='A.json'
            )
