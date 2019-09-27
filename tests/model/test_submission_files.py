# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test file store functionality of the submission manager."""

import os
import pytest

from passlib.hash import pbkdf2_sha256

from robcore.model.template.benchmark.repo import BenchmarkRepository
from robcore.model.submission import SubmissionManager
from robcore.tests.io import FakeStream
from robcore.model.template.repo.fs import TemplateFSRepository

import robcore.error as err
import robcore.tests.db as db
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

USER_1 = util.get_unique_identifier()


class TestSubmissionManagerFilestore(object):
    """Unit tests for functionality of the submission manager that maintains
    files that are uploaded by the user.
    """
    def init(self, base_dir):
        """Create a fresh database. Returns an tuple containing an instance of
        the submission manager and the handles for two submissions.
        """
        con = db.init_db(base_dir).connect()
        sql = 'INSERT INTO api_user(user_id, name, secret, active) VALUES(?, ?, ?, ?)'
        con.execute(sql, (USER_1, USER_1, pbkdf2_sha256.hash(USER_1), 1))
        con.commit()
        benchmark = BenchmarkRepository(
            con=con,
            template_repo=TemplateFSRepository(base_dir=str(base_dir))
        ).add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        submissions = SubmissionManager(con=con, directory=base_dir)
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
        return submissions, s1, s2

    def test_delete_file(self, tmpdir):
        """Test deleting an uploaded file."""
        # Initialize database. Get submission manager and submission handles.
        submissions, s1, s2 = self.init(str(tmpdir))
        # Upload one file for s1
        fh = submissions.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        fh = submissions.get_file(s1.identifier, fh.identifier)
        assert os.path.isfile(fh.filepath)
        submissions.delete_file(s1.identifier, fh.identifier)
        assert not os.path.isfile(fh.filepath)
        with pytest.raises(err.UnknownFileError):
            submissions.get_file(s1.identifier, fh.identifier)
        with pytest.raises(err.UnknownFileError):
            submissions.delete_file(s1.identifier, fh.identifier)

    def test_get_file(self, tmpdir):
        """Test accessing uploaded files."""
        # Initialize database. Get submission manager and submission handles.
        submissions, s1, s2 = self.init(str(tmpdir))
        # Upload one file for s1
        fh = submissions.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        file_id = fh.identifier
        fh = submissions.get_file(s1.identifier, file_id)
        assert fh.identifier == file_id
        assert fh.name == 'A.json'
        assert util.read_object(filename=fh.filepath) == {'A': 1}
        # Error situations
        # - File handle is unknown for s2
        with pytest.raises(err.UnknownFileError):
            submissions.get_file(s2.identifier, file_id)
        # - Access file with unknown file identifier
        with pytest.raises(err.UnknownFileError):
            submissions.get_file('UNK', file_id)

    def test_list_files(self, tmpdir):
        """Test listing uploaded files."""
        # Initialize database. Get submission manager and submission handles.
        submissions, s1, s2 = self.init(str(tmpdir))
        # Upload two files for s1 and one file for s2
        fh1 = submissions.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        fh2 = submissions.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='B.json'
        )
        fh3 = submissions.upload_file(
            submission_id=s2.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        assert fh1.identifier != fh3.identifier
        # The file listing for s1 should contain A.json and B.json
        files = submissions.list_files(s1.identifier)
        assert len(files) == 2
        names = [f.file_name for f in files]
        assert 'A.json' in names
        assert 'B.json' in names
        ids = [f.identifier for f in files]
        assert fh1.identifier in ids
        assert fh2.identifier in ids
        # The file listing for s2 should only contain A.json
        files = submissions.list_files(s2.identifier)
        assert len(files) == 1
        fh = files[0]
        assert fh.file_name == 'A.json'
        assert fh.identifier == fh3.identifier
        # Error when listing files for unknown submission
        with pytest.raises(err.UnknownSubmissionError):
            submissions.list_files(submission_id='UNK')

    def test_upload_file(self, tmpdir):
        """Test uploading files."""
        # Initialize database. Get submission manager and submission handles.
        submissions, s1, s2 = self.init(str(tmpdir))
        # Upload a new file for the submission
        fh1 = submissions.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        assert fh1.name == 'A.json'
        assert fh1.size > 0
        assert os.path.isfile(fh1.filepath)
        assert util.read_object(filename=fh1.filepath) == {'A': 1}
        # Can have multiple files with the same name
        fh2 = submissions.upload_file(
            submission_id=s1.identifier,
            file=FakeStream(data={'A': 1}),
            file_name='A.json'
        )
        assert fh2.name == 'A.json'
        assert fh1.identifier != fh2.identifier
        # Test error cases
        # - Invalid file name
        with pytest.raises(err.ConstraintViolationError):
            submissions.upload_file(
                submission_id=s1.identifier,
                file=FakeStream(data={'A': 1}),
                file_name=' '
            )
        # - Unknown submission
        with pytest.raises(err.UnknownSubmissionError):
            submissions.upload_file(
                submission_id='UNK',
                file=FakeStream(data={'A': 1}),
                file_name='A.json'
            )
