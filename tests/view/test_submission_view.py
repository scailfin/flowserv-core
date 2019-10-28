# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test API methods for benchmark submissions."""

import os
import pytest

from robcore.tests.io import FakeStream

import robcore.error as err
import robcore.view.hateoas as hateoas
import robcore.view.labels as labels
import robcore.tests.api as api
import robcore.tests.serialize as serialize
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Mandatory submission descriptor (D), handle (H), and listing (L) labels
DLABELS = [labels.ID, labels.NAME, labels.BENCHMARK, labels.LINKS]
HLABELS = DLABELS + [labels.MEMBERS, labels.PARAMETERS]
LLABELS = [labels.SUBMISSIONS, labels.LINKS]

# Mandatory labels for file handles
FILE_HANDLE = [labels.ID, labels.NAME, labels.CREATED_AT, labels.FILESIZE, labels.LINKS]

# Mandatory HATEOAS relationships in submission handles
RELS = [hateoas.SELF, hateoas.action(hateoas.UPLOAD), hateoas.BENCHMARK]
RELSFH = [hateoas.action(hateoas.DOWNLOAD), hateoas.action(hateoas.DELETE)]


class TestSubmissionsView(object):
    """Test API methods that access and list submissions and their results."""
    @staticmethod
    def init(base_dir):
        """Initialize the database, benchmark repository, and submission
        manager. Loads one benchmark.

        Returns the submission service, handles for created users, and the
        benchmark handle.
        """
        repository, submissions, user_service, _, _ = api.init_api(base_dir)
        users = list()
        for i in range(3):
            user_id = util.get_unique_identifier()
            users.append(user_service.manager.register_user(user_id, user_id))
        bm = repository.repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
        return submissions, users, bm

    def test_create_submission(self, tmpdir):
        """Test create new submission."""
        service, users, benchmark = TestSubmissionsView.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member
        r = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user
        )
        util.validate_doc(doc=r, mandatory_labels=HLABELS)
        assert len(r[labels.MEMBERS]) == 1
        serialize.validate_links(r, RELS)
        # Create new submission with a three members
        r = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='B',
            user=user,
            members=[users[1].identifier, users[2].identifier]
        )
        util.validate_doc(doc=r, mandatory_labels=HLABELS)
        assert len(r[labels.MEMBERS]) == 3
        for member in r[labels.MEMBERS]:
            util.validate_doc(
                doc=member,
                mandatory_labels=[labels.ID, labels.USERNAME]
            )
        serialize.validate_links(r, RELS)

    def test_delete_submission(self, tmpdir):
        """Test deleting submission."""
        service, users, benchmark = TestSubmissionsView.init(str(tmpdir))
        # Get handle forall three users
        user_1 = users[0]
        user_2 = users[1]
        user_3 = users[2]
        # Create two submission
        s1 = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user_1,
            members=[user_1.identifier, user_2.identifier]
        )
        s2 = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='C',
            user=user_3,
            members=[user_1.identifier]
        )
        # USER_1 is member of two submissions
        r = service.list_submissions(user=user_1)
        assert len(r[labels.SUBMISSIONS]) == 2
        service.delete_submission(s1[labels.ID], user_1)
        r = service.list_submissions(user=user_1)
        util.validate_doc(doc=r, mandatory_labels=LLABELS)
        serialize.validate_links(r, [hateoas.SELF])
        assert len(r[labels.SUBMISSIONS]) == 1
        for s in r[labels.SUBMISSIONS]:
            util.validate_doc(doc=s, mandatory_labels=DLABELS)
        with pytest.raises(err.UnauthorizedAccessError):
            service.delete_submission(s2[labels.ID], user_2)

    def test_file_uploads(self, tmpdir):
        """Test uploading, downloading and listing files that are associated
        with a benchmark submission.
        """
        # Initialize the database and create a single submissions
        service, users, benchmark = TestSubmissionsView.init(str(tmpdir))
        user_1 = users[0]
        user_3 = users[2]
        r = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user_1,
            members=[user_1.identifier, users[1].identifier]
        )
        submission_id = r[labels.ID]
        # Upload file (from fake streams)
        r = service.upload_file(
            submission_id=submission_id,
            file=FakeStream(data={'A': 1}),
            file_name='A.json',
            user=user_1
        )
        util.validate_doc(doc=r, mandatory_labels=FILE_HANDLE)
        serialize.validate_links(doc=r, keys=RELSFH)
        # Error when uploading without being a member
        with pytest.raises(err.UnauthorizedAccessError):
            service.upload_file(
                submission_id=submission_id,
                file=FakeStream(data={'A': 1}),
                file_name='A.json',
                user=user_3
            )
        # Upload second file ad retrieve file listing
        service.upload_file(
            submission_id=submission_id,
            file=FakeStream(data={'A': 1}),
            file_name='A.json',
            user=user_1
        )
        # Get a listing of files for the submission
        r = service.list_files(submission_id=submission_id, user=user_1)
        util.validate_doc(doc=r, mandatory_labels=[labels.FILES, labels.LINKS])
        files = r[labels.FILES]
        assert len(files) == 2
        for f in files:
            util.validate_doc(doc=f, mandatory_labels=FILE_HANDLE)
            serialize.validate_links(doc=f, keys=RELSFH)
        # Error when attempting to list files for submissions without being
        # a member
        with pytest.raises(err.UnauthorizedAccessError):
            service.list_files(submission_id=submission_id, user=user_3)
        # Get file returns file handle and its serialization
        fh, r = service.get_file(
            submission_id=submission_id,
            file_id=files[0][labels.ID],
            user=user_1
        )
        assert os.path.isfile(fh.filepath)
        util.validate_doc(doc=r, mandatory_labels=FILE_HANDLE)
        serialize.validate_links(doc=r, keys=RELSFH)
        # Error when attempting to access files for submissions without being
        # a member
        with pytest.raises(err.UnauthorizedAccessError):
            service.get_file(
                submission_id=submission_id,
                file_id=files[0][labels.ID],
                user=user_3
            )
        # Delete file returns listing of remaining files
        service.delete_file(
            submission_id=submission_id,
            file_id=files[0][labels.ID],
            user=user_1
        )
        r = service.list_files(submission_id=submission_id, user=user_1)
        util.validate_doc(doc=r, mandatory_labels=[labels.FILES, labels.LINKS])
        files = r[labels.FILES]
        assert len(files) == 1
        # Error when attempting to delete files for submissions without being
        # a member
        with pytest.raises(err.UnauthorizedAccessError):
            service.delete_file(
                submission_id=submission_id,
                file_id=files[0][labels.ID],
                user=user_3
            )

    def test_get_submission(self, tmpdir):
        """Test retrieving a submission handle."""
        service, users, benchmark = TestSubmissionsView.init(str(tmpdir))
        # Get handle forall three users
        user_1 = users[0]
        user_2 = users[1]
        user_3 = users[2]
        # Create new submission with two members
        r = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user_1,
            members=[user_1.identifier, user_2.identifier]
        )
        submission_id = r[labels.ID]
        r = service.get_submission(submission_id)
        util.validate_doc(doc=r, mandatory_labels=HLABELS)
        assert len(r[labels.MEMBERS]) == 2
        for member in r[labels.MEMBERS]:
            util.validate_doc(
                doc=member,
                mandatory_labels=[labels.ID, labels.USERNAME]
            )
        serialize.validate_links(r, RELS)
        # USER_2 and USER_3 can also access the submission
        r = service.get_submission(submission_id)
        util.validate_doc(doc=r, mandatory_labels=HLABELS)
        r = service.get_submission(submission_id)
        util.validate_doc(doc=r, mandatory_labels=HLABELS)

    def test_list_submissions(self, tmpdir):
        """Test retrieving a submission handle."""
        service, users, benchmark = TestSubmissionsView.init(str(tmpdir))
        # Get handle forall three users
        user_1 = users[0]
        user_2 = users[1]
        user_3 = users[2]
        # Create three submission
        service.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user_1,
            members=[user_1.identifier, user_2.identifier]
        )
        service.create_submission(
            benchmark_id=benchmark.identifier,
            name='B',
            user=user_1,
            members=[user_1.identifier, user_3.identifier]
        )
        service.create_submission(
            benchmark_id=benchmark.identifier,
            name='C',
            user=user_3,
            members=[user_1.identifier]
        )
        # USER_1 is member of three submissions
        r = service.list_submissions(user=user_1)
        util.validate_doc(doc=r, mandatory_labels=LLABELS)
        serialize.validate_links(r, [hateoas.SELF])
        assert len(r[labels.SUBMISSIONS]) == 3
        for s in r[labels.SUBMISSIONS]:
            util.validate_doc(doc=s, mandatory_labels=DLABELS)
        # USER_2 is member of one submission
        r = service.list_submissions(user=user_2)
        util.validate_doc(doc=r, mandatory_labels=LLABELS)
        serialize.validate_links(r, [hateoas.SELF])
        assert len(r[labels.SUBMISSIONS]) == 1
        # USER_3 is member of two submissions
        r = service.list_submissions(user=user_3)
        util.validate_doc(doc=r, mandatory_labels=LLABELS)
        serialize.validate_links(r, [hateoas.SELF])
        assert len(r[labels.SUBMISSIONS]) == 2

    def test_update_submission(self, tmpdir):
        """Test updating submission name and member list."""
        service, users, benchmark = TestSubmissionsView.init(str(tmpdir))
        # Get handle forall three users
        user_1 = users[0]
        user_2 = users[1]
        user_3 = users[2]
        # Create three submission
        r = service.create_submission(
            benchmark_id=benchmark.identifier,
            name='A',
            user=user_1,
            members=[user_1.identifier, user_2.identifier]
        )
        assert r[labels.NAME] == 'A'
        assert len(r[labels.MEMBERS]) == 2
        r = service.update_submission(
            submission_id=r[labels.ID],
            user=user_1,
            name='B',
            members=[user_3.identifier]
        )
        assert r[labels.NAME] == 'B'
        assert len(r[labels.MEMBERS]) == 1
        # Cannot update submission if not a member
        with pytest.raises(err.UnauthorizedAccessError):
            service.update_submission(
                submission_id=r[labels.ID],
                user=user_1,
                name='C',
                members=[user_1.identifier]
            )
