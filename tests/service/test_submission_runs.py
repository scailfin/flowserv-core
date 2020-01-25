# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods for running benchmark submissions."""

import os

from flowserv.tests.files import FakeStream

import flowserv.model.parameter.base as pb
import flowserv.model.parameter.declaration as pd
import flowserv.tests.api as api
import flowserv.core.util as util
import flowserv.view.labels as labels


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


class TestSubmissionsRuns(object):
    """Test API methods that access and list submissions and their results."""
    @staticmethod
    def init(basedir):
        """Initialize the database, benchmark repository, and submission
        manager. Loads one benchmark.

        Returns the submission service, handles for created users, and the
        benchmark handle.
        """
        repository, submissions, user_service, runs, _, _, _ = api.init_api(
            basedir
        )
        users = list()
        for i in range(3):
            user_id = util.get_unique_identifier()
            users.append(user_service.manager.register_user(user_id, user_id))
        bm = repository.repo.add_benchmark(name='A', sourcedir=TEMPLATE_DIR)
        return submissions, users, bm, runs

    def test_run_submission(self, tmpdir):
        """Test running a submission with modified parameter list."""
        submissions, users, bm, runs = TestSubmissionsRuns.init(str(tmpdir))
        # Get handle for USER_1
        user = users[0]
        # Create new submission with a single member
        s = submissions.create_submission(
            benchmark_id=bm.identifier,
            name='A',
            user=user,
            parameters=pb.create_parameter_index([
                pd.parameter_declaration(
                    identifier='downtime',
                    name="Down Time",
                    index=1,
                    data_type=pd.DT_INTEGER
                ),
                pd.parameter_declaration(
                    identifier='sleeptime',
                    name="Sleep Time",
                    index=2,
                    default_value=20,
                    data_type=pd.DT_INTEGER
                )
            ])
        )
        submission_id = s[labels.ID]
        f_id = submissions.upload_file(
            submission_id=submission_id,
            file=FakeStream(),
            file_name='names.txt',
            user=user
        )[labels.ID]
        # Start a new run. The resulting run is expected to be in pending state
        r = runs.start_run(
            submission_id,
            [
                {labels.ID: 'names', labels.VALUE: f_id},
                {labels.ID: 'sleeptime', labels.VALUE: 5},
                {labels.ID: 'downtime', labels.VALUE: 6}
            ],
            user
        )
        assert len(r[labels.ARGUMENTS]) == 3
