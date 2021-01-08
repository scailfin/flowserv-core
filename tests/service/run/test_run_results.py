# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for accessing the result files in a successful workflow run."""

import pytest
import tarfile
import tempfile

from flowserv.config import Config
from flowserv.tests.service import (
    create_group, create_user, start_hello_world, write_results
)

import flowserv.error as err
import flowserv.util as util


def init_db(service, config, hello_world):
    """Initialize a new database with two users, one group and a successful
    run. Returns user identifier, group identifier and run identifier.
    """
    with service(config=config) as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api).workflow_id
    with service(config=config, user_id=user_1) as api:
        group_id = create_group(api, workflow_id=workflow_id)
        # Start the new run. Then set it into SUCESS state.
        run_id, file_id = start_hello_world(api, group_id)
        tmpdir = tempfile.mkdtemp()
        write_results(
            rundir=tmpdir,
            files=[
                ({'group': group_id, 'run': run_id}, None, 'results/data.json'),
                ([group_id, run_id], 'txt/plain', 'values.txt')
            ]
        )
        api.runs().update_run(
            run_id=run_id,
            state=api.runs().backend.success(
                run_id,
                files=['results/data.json', 'values.txt']
            ),
            rundir=tmpdir
        )
    return user_1, user_2, group_id, run_id


def test_access_run_result_files_local(local_service, hello_world, tmpdir):
    """Test accessing run result files."""
    # -- Setup ----------------------------------------------------------------
    config = Config().basedir(tmpdir)
    user_1, user_2, group_id, run_id = init_db(local_service, config, hello_world)
    # -- Read result files ----------------------------------------------------
    with local_service(config=config, user_id=user_1) as api:
        # Map file names to file handles.
        r = api.runs().get_run(run_id=run_id)
        files = dict()
        for fh in r['files']:
            files[fh['name']] = fh['id']
        # Read content of result files.
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/data.json']
        )
        results = util.read_object(fh)
        assert results == {'group': group_id, 'run': run_id}
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['values.txt']
        )
        values = fh.read().decode('utf-8').strip()
        assert values == '{}\n{}'.format(group_id, run_id)
    # -- Error when user 2 attempts to read file ------------------------------
    with local_service(config=config, user_id=user_2) as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().get_result_file(
                run_id=run_id,
                file_id=files['results/data.json']
            )


def test_result_archive_local(local_service, hello_world, tmpdir):
    """Test getting an archive of run results."""
    # -- Setup ----------------------------------------------------------------
    config = Config().basedir(tmpdir)
    user_1, user_2, group_id, run_id = init_db(local_service, config, hello_world)
    # -- Get result archive ---------------------------------------------------
    with local_service(config=config, user_id=user_1) as api:
        archive = api.runs().get_result_archive(run_id=run_id)
        tar = tarfile.open(fileobj=archive, mode='r:gz')
        members = [t.name for t in tar.getmembers()]
        assert len(members) == 2
        assert 'results/data.json' in members
        assert 'values.txt' in members


def test_result_archive_remote(remote_service, mock_response):
    """Test downloading run result archive from the remote service."""
    remote_service.runs().get_result_archive(run_id='0000')


def test_result_file_remote(remote_service, mock_response):
    """Test downloading a run result file from the remote service."""
    remote_service.runs().get_result_file(run_id='0000', file_id='0001')
