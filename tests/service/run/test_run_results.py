# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for accessing the result files in a successful workflow run."""

import pytest
import tarfile

from flowserv.config import Config
from flowserv.tests.controller import StateEngine
from flowserv.tests.model import create_user, success_run
from flowserv.service.local import LocalAPIFactory
from flowserv.volume.factory import Volume

import flowserv.error as err
import flowserv.util as util


def test_access_run_result_files_local(database, tmpdir):
    """Test accessing run result files."""
    # -- Setup ----------------------------------------------------------------
    env = Config().basedir(tmpdir).auth()
    fs = Volume(env=env)
    workflow_id, group_id, run_id, user_id = success_run(database, fs, tmpdir)
    local_service = LocalAPIFactory(env=env, db=database, engine=StateEngine())
    # -- Read result files ----------------------------------------------------
    with local_service(user_id=user_id) as api:
        # Map file names to file handles.
        r = api.runs().get_run(run_id=run_id)
        files = dict()
        for fh in r['files']:
            files[fh['name']] = fh['id']
        # Read content of result files.
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/B.json']
        )
        results = util.read_object(fh.open())
        assert results == {'B': 1}
    # -- Error when user 2 attempts to read file ------------------------------
    with database.session() as session:
        user_2 = create_user(session, active=True)
    with local_service(user_id=user_2) as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().get_result_file(
                run_id=run_id,
                file_id=files['results/B.json']
            )
    # -- With an open access policy user 2 can read the data file -------------
    env = Config().basedir(tmpdir).open_access()
    local_service = LocalAPIFactory(env=env, db=database, engine=StateEngine())
    with local_service(user_id=user_2) as api:
        api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/B.json']
        )


def test_result_archive_local(database, tmpdir):
    """Test getting an archive of run results."""
    # -- Setup ----------------------------------------------------------------
    env = Config().basedir(tmpdir).auth()
    fs = Volume(env=env)
    workflow_id, group_id, run_id, user_id = success_run(database, fs, tmpdir)
    local_service = LocalAPIFactory(env=env, db=database, engine=StateEngine())
    # -- Get result archive ---------------------------------------------------
    with local_service(user_id=user_id) as api:
        archive = api.runs().get_result_archive(run_id=run_id)
        tar = tarfile.open(fileobj=archive.open(), mode='r:gz')
        members = [t.name for t in tar.getmembers()]
        assert len(members) == 2
        assert 'A.json' in members
        assert 'results/B.json' in members


def test_result_archive_remote(remote_service, mock_response):
    """Test downloading run result archive from the remote service."""
    remote_service.runs().get_result_archive(run_id='0000')


def test_result_file_remote(remote_service, mock_response):
    """Test downloading a run result file from the remote service."""
    remote_service.runs().get_result_file(run_id='0000', file_id='0001')
