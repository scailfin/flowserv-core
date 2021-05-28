# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the serial workflow runner."""

import json
import os

from flowserv.controller.serial.engine.runner import exec_workflow
from flowserv.controller.serial.workflow.result import RunResult
from flowserv.controller.worker.manager import Code, WorkerPool
from flowserv.model.workflow.step import CodeStep
from flowserv.tests.worker import multi_by_x
from flowserv.volume.fs import FStore
from flowserv.volume.manager import VolumeManager, DEFAULT_STORE


def test_run_with_two_steps(tmpdir):
    """Test executing a sequence of two code steps that operate on the same
    file in different storage volumes.
    """
    # -- Setup ----------------------------------------------------------------
    # Create two separate storage volumes.
    vol1_dir = os.path.join(tmpdir, 'v1')
    os.makedirs(vol1_dir)
    vol2_dir = os.path.join(tmpdir, 'v2')
    volumes = VolumeManager(
        stores=[
            FStore(basedir=vol1_dir, identifier=DEFAULT_STORE),
            FStore(basedir=vol2_dir, identifier='v2')
        ],
        files={'data.json': [DEFAULT_STORE]}
    )
    # Create data.json file in v1.
    with open(os.path.join(vol1_dir, 'data.json'), 'w') as f:
        json.dump({"value": 5}, f)
    # Use separate workers for each step.
    workers = WorkerPool(
        workers=[
            Code(identifier='w1', volume=DEFAULT_STORE),
            Code(identifier='w2', volume='v2')
        ],
        managers={'s1': 'w1', 's2': 'w2'}
    )
    # Create workflow steps.
    steps = [
        CodeStep(
            identifier='s1',
            func=multi_by_x,
            arg='s1',
            varnames={'x': 'x1'},
            inputs=['data.json']
        ),
        CodeStep(
            identifier='s2',
            func=multi_by_x,
            arg='s2',
            varnames={'x': 'x2'},
            inputs=['data.json']
        )
    ]
    # Initialize the workflow context arguments.
    arguments = {'filename': 'data.json', 'x1': 2, 'x2': 3}
    # -- Test workflow run ----------------------------------------------------
    run_result = exec_workflow(
        steps=steps,
        workers=workers,
        volumes=volumes,
        result=RunResult(arguments=arguments)
    )
    assert len(run_result.steps) == 2
    assert run_result.context == {
        'filename': 'data.json',
        'x1': 2,
        'x2': 3,
        's1': 10,
        's2': 15
    }
    assert os.path.isfile(os.path.join(vol2_dir, 'data.json'))
    # Error case.
    os.unlink(os.path.join(vol1_dir, 'data.json'))
    run_result = exec_workflow(
        steps=steps,
        workers=workers,
        volumes=volumes,
        result=RunResult(arguments=arguments)
    )
    assert len(run_result.steps) == 1
    assert run_result.context == {'filename': 'data.json', 'x1': 2, 'x2': 3}
