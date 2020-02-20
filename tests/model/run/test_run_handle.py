# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for run handles."""

from flowserv.model.run.base import RunHandle
from flowserv.model.workflow.state import StatePending


def test_update_run_handle_state():
    """Test the update state method for the run handle."""
    r1 = RunHandle(
        identifier='1',
        workflow_id='W1',
        group_id='G1',
        state=StatePending(),
        arguments={'a': 1},
        rundir='/dev/null'
    )
    assert r1.identifier == '1'
    assert r1.workflow_id == 'W1'
    assert r1.group_id == 'G1'
    assert r1.arguments == {'a': 1}
    assert r1.rundir == '/dev/null'
    assert r1.is_pending()
    # Update run state returns a copy of the handle with updated state
    r2 = r1.update_state(r1.state.start())
    assert r2.identifier == '1'
    assert r2.workflow_id == 'W1'
    assert r2.group_id == 'G1'
    assert r2.arguments == {'a': 1}
    assert r2.rundir == '/dev/null'
    assert r2.is_running()
    # Run 1 has not changed
    assert r1.identifier == '1'
    assert r1.workflow_id == 'W1'
    assert r1.group_id == 'G1'
    assert r1.arguments == {'a': 1}
    assert r1.rundir == '/dev/null'
    assert r1.is_pending()
