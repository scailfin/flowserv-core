# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the serialize/deserialize methods for workflow states."""

import os
import pytest

from flowserv.model.workflow.resource import FSObject

import flowserv.core.util as util
import flowserv.model.workflow.state as state


"""Default timestamps."""
CREATED_AT = '2019-09-15T11:23:19'
FINISHED_AT = '2019-10-05T08:17:09'
STARTED_AT = '2019-09-16T11:46:31'


def test_canceled_state():
    """Test serialization/deserialization of canceled states."""
    s = state.StateCanceled(
        created_at=util.to_datetime(CREATED_AT),
        started_at=util.to_datetime(STARTED_AT),
        stopped_at=util.to_datetime(FINISHED_AT),
        messages=['there', 'were', 'errors']
    )
    s = state.deserialize_state(state.serialize_state(s))
    assert s.is_canceled()
    assert s.messages == ['there', 'were', 'errors']
    validate_date(s.created_at, util.to_datetime(CREATED_AT))
    validate_date(s.started_at, util.to_datetime(STARTED_AT))
    validate_date(s.stopped_at, util.to_datetime(FINISHED_AT))


def test_error_state():
    """Test serialization/deserialization of error states."""
    s = state.StateError(
        created_at=util.to_datetime(CREATED_AT),
        started_at=util.to_datetime(STARTED_AT),
        stopped_at=util.to_datetime(FINISHED_AT),
        messages=['there', 'were', 'errors']
    )
    s = state.deserialize_state(state.serialize_state(s))
    assert s.is_error()
    assert s.messages == ['there', 'were', 'errors']
    validate_date(s.created_at, util.to_datetime(CREATED_AT))
    validate_date(s.started_at, util.to_datetime(STARTED_AT))
    validate_date(s.stopped_at, util.to_datetime(FINISHED_AT))


def test_invalid_object():
    """Ensure there is an error if an object with an unknown state
    type is given.
    """
    with pytest.raises(KeyError):
        state.deserialize_state({
            state.LABEL_STATE_TYPE: 'unknown'
        })
    with pytest.raises(ValueError):
        state.deserialize_state({
            state.LABEL_STATE_TYPE: 'unknown',
            state.LABEL_CREATED_AT: CREATED_AT
        })


def test_pending_state():
    """Test serialization/deserialization of pending states."""
    s = state.StatePending(created_at=util.to_datetime(CREATED_AT))
    s = state.deserialize_state(state.serialize_state(s))
    assert s.is_pending()
    validate_date(s.created_at, util.to_datetime(CREATED_AT))


def test_running_state():
    """Test serialization/deserialization of running states."""
    s = state.StateRunning(
        created_at=util.to_datetime(CREATED_AT),
        started_at=util.to_datetime(STARTED_AT)
    )
    s = state.deserialize_state(state.serialize_state(s))
    assert s.is_running()
    validate_date(s.created_at, util.to_datetime(CREATED_AT))
    validate_date(s.started_at, util.to_datetime(STARTED_AT))


def test_success_state(tmpdir):
    """Test serialization/deserialization of success states."""
    filename = os.path.join(str(tmpdir), 'results.json')
    util.write_object(filename=filename, obj={'A': 1})
    s = state.StateSuccess(
        created_at=util.to_datetime(CREATED_AT),
        started_at=util.to_datetime(STARTED_AT),
        finished_at=util.to_datetime(FINISHED_AT),
        resources=[
            FSObject(identifier='0', name='myfile1', filename=filename),
            FSObject(identifier='1', name='myfile2', filename=filename)
        ]
    )
    s = state.deserialize_state(state.serialize_state(s))
    assert s.is_success()
    assert s.get_resource(name='myfile1').identifier == '0'
    assert s.get_resource(name='myfile2').identifier == '1'
    assert s.get_resource(identifier='0').name == 'myfile1'
    assert s.get_resource(identifier='1').name == 'myfile2'
    assert len(s.resources) == 2
    validate_date(s.created_at, util.to_datetime(CREATED_AT))
    validate_date(s.started_at, util.to_datetime(STARTED_AT))
    validate_date(s.finished_at, util.to_datetime(FINISHED_AT))


def validate_date(dt, ts):
    """Ensure that the given datetime is matches the given timestamp."""
    assert dt.year == ts.year
    assert dt.month == ts.month
    assert dt.day == ts.day
    assert dt.hour == ts.hour
    assert dt.minute == ts.minute
    assert dt.second == ts.second
