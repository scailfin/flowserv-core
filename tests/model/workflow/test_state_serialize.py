# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test serialization/deserialization methods for workflow states."""

import pytest

from robcore.model.resource import FileResource

import robcore.core.util as util
import robcore.model.workflow.state as state
import robcore.controller.backend.sync as sync


"""Default timestamps."""
CREATED_AT = '2019-09-15T11:23:19'
FINISHED_AT = '2019-10-05T08:17:09'
STARTED_AT = '2019-09-16T11:46:31'


class TestWorkflowStateSerializer(object):
    """Unit tests for the serialize/deserialize methods for workflow states."""
    def test_canceled_state(self):
        """Test serialization/deserialization of canceled states."""
        s = state.StateCanceled(
            created_at=util.to_datetime(CREATED_AT),
            started_at=util.to_datetime(STARTED_AT),
            stopped_at=util.to_datetime(FINISHED_AT),
            messages=['there', 'were', 'errors']
        )
        s = sync.deserialize_state(sync.serialize_state(s))
        assert s.is_canceled()
        assert s.messages == ['there', 'were', 'errors']
        self.validate_date(s.created_at, util.to_datetime(CREATED_AT))
        self.validate_date(s.started_at, util.to_datetime(STARTED_AT))
        self.validate_date(s.stopped_at, util.to_datetime(FINISHED_AT))

    def test_error_state(self):
        """Test serialization/deserialization of error states."""
        s = state.StateError(
            created_at=util.to_datetime(CREATED_AT),
            started_at=util.to_datetime(STARTED_AT),
            stopped_at=util.to_datetime(FINISHED_AT),
            messages=['there', 'were', 'errors']
        )
        s = sync.deserialize_state(sync.serialize_state(s))
        assert s.is_error()
        assert s.messages == ['there', 'were', 'errors']
        self.validate_date(s.created_at, util.to_datetime(CREATED_AT))
        self.validate_date(s.started_at, util.to_datetime(STARTED_AT))
        self.validate_date(s.stopped_at, util.to_datetime(FINISHED_AT))

    def test_invalid_object(self):
        """Ensure there is an error if an object with an unknown state
        type is given.
        """
        with pytest.raises(KeyError):
            sync.deserialize_state({
                sync.LABEL_STATE_TYPE: 'unknown'
            })
        with pytest.raises(ValueError):
            sync.deserialize_state({
                sync.LABEL_STATE_TYPE: 'unknown',
                sync.LABEL_CREATED_AT: CREATED_AT
            })

    def test_pending_state(self):
        """Test serialization/deserialization of pending states."""
        s = state.StatePending(created_at=util.to_datetime(CREATED_AT))
        s = sync.deserialize_state(sync.serialize_state(s))
        assert s.is_pending()
        self.validate_date(s.created_at, util.to_datetime(CREATED_AT))

    def test_running_state(self):
        """Test serialization/deserialization of running states."""
        s = state.StateRunning(
            created_at=util.to_datetime(CREATED_AT),
            started_at=util.to_datetime(STARTED_AT)
        )
        s = sync.deserialize_state(sync.serialize_state(s))
        assert s.is_running()
        self.validate_date(s.created_at, util.to_datetime(CREATED_AT))
        self.validate_date(s.started_at, util.to_datetime(STARTED_AT))

    def test_success_state(self):
        """Test serialization/deserialization of success states."""
        s = state.StateSuccess(
            created_at=util.to_datetime(CREATED_AT),
            started_at=util.to_datetime(STARTED_AT),
            finished_at=util.to_datetime(FINISHED_AT),
            files=[
                FileResource('0', 'myfile1', 'dev/null/myfile1'),
                FileResource('1', 'myfile2', 'dev/null/myfile2')
            ]
        )
        s = sync.deserialize_state(sync.serialize_state(s))
        assert s.is_success()
        assert s.get_resource('myfile1').filename == 'dev/null/myfile1'
        assert s.get_resource('myfile2').filename == 'dev/null/myfile2'
        assert len(s.list_resources()) == 2
        self.validate_date(s.created_at, util.to_datetime(CREATED_AT))
        self.validate_date(s.started_at, util.to_datetime(STARTED_AT))
        self.validate_date(s.finished_at, util.to_datetime(FINISHED_AT))

    def validate_date(self, dt, ts):
        """Ensure that the given datetime is matches the given timestamp."""
        assert dt.year == ts.year
        assert dt.month == ts.month
        assert dt.day == ts.day
        assert dt.hour == ts.hour
        assert dt.minute == ts.minute
        assert dt.second == ts.second
