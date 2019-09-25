# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Default methods to (de-)serialize workflow state."""

from robcore.model.workflow.resource import FileResource

import robcore.util as util
import robcore.model.workflow.state.base as state


"""Labels for serialization."""
LABEL_CREATED_AT = 'createdAt'
LABEL_FILENAME = 'filename'
LABEL_FINISHED_AT = 'finishedAt'
LABEL_ID = 'id'
LABEL_MESSAGES = 'messages'
LABEL_RESOURCES = 'resources'
LABEL_STARTED_AT = 'startedAt'
LABEL_STATE_TYPE = 'type'
LABEL_STOPPED_AT = 'stoppedAt'


def deserialize_state(doc):
    """Create instance of workflow state from a given dictionary serialization.

    Parameters
    ----------
    doc: dict
        Serialization if the workflow state

    Returns
    -------
    robcore.model.workflow.state.base.WorkflowState

    Raises
    ------
    KeyError
    ValueError
    """
    type_id = doc[LABEL_STATE_TYPE]
    # All state serializations have to have a 'created at' timestamp
    created_at = util.to_datetime(doc[LABEL_CREATED_AT])
    if type_id == state.STATE_PENDING:
        return state.StatePending(created_at=created_at)
    elif type_id == state.STATE_RUNNING:
        return state.StateRunning(
            created_at=created_at,
            started_at=util.to_datetime(doc[LABEL_STARTED_AT])
        )
    elif type_id == state.STATE_CANCELED:
        return state.StateCanceled(
            created_at=created_at,
            started_at=util.to_datetime(doc[LABEL_STARTED_AT]),
            stopped_at=util.to_datetime(doc[LABEL_FINISHED_AT]),
            messages=doc[LABEL_MESSAGES]
        )
    elif type_id == state.STATE_ERROR:
        return state.StateError(
            created_at=created_at,
            started_at=util.to_datetime(doc[LABEL_STARTED_AT]),
            stopped_at=util.to_datetime(doc[LABEL_FINISHED_AT]),
            messages=doc[LABEL_MESSAGES]
        )
    elif type_id == state.STATE_SUCCESS:
        files = dict()
        for obj in doc[LABEL_RESOURCES]:
            res_id = obj[LABEL_ID]
            res = FileResource(identifier=res_id, filename=obj[LABEL_FILENAME])
            files[res_id] = res
        return state.StateSuccess(
            created_at=created_at,
            started_at=util.to_datetime(doc[LABEL_STARTED_AT]),
            finished_at=util.to_datetime(doc[LABEL_FINISHED_AT]),
            files=files
        )
    else:
        raise ValueError('invalid state type \'{}\''.format(type_id))


def serialize_state(wf_state):
    """Create dictionary serialization if a given workflow state.

    Parameters
    ----------
    wf_state: robcore.model.workflow.state.base.WorkflowState
        Workflow state

    Returns
    -------
    dict
    """
    doc = {
        LABEL_STATE_TYPE: wf_state.type_id,
        LABEL_CREATED_AT: wf_state.created_at.isoformat()
    }
    if wf_state.is_running():
        doc[LABEL_STARTED_AT] = wf_state.started_at.isoformat()
    elif wf_state.is_canceled() or wf_state.is_error():
        doc[LABEL_STARTED_AT] = wf_state.started_at.isoformat()
        doc[LABEL_FINISHED_AT] = wf_state.stopped_at.isoformat()
        doc[LABEL_MESSAGES] = wf_state.messages
    elif wf_state.is_success():
        doc[LABEL_STARTED_AT] = wf_state.started_at.isoformat()
        doc[LABEL_FINISHED_AT] = wf_state.finished_at.isoformat()
        doc[LABEL_RESOURCES] = [
            {
                LABEL_ID: f.identifier,
                LABEL_FILENAME: f.filename
            } for f in wf_state.files.values()
        ]
    return doc
