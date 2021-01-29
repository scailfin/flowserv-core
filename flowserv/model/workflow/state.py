# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Definition of workflow states. The classes in this module represent the
different possible states of a workflow run. There are four different states:
(PENDING) the workflow run has been submitted and is waiting to start running,
(RUNNING) the workflow is actively executing at the moment,
(ERROR) workflow execution was interrupted by an error or canceled by the user,
(SUCCESS) the workflow run completed successfully.

Contains default methods to (de-)serialize workflow state.
"""

from flowserv.util import utc_now


"""Default log messages for canceled runs."""
CANCELED = ['canceled at user request']


"""Definition of state type identifier."""
STATE_CANCELED = 'CANCELED'
STATE_ERROR = 'ERROR'
STATE_PENDING = 'PENDING'
STATE_RUNNING = 'RUNNING'
STATE_SUCCESS = 'SUCCESS'


"""Short cut to list of active states."""
ACTIVE_STATES = [STATE_PENDING, STATE_RUNNING]


class WorkflowState(object):
    """The base class for workflow states contains the state type identifier
    that is used by the different state type methods. The state also maintains
    the timestamp of workflow run creation. Subclasses will add additional
    timestamps and properties.
    """
    def __init__(self, type_id, created_at=None):
        """Initialize the type identifier and the 'created at' timestamp.

        Parameters
        ----------
        type_id: string
            Type identifier
        """
        self.type_id = type_id
        self.created_at = created_at if created_at is not None else utc_now()

    def __eq__(self, other):
        """Equality between two states is defined by comparing their respective
        type identifier.

        Parameters
        ----------
        other: flowserv.model.workflow.state.WorkflowState
            Workflow state that this state is compared to.

        Returns
        -------
        bool
        """
        return self.type_id == other.type_id

    def __str__(self):
        """Get printable representation of the state type.

        Returns
        -------
        string
        """
        return self.type_id

    def is_active(self):
        """A workflow is in active state if it is either pending or running.

        Returns
        --------
        bool
        """
        return self.type_id in ACTIVE_STATES

    def is_canceled(self):
        """Returns True if the workflow state is of type CANCELED.

        Returns
        -------
        bool
        """
        return self.type_id == STATE_CANCELED

    def is_error(self):
        """Returns True if the workflow state is of type ERROR.

        Returns
        -------
        bool
        """
        return self.type_id == STATE_ERROR

    def is_pending(self):
        """Returns True if the workflow state is of type PENDING.

        Returns
        -------
        bool
        """
        return self.type_id == STATE_PENDING

    def is_running(self):
        """Returns True if the workflow state is of type RUNNING.

        Returns
        -------
        bool
        """
        return self.type_id == STATE_RUNNING

    def is_success(self):
        """Returns True if the workflow state is of type SUCCESS.

        Returns
        -------
        bool
        """
        return self.type_id == STATE_SUCCESS


class StateCanceled(WorkflowState):
    """Cancel state representation for a workflow run. The workflow has three
    timestamps: the workflow creation time, workflow run start time and the
    time when the workflow was canceled. The state also maintains an optional
    list of messages.
    """
    def __init__(
        self, created_at, started_at=None, stopped_at=None, messages=None
    ):
        """Initialize the timestamps that are associated with the workflow
        state and the optional messages.

        Parameters
        ----------
        created_at: string
            Timestamp of workflow creation
        started_at: string
            Timestamp when the workflow started running
        stopped_at: string, optional
            Timestamp when workflow was canceled
        messages: list(string), optional
            Optional list of messages
        """
        super(StateCanceled, self).__init__(
            type_id=STATE_CANCELED,
            created_at=created_at
        )
        self.started_at = started_at if started_at is not None else created_at
        self.stopped_at = stopped_at if stopped_at is not None else utc_now()
        self.messages = messages if messages is not None else CANCELED


class StateError(WorkflowState):
    """Error state representation for a workflow run. The workflow has three
    timestamps: the workflow creation time, workflow run start time and the
    time at which the error occured (ot workflow was canceled). The state also
    maintains an optional list of error messages.
    """
    def __init__(
        self, created_at, started_at=None, stopped_at=None, messages=None
    ):
        """Initialize the timestamps that are associated with the workflow
        state and the optional error messages.

        Parameters
        ----------
        created_at: string
            Timestamp of workflow creation
        started_at: string
            Timestamp when the workflow started running
        stopped_at: string, optional
            Timestamp when workflow error occurred or the when the workflow was
            canceled
        messages: list(string), optional
            Optional list of error messages
        """
        super(StateError, self).__init__(
            type_id=STATE_ERROR,
            created_at=created_at
        )
        self.started_at = started_at if started_at is not None else created_at
        self.stopped_at = stopped_at if stopped_at is not None else utc_now()
        self.messages = messages if messages is not None else list()


class StatePending(WorkflowState):
    """State representation for a pending workflow that is waiting to start
    running. The workflow has only one timestamp representing the workflow
    creation time.
    """
    def __init__(self, created_at=None):
        """Initialize the timestamp that is associated with the workflow state.

        Parameters
        ----------
        created_at: string, optional
            Timestamp of workflow creation
        """
        super(StatePending, self).__init__(
            type_id=STATE_PENDING,
            created_at=created_at
        )

    def cancel(self, messages=None):
        """Get instance of canceled state for a pending wokflow.

        Since the workflow did not start to run the started_at timestamp is set
        to the current time just like the stopped_at timestamp.

        Parameters
        ----------
        messages: list(string), optional
            Optional list of messages

        Returns
        -------
        flowserv.model.workflow.state.StateCanceled
        """
        ts = utc_now()
        return StateCanceled(
            created_at=self.created_at,
            started_at=ts,
            stopped_at=ts,
            messages=messages
        )

    def error(self, messages=None):
        """Get instance of error state for a pending wokflow. If the exception
        that caused the workflow execution to terminate is given it will be
        used to create the list of error messages.

        Since the workflow did not start to run the started_at timestamp is set
        to the current time just like the stopped_at timestamp.

        Parameters
        ----------
        messages: list(string), optional
            Optional list of error messages

        Returns
        -------
        flowserv.model.workflow.state.StateError
        """
        ts = utc_now()
        return StateError(
            created_at=self.created_at,
            started_at=ts,
            stopped_at=ts,
            messages=messages
        )

    def start(self):
        """Get instance of running state with the same create at timestamp as
        this state and the started at with the current timestamp.

        Returns
        -------
        flowserv.model.workflow.state.StateRunning
        """
        return StateRunning(created_at=self.created_at)

    def success(self, files=None):
        """Get instance of success state for a competed wokflow.

        Parameters
        ----------
        files: list(string), default=None
            Optional list of created files (relative path).

        Returns
        -------
        flowserv.model.workflow.state.StateSuccess
        """
        return StateSuccess(
            created_at=self.created_at,
            started_at=self.created_at,
            files=files
        )


class StateRunning(WorkflowState):
    """State representation for a active workflow run. The workflow has two
    timestamps: the workflow creation time and the workflow run start time.
    """
    def __init__(self, created_at, started_at=None):
        """Initialize the timestamps that are associated with the workflow
        state.

        Parameters
        ----------
        created_at: string
            Timestamp of workflow creation
        started_at: string
            Timestamp when the workflow started running
        """
        super(StateRunning, self).__init__(
            type_id=STATE_RUNNING,
            created_at=created_at
        )
        self.started_at = started_at if started_at is not None else utc_now()

    def cancel(self, messages=None):
        """Get instance of class cancel state for a running wokflow.

        Parameters
        ----------
        messages: list(string), optional
            Optional list of messages

        Returns
        -------
        flowserv.model.workflow.state.StateCanceled
        """
        return StateCanceled(
            created_at=self.created_at,
            started_at=self.started_at,
            messages=messages
        )

    def error(self, messages=None):
        """Get instance of error state for a running wokflow. If the exception
        that caused the workflow execution to terminate is given it will be
        used to create the list of error messages.

        Parameters
        ----------
        messages: list(string), optional
            Optional list of error messages

        Returns
        -------
        flowserv.model.workflow.state.StateError
        """
        return StateError(
            created_at=self.created_at,
            started_at=self.started_at,
            messages=messages
        )

    def success(self, files=None):
        """Get instance of success state for a competed wokflow.

        Parameters
        ----------
        files: list(string), default=None
            Optional list of created files (relative path).

        Returns
        -------
        flowserv.model.workflow.state.StateSuccess
        """
        return StateSuccess(
            created_at=self.created_at,
            started_at=self.started_at,
            files=files
        )


class StateSuccess(WorkflowState):
    """Success state representation for a workflow run. The workflow has three
    timestamps: the workflow creation time, workflow run start time and the
    time when the workflow execution finished. The state also maintains handles
    to any files that were created by the workflow run.
    """
    def __init__(
        self, created_at, started_at, finished_at=None, files=None
    ):
        """Initialize the timestamps that are associated with the workflow
        state and the list of created files.

        Parameters
        ----------
        created_at: string
            Timestamp of workflow creation
        started_at: string
            Timestamp when the workflow started running
        finished_at: string, optional
            Timestamp when workflow execution completed
        files: list(string), default=None
            Optional list of created files (relative path).
        """
        super(StateSuccess, self).__init__(
            type_id=STATE_SUCCESS,
            created_at=created_at
        )
        self.started_at = started_at
        self.finished_at = finished_at if finished_at is not None else utc_now()  # noqa: E501
        self.files = files if files is not None else list()


# -- Serialization/Deserialization helper methods -----------------------------

"""Labels for serialization."""
LABEL_CREATED_AT = 'createdAt'
LABEL_FILES = 'files'
LABEL_FINISHED_AT = 'finishedAt'
LABEL_MESSAGES = 'messages'
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
    flowserv.model.workflow.state.WorkflowState

    Raises
    ------
    KeyError
    ValueError
    """
    type_id = doc[LABEL_STATE_TYPE]
    # All state serializations have to have a 'created at' timestamp
    created_at = doc[LABEL_CREATED_AT]
    if type_id == STATE_PENDING:
        return StatePending(created_at=created_at)
    elif type_id == STATE_RUNNING:
        return StateRunning(
            created_at=created_at,
            started_at=doc[LABEL_STARTED_AT]
        )
    elif type_id == STATE_CANCELED:
        return StateCanceled(
            created_at=created_at,
            started_at=doc[LABEL_STARTED_AT],
            stopped_at=doc[LABEL_FINISHED_AT],
            messages=doc[LABEL_MESSAGES]
        )
    elif type_id == STATE_ERROR:
        return StateError(
            created_at=created_at,
            started_at=doc[LABEL_STARTED_AT],
            stopped_at=doc[LABEL_FINISHED_AT],
            messages=doc[LABEL_MESSAGES]
        )
    elif type_id == STATE_SUCCESS:
        return StateSuccess(
            created_at=created_at,
            started_at=doc[LABEL_STARTED_AT],
            finished_at=doc[LABEL_FINISHED_AT],
            files=doc[LABEL_FILES]
        )
    else:
        raise ValueError('invalid state type \'{}\''.format(type_id))


def serialize_state(state):
    """Create dictionary serialization if a given workflow state.

    Parameters
    ----------
    state: flowserv.model.workflow.state.WorkflowState
        Workflow state

    Returns
    -------
    dict
    """
    doc = {
        LABEL_STATE_TYPE: state.type_id,
        LABEL_CREATED_AT: state.created_at
    }
    if state.is_running():
        doc[LABEL_STARTED_AT] = state.started_at
    elif state.is_canceled() or state.is_error():
        doc[LABEL_STARTED_AT] = state.started_at
        doc[LABEL_FINISHED_AT] = state.stopped_at
        doc[LABEL_MESSAGES] = state.messages
    elif state.is_success():
        doc[LABEL_STARTED_AT] = state.started_at
        doc[LABEL_FINISHED_AT] = state.finished_at
        doc[LABEL_FILES] = state.files
    return doc
