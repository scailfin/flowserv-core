# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implemenation of the workflow engine interface. Executes all workflows
synchronously. Primarily intended for debugging and test purposes.
"""

import os
import shutil

from robcore.model.workflow.controller import WorkflowController
from robcore.model.workflow.resource import FileResource
from robcore.model.workflow.serial import SerialWorkflow

import robcore.error as err
import robcore.model.workflow.state as state
import robcore.util as util


class SyncWorkflowEngine(WorkflowController):
    """Workflow controller that implements a workflow engine that executes each
    workflow run synchronously. The engine maintains workflow files in
    directories under a given base directory. Information about workflow results
    are stored in files that are named using the run identifier.

    This implementation of the workflow engine expects a workflow specification
    that follow the syntax of REANA serial workflows.
    """
    def __init__(self, base_dir):
        """Initialize the base directory. Workflow runs are maintained in
        sub-directories in this base directory (named by the run identifier).
        Workflow results are kept as files in the base directory.

        Parameters
        ----------
        base_dir: string
            Path to the base directory
        """
        # Set base directory and ensure that it exists
        self.base_dir = util.create_dir(base_dir)

    def asynchronous_events(self):
        """All executed workflows will be in an inactive state. The synchronous
        workflow controller therefore has no need to update the database.

        Returns
        -------
        bool
        """
        return False

    def cancel_run(self, run_id):
        """Request to cancel execution of the given run. Since all runs are
        executed synchronously they cannot be canceled.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        """
        pass

    def exec_workflow(self, run_id, template, arguments):
        """Initiate the execution of a given workflow template for a set of
        argument values. Returns the state of the workflow.

        The client provides a unique identifier for the workflow run that is
        being used to retrieve the workflow state in future calls.

        Parameters
        ----------
        run_id: string
            Unique identifier for the workflow run.
        template: robcore.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robcore.model.workflow.state.WorkflowState

        Raises
        ------
        robcore.error.DuplicateRunError
        """
        # Create run folder and run state file. If either of the two exists we
        # assume that the given run identifier is not unique.
        run_file = self.get_run_file(run_id)
        if os.path.isfile(run_file):
            raise err.DuplicateRunError(run_id)
        run_dir = self.get_run_dir(run_id)
        if os.path.isdir(run_dir):
            raise err.DuplicateRunError(run_id)
        os.makedirs(run_dir)
        # Execute the workflow synchronously and write the resulting state to
        # disk before returning
        wf = SerialWorkflow(template, arguments)
        state = wf.run(run_dir, verbose=True)
        util.write_object(
            filename=run_file,
            obj=serialize_state(state)
        )
        return state

    def get_run_dir(self, run_id):
        """Get the path to directory that stores the run files.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, run_id)

    def get_run_file(self, run_id):
        """Get the path to file that stores the run results.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, run_id + '.json')

    def get_run_state(self, run_id):
        """Get the status of the workflow with the given identifier.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        robcore.model.workflow.state.WorkflowState

        Raises
        ------
        robcore.error.UnknownRunError
        """
        run_file = self.get_run_file(run_id)
        if os.path.isfile(run_file):
            doc = util.read_object(filename=run_file)
            return deserialize_state(doc)
        else:
            raise err.UnknownRunError(run_id)

    def remove_run(self, run_id):
        """Remove all files and directories that belong to the run with the
        given identifier.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        robcore.error.UnknownRunError
        """
        run_dir = self.get_run_dir(run_id)
        if os.path.isdir(run_dir):
            shutil.rmtree(run_dir)
        else:
            raise err.UnknownRunError(run_id)
        run_file = self.get_run_file(run_id)
        if os.path.isfile(run_file):
            os.remove(run_file)


# -- Serialization/Deserialization helper methods ------------------------------

"""Labels for serialization."""
LABEL_CREATED_AT = 'createdAt'
LABEL_FILEPATH = 'filename'
LABEL_FINISHED_AT = 'finishedAt'
LABEL_ID = 'id'
LABEL_MESSAGES = 'messages'
LABEL_NAME = 'name'
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
    robcore.model.workflow.state.WorkflowState

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
            resource_name = obj[LABEL_NAME]
            res = FileResource(
                resource_id=obj[LABEL_ID],
                resource_name=resource_name,
                file_path=obj[LABEL_FILEPATH]
            )
            files[resource_name] = res
        return state.StateSuccess(
            created_at=created_at,
            started_at=util.to_datetime(doc[LABEL_STARTED_AT]),
            finished_at=util.to_datetime(doc[LABEL_FINISHED_AT]),
            files=files
        )
    else:
        raise ValueError('invalid state type \'{}\''.format(type_id))


def serialize_state(state):
    """Create dictionary serialization if a given workflow state.

    Parameters
    ----------
    state: robcore.model.workflow.state.WorkflowState
        Workflow state

    Returns
    -------
    dict
    """
    doc = {
        LABEL_STATE_TYPE: state.type_id,
        LABEL_CREATED_AT: state.created_at.isoformat()
    }
    if state.is_running():
        doc[LABEL_STARTED_AT] = state.started_at.isoformat()
    elif state.is_canceled() or state.is_error():
        doc[LABEL_STARTED_AT] = state.started_at.isoformat()
        doc[LABEL_FINISHED_AT] = state.stopped_at.isoformat()
        doc[LABEL_MESSAGES] = state.messages
    elif state.is_success():
        doc[LABEL_STARTED_AT] = state.started_at.isoformat()
        doc[LABEL_FINISHED_AT] = state.finished_at.isoformat()
        doc[LABEL_RESOURCES] = [
            {
                LABEL_ID: f.resource_id,
                LABEL_NAME: f.resource_name,
                LABEL_FILEPATH: f.file_path
            } for f in state.files.values()
        ]
    return doc
