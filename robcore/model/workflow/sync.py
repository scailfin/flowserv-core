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
from robcore.model.workflow.serial import SerialWorkflow

import robcore.error as err
import robcore.util as util
import robcore.model.workflow.state as serialize


class SyncWorkflowEngine(WorkflowController):
    """Workflow controller that implements a workflow engine that executes each
    workflow run synchronously. The engine maintains workflow files in
    directories under a given base directory. Information about workflow results
    are stored in files that are named using the run identifier.

    This implementation of the workflow engine expects a workflow specification
    that follow the syntax of REANA serial workflows.
    """
    def __init__(self, base_dir):
        """Initialize the base directory. Workflow runs are maintained a
        sub-directories in the base directory (named by the run identifier).
        Workflow results are kept as files in the base directory.

        Parameters
        ----------
        base_dir: string
            Path to the base directory
        """
        # Set base directory and ensure that it exists
        self.base_dir = util.create_dir(base_dir)

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
            obj=serialize.serialize_state(state)
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
            return serialize.deserialize_state(doc)
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
