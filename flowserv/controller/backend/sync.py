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

from flowserv.controller.backend.base import WorkflowController

import flowserv.core.error as err
import flowserv.controller.serial as serial
import flowserv.core.util as util
import flowserv.model.workflow.state as serialize


class SyncWorkflowEngine(WorkflowController):
    """Workflow controller that implements a workflow engine that executes each
    workflow run synchronously. The engine maintains workflow files in
    directories under a given base directory. Information about workflow
    results are stored in files that are named using the run identifier.

    This implementation of the workflow engine expects a workflow specification
    that follow the syntax of REANA serial workflows.
    """
    def __init__(self, basedir, verbose=False):
        """Initialize the base directory. Workflow runs are maintained in
        sub-directories in this base directory (named by the run identifier).
        Workflow results are kept as files in the base directory.

        Parameters
        ----------
        basedir: string
            Path to the base directory
        verbose: bool, optional
            Print command strings to STDOUT during workflow execution
        """
        # Set base directory and ensure that it exists
        self.basedir = util.create_dir(basedir)
        self.verbose = verbose

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
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState

        Raises
        ------
        flowserv.core.error.DuplicateRunError
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
        # Expand template parameters. Get (i) list of files that need to be
        # copied, (ii) the expanded commands that represent the workflow steps,
        # and (iii) the list of output files.
        files = serial.upload_files(template, arguments)
        steps = serial.commands(template, arguments)
        output_files = serial.output_files(template, arguments)
        # Copy workflow files and then execute the workflow synchronously.
        util.copy_files(files=files, target_dir=run_dir)
        state = serial.run(
            run_dir=run_dir,
            steps=steps,
            output_files=output_files,
            verbose=self.verbose
        )
        # Write the resulting state to disk before returning.
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
        return os.path.join(self.basedir, run_id)

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
        return os.path.join(self.basedir, run_id + '.json')

    def get_run_state(self, run_id):
        """Get the status of the workflow with the given identifier.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        run_file = self.get_run_file(run_id)
        if os.path.isfile(run_file):
            doc = util.read_object(filename=run_file)
            return serialize.deserialize_state(doc)
        else:
            raise err.UnknownRunError(run_id)

    def modify_template(self, workflow_spec, tmpl_parameters, add_parameters):
        """Modify a given workflow specification by adding the given parameters
        to a given set of template parameters.

        This function is dependent on the workflow specification syntax that is
        supported by a workflow engine.

        Returns the modified workflow specification and the modified parameter
        index. Raises an error if the parameter identifier in the resulting
        parameter index are no longer unique.

        Parameters
        ----------
        workflow_spec: dict
            Workflow specification
        tmpl_parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Existing template parameters
        add_parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        dict, dict(flowserv.model.parameter.base.TemplateParameter)

        Raises
        ------
        flowserv.core.error.DuplicateParameterError
        flowserv.core.error.InvalidTemplateError
        """
        return serial.modify_spec(
            workflow_spec=workflow_spec,
            tmpl_parameters=tmpl_parameters,
            add_parameters=add_parameters
        )

    def remove_run(self, run_id):
        """Remove all files and directories that belong to the run with the
        given identifier.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        run_dir = self.get_run_dir(run_id)
        if os.path.isdir(run_dir):
            shutil.rmtree(run_dir)
        else:
            raise err.UnknownRunError(run_id)
        run_file = self.get_run_file(run_id)
        if os.path.isfile(run_file):
            os.remove(run_file)
