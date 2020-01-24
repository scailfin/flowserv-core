# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow engine is responsible maintaining information about workflow
runs in the underlying database. Execution of workflows is controlled by a
given workflow controller.
"""

from flowserv.model.run.base import RunHandle

import flowserv.controller.run as store
import flowserv.core.error as err


class WorkflowEngine(object):
    """The workflow engine is a lightweight wrapper around a workflow
    controller that is responsible for workflow execution. The state of
    workflow runs in maintained in the underlying relational database.
    """
    def __init__(self, runstore, backend):
        """Initialize the workflow run manager and the backend that is
        responsible for workflow execution.

        Parameters
        ----------
        runstore: flowserv.model.run.manager.RunManager
            Manager that maintains run information in the underlying database
        backend: flowserv.controller.backend.base.WorkflowController
            Workflow controller that is responsible for workflow execution
        """
        self.runstore = runstore
        self.backend = backend

    def cancel_run(self, run_id, reason=None):
        """Cancel the given run. This will raise an error if the run is not in
        an active state.

        Parameters
        ----------
        run_id: string
            Unique submission identifier
        reason: string, optional
            Optional text describing the reason for cancelling the run

        Returns
        -------
        flowserv.model.run.base.RunHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.InvalidRunStateError
        """
        # Get the run handle. This will raise an error if the run is unknown
        run = self.runstore.get_run(run_id)
        # Raise an error if the run is not in an active state
        if not run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Cancel execution at the backend
        self.backend.cancel_run(run_id)
        # Update the run state and return the run handle
        messages = None
        if reason is not None:
            messages = list([reason])
        state = run.state.cancel(messages=messages)
        self.runstore.update_run(
            run_id=run_id,
            state=state
        )
        return run.update_state(state)

    def delete_run(self, run_id):
        """Delete the entry for the given run from the underlying database.
        This will also remove any run results and result file resources.

        Deleting a run will raise an error if the run is in an active state.

        Parameters
        ----------
        run_id: string
            Unique submission identifier

        Raises
        ------
        flowserv.core.error.UnknownRunError
        flowserv.core.error.InvalidRunStateError
        """
        # Get the handle for the run to raise an error if the run is still
        # active. This will also raise an error if the run is unknown.
        run = self.runstore.get_run(run_id)
        if run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Use the run manager to delete the run from the underlying database
        # and to delete all run files
        self.runstore.delete_run(run_id)

    def start_run(self, workflow_id, group_id, arguments, template):
        """Run benchmark for a given submission with the given set of arguments.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations

        Returns
        -------
        flowserv.model.run.base.RunHandle

        Raises
        ------
        flowserv.core.error.MissingArgumentError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Create a unique run identifier
        run = self.runstore.create_run(
            workflow_id=workflow_id,
            group_id=group_id,
            arguments=arguments
        )
        run_id = run.identifier
        # Execute the benchmark workflow for the given set of arguments.
        state = self.backend.exec_workflow(
            run_id=run_id,
            template=template,
            arguments=arguments
        )
        # Update the run state if it is no longer pending for execution.
        if not state.is_pending():
            self.runstore.update_run(
                run_id=run_id,
                state=state
            )
        return RunHandle(
            identifier=run_id,
            group_id=group_id,
            state=state,
            arguments=run.arguments
        )
