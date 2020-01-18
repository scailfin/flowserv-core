# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The benchmark engine is responsible maintaining information about workflow
runs in the underlying database. Execution of workflows is controlled by a
given workflow controller implementation.
"""

from robcore.core.io.files import InputFile
from robcore.model.workflow.run import RunHandle
from robcore.model.template.schema import ResultSchema
from robcore.model.workflow.state import StatePending

import robcore.controller.run as store
import robcore.core.error as err
import robcore.core.util as util


class BenchmarkEngine(object):
    """The benchmark engine executes benchmark workflows for a given set of
    argument values. The state of workflow runs in maintained in the underlying
    relational database. WOrkflow execution is handled by a given workflow
    controller.
    """
    def __init__(self, con, backend):
        """Initialize the connection to the databases that contains the
        benchmark result tables and the workflow controller that is responsible
        for executing and managing workflow runs.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to the underlying database
        backend: robcore.controller.backend.base.WorkflowController
            Workflow controller that is responsible for workflow execution
        """
        self.con = con
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
        robcore.model.workflow.run.RunHandle

        Raises
        ------
        robcore.core.error.ConstraintViolationError
        robcore.core.error.UnknownRunError
        robcore.core.error.InvalidRunStateError
        """
        # Get the run handle. This will raise an error if the run is unknown
        run = self.get_run(run_id)
        # Raise an error if the run is not in an active state
        if not run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Cancel execution at the backend
        self.backend.cancel_run(run_id)
        # Update the run state and return the run handle
        messages = None
        if not reason is None:
            messages = list([reason])
        state = run.state.cancel(messages=messages)
        store.update_run(
            con=self.con,
            run_id=run_id,
            state=state
        )
        return RunHandle(
            identifier=run_id,
            submission_id=run.submission_id,
            state=state,
            arguments=run.arguments
        )

    def delete_run(self, run_id):
        """Delete the entry for the given run from the underlying database. This
        will also remove any run results and result file resources.

        Deleting a run will raise an error if the run is in an active state.

        Parameters
        ----------
        run_id: string
            Unique submission identifier

        Returns
        -------
        robcore.model.workflow.run.RunHandle

        Raises
        ------
        robcore.core.error.UnknownRunError
        robcore.core.error.InvalidRunStateError
        """
        # Get the handle for the runs to get the list of file resources that
        # have been created (if the run was executed successfully). This will
        # raise an error if the run is unknown.
        run = self.get_run(run_id)
        # If the run is active an error is raised. Since we use get_run() the
        # run state is already up to date.
        if run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Start by deleting all rows in the database that belong to the run.
        # Delete files after the database changes are committed since there is
        # no rollback option for file deletes.
        store.delete_run(con=self.con, run_id=run_id)
        # Delete all file resources.
        self.backend.remove_run(run_id)
        if run.is_success():
            for res in run.list_resources():
                # Don't raise an error if the file does not exist or cannot be
                # removed
                try:
                    res.delete()
                except OSError:
                    pass
        return run

    def exists_run(self, run_id):
        """Test if a run with the given identifier exists in the underlying
        database.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        bool
        """
        return store.exists_run(con=self.con, run_id=run_id)

    def get_run(self, run_id):
        """Get handle for the given run. The run state and associated submission
        and benchmark are read from the underlying database. If the runs state
        is active the backend is queried to eventually update the state in case
        it has changed since the last access.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        robcore.model.workflow.run.RunHandle

        Raises
        ------
        robcore.core.error.UnknownRunError
        """
        run = store.get_run(con=self.con, run_id=run_id)
        # If the run is in an active state and the backend does not update the
        # state state asynchronously we have to query the backend to see if
        # there has been a change to the run state.
        if not self.backend.asynchronous_events() and run.is_active():
            state = self.backend.get_run_state(run_id)
            # If the run state in the backend is different from the run state
            # in the database we update the database.
            if run.state.has_changed(state):
                run = store.update_run(con=self.con, run_id=run_id, state=state)
        return run

    def list_runs(self, submission_id):
        """Get a list of all runs for a given submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(robcore.model.workflow.run.RunHandle)
        """
        # Fetch list of run identifier for the submission from the database.
        # Each run is then loaded separately to ensure that he result has
        # updated run state information.
        sql = 'SELECT run_id FROM benchmark_run r WHERE submission_id = ?'
        result = list()
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            result.append(self.get_run(row['run_id']))
        return result

    def start_run(self, submission_id, arguments, template):
        """Run benchmark for a given submission with the given set of arguments.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        template: robcore.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations

        Returns
        -------
        robcore.model.workflow.run.RunHandle

        Raises
        ------
        robcore.core.error.MissingArgumentError
        robcore.core.error.UnknownSubmissionError
        """
        # Get the workflow template for the benchmark that is associated with
        # the given submission
        sql = 'SELECT benchmark_id FROM benchmark_submission WHERE submission_id = ?'
        row = self.con.execute(sql, (submission_id,)).fetchone()
        if row is None:
            raise err.UnknownSubmissionError(submission_id)
        # Create a unique run identifier
        run = store.create_run(
            con=self.con,
            submission_id=submission_id,
            arguments=arguments,
            commit_changes=False
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
            store.update_run(
                con=self.con,
                run_id=run_id,
                state=state,
                commit_changes=False
            )
        self.con.commit()
        return RunHandle(
            identifier=run_id,
            submission_id=submission_id,
            state=state,
            arguments=run.arguments
        )
