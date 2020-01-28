# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods and classes for unit test for components of the benchmark
modules.
"""

import os

from flowserv.model.template.schema import ResultColumn, ResultSchema
from flowserv.controller.base import WorkflowController
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.model.workflow.resource import FSObject

import flowserv.core.error as err
import flowserv.model.parameter.declaration as pd
import flowserv.model.workflow.state as st
import flowserv.core.util as util


"""Result schema for the default benchmark."""
RESULT_FILE_ID = 'results.json'
BENCHMARK_SCHEMA = ResultSchema(
    result_file=RESULT_FILE_ID,
    columns=[
        ResultColumn('col1', 'col1', pd.DT_INTEGER),
        ResultColumn('col2', 'col2', pd.DT_DECIMAL),
        ResultColumn('col3', 'col3', pd.DT_STRING, required=False)
    ]
)


class StateEngine(WorkflowController):
    """Fake workflow engine. Returns the given workflow state when the execute
    method is called.
    """
    def __init__(
        self, state=None, messages=None, values=None, result_file=None,
        basedir=None, asynchronous_events=None
    ):
        """Initialize the workflow state that the engine is returning for all
        executed workflows. If the state is ERROR or CANCELED the given error
        messages will be part of the state object. If the state is SUCCESS the
        resulting run state will contain a result file that is generated from
        the values and result_file arguments (if given).

        Parameters
        ----------
        state: string, optional
            Identifier of the workflow state type
        messages: list(string), optional
            Optional list of error messages
        values: dict, optional
            Optional dictionary with result values
        result_file: string, optional
            Identifier for result file resource for successful workfkow runs
        basedir: string, optional
            Directory where the result file will be stored.
        asynchronous_events: bool, optional
            Flag indicating whether the controller is updating the underlying
            database asynchronously or not
        """
        self.state = state if not state is None else st.STATE_PENDING
        self.messages = messages
        self.values = values if not values is None else {'col1': 1, 'col2': 1.1, 'col3': 'R0'}
        self.result_file = result_file if not result_file is None else RESULT_FILE_ID
        if not basedir is None:
            self.basedir = util.create_dir(basedir)
        else:
            self.basedir = None
        self._async_events = asynchronous_events if not asynchronous_events is None else False
        # Index of workflow runs
        self.runs = dict()

    def cancel_run(self, run_id):
        """Request to cancel execution of the given run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        if run_id in self.runs:
            run = self.runs[run_id]
            if run.is_active():
                self.runs[run_id] = run.cancel(messages=self.messages)
        else:
            raise err.UnknownRunError(run_id)

    def configuration(self):
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        return list()

    def error(self, messages=None):
        """Set the default state to ERROR.

        Parameters
        ----------
        messages: list(string), optional
            Default error messages

        Returns
        -------
        flowserv.tests.benchmark.StateEngine
        """
        self.state = st.STATE_ERROR
        if not messages is None:
            self.messages = messages
        return self

    def exec_workflow(self, run_id, template, arguments):
        """Fake execute method that returns the workflow state that the was
        provided when the object was instantiated. Ignores all given arguments.

        Parameters
        ----------
        run_id: string
            Unique identifier for the workflow run.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        if self.state == st.STATE_PENDING:
            run = st.StatePending()
        elif self.state == st.STATE_RUNNING:
            run = st.StatePending().start()
        elif self.state == st.STATE_ERROR:
            run = st.StatePending().error(messages=self.messages)
        elif self.state == st.STATE_CANCELED:
            run = st.StatePending().cancel(messages=self.messages)
        else:
            if not self.values is None:
                filename = util.get_unique_identifier() + '.json'
                result_file = os.path.join(self.basedir, filename)
                util.write_object(filename=result_file, obj=self.values)
                resource_name = self.result_file
                f = FSObject(
                    resource_id=util.get_unique_identifier(),
                    resource_name=resource_name,
                    file_path=result_file
                )
                files = {resource_name: f}
            else:
                files = None
            run = st.StatePending().start().success(files=files)
        self.runs[run_id] = run
        return run

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
        if run_id in self.runs:
            return self.runs[run_id]
        else:
            raise err.UnknownRunError(run_id)

    def modify_template(self, template, parameters):
        """Modify a the workflow specification in a given template by adding
        the a set of parameters. If a parameter in the added parameters set
        already exists in the template the name, index, default value, the
        value list and the required flag of the existing parameter are replaced
        by the values of the given parameter.

        Returns a modified workflow template. Raises an error if the parameter
        identifier in the resulting template are no longer unique.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template handle.
        parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        """
        return SerialWorkflowEngine().modify_template(template, parameters)

    def start(self):
        """Set the default state to RUNNING.

        Returns
        -------
        flowserv.tests.benchmark.StateEngine
        """
        self.state = st.STATE_RUNNING
        return self

    def success(self, values=None):
        """Set the default state to SUCCESS.

        Parameters
        ----------
        values: dict, optional
            Default values for the result file

        Returns
        -------
        flowserv.tests.benchmark.StateEngine
        """
        self.state = st.STATE_SUCCESS
        if not values is None:
            self.values = values
        return self
