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

from flowserv.model.template.schema import ResultColumn, ResultSchema
from flowserv.controller.base import WorkflowController
from flowserv.controller.serial.engine import SerialWorkflowEngine

import flowserv.core.error as err
import flowserv.model.parameter.declaration as pd
import flowserv.model.workflow.state as st


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
    """Workflow controller for test purposes. Maintains a dictionary of run
    states. Allows to modify the state of maintained runs
    """
    def __init__(self):
        """Initialize the run index."""
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
        if run_id not in self.runs:
            raise err.UnknownRunError(run_id)

    def configuration(self):
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        return list()

    def error(self, run_id, messages=None):
        """Set the run with the given identifier into error state.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        messages: list(string), optional
            Default error messages

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        state = self.runs[run_id].error(messages=messages)
        self.runs[run_id] = state
        return state

    def exec_workflow(self, run, template, arguments):
        """Fake execute method that returns the workflow state that the was
        provided when the object was instantiated. Ignores all given arguments.

        Parameters
        ----------
        run: flowserv.model.run.base.RunHandle
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        state = st.StatePending()
        self.runs[run.identifier] = state
        return state

    def get_run(self, run_id):
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

    def start(self, run_id):
        """Set the run with the given identifier into running state. Returns
        the modified workflow state.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        state = self.runs[run_id].start()
        self.runs[run_id] = state
        return state

    def success(self, run_id, resources=None):
        """Set the default state to SUCCESS.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        resources: list(flowserv.model.workflow.resource.WorkflowResource), optional
            List of created resource files

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        state = self.runs[run_id].success(resources=resources)
        self.runs[run_id] = state
        return state
