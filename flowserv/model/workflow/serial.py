# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class to execute workflow templates that follow the syntax of the
REANA serial workflow specifications.
"""

from string import Template

from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.template.parameter as tp


class Step(object):
    """List of command line statements that are executed in a given
    environment. The environment can, for example, specify a Docker image.
    """
    def __init__(self, env, commands=None):
        """Initialize the object properties.

        Parameters
        ----------
        env: string
            Execution environment name
        commands: list(string), optional
            List of command line statements
        """
        self.env = env
        self.commands = commands if commands is not None else list()

    def add(self, cmd):
        """Append a given command line statement to the list of commands in the
        workflow step.

        Parameters
        ----------
        cmd: string
            Command line statement

        Returns
        -------
        flowserv.model.template.step.Step
        """
        self.commands.append(cmd)
        return self


class SerialWorkflow(object):
    """Wrapper around a workflow template for serial workflow specifications
    that are following the basic structure of REANA serial workflows.

    The methods to get the list of commands, output files and upload files are
    modeled as properties to avoid confusion with the same properties for the
    remote workflow handle.
    """
    def __init__(
        self, template: WorkflowTemplate, arguments: dict, sourcedir: str
    ):
        """Initialize the object properties.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations
        arguments: dict
            Dictionary of argument values for parameters in the template. Maps
            the parameter identifier to the provided argument value.
        sourcedir: string
            Path to the source directory for template files.
        """
        self.template = template
        self.arguments = arguments
        self.sourcedir = sourcedir

    def commands(self):
        """Get expanded commands from template workflow specification. The
        commands within each step of the serial workflow specification are
        expanded for the given set of arguments and appended to the result
        list of commands.

        Returns
        -------
        list(flowserv.model.template.step.Step)

        Raises
        ------
        flowserv.error.InvalidTemplateError
        flowserv.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        # Get the input parameters dictionary from the workflow specification
        # and replace all references to template parameters with the given
        # arguments or default values.
        workflow_parameters = tp.replace_args(
            spec=workflow_spec.get('inputs', {}).get('parameters', {}),
            arguments=self.arguments,
            parameters=self.template.parameters
        )
        # Add any workflow argument that is not contained in the modified
        # parameter list as a workflow parameter that is available for
        # replacement.
        for key in self.arguments:
            if key not in workflow_parameters:
                workflow_parameters[key] = str(self.arguments[key])
        # Add all command stings in workflow steps to result after replacing
        # references to parameters
        result = list()
        spec = workflow_spec.get('workflow', {}).get('specification', {})
        for step in spec.get('steps', []):
            env = tp.expand_value(
                value=step.get('environment'),
                arguments=workflow_parameters,
                parameters=self.template.parameters
            )
            script = Step(env=env)
            for cmd in step.get('commands', []):
                cmd = tp.expand_value(
                    value=cmd,
                    arguments=workflow_parameters,
                    parameters=self.template.parameters
                )
                script.add(Template(cmd).substitute(workflow_parameters))
            result.append(script)
        return result

    def output_files(self):
        """Replace references to template parameters in the list of output
        files in the workflow specification.

        Returns
        -------
        list(string)

        Raises
        ------
        flowserv.error.InvalidTemplateError
        flowserv.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        return tp.replace_args(
            spec=workflow_spec.get('outputs', {}).get('files', {}),
            arguments=self.arguments,
            parameters=self.template.parameters
        )
