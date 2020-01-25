# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class to execute workflow templates that follow the syntax of the
REANA serial workflow specifications.
"""

from string import Template

from flowserv.controller.serial.step import Step
import flowserv.model.template.parameter as tp


class SerialWorkflow(object):
    """Wrapper around a workflow template for serial workflow specifications
    that are following the basic structure of REANA serial workflows.
    """
    def __init__(self, template, arguments):
        """Initialize the object properties.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        """
        self.template = template
        self.arguments = arguments

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
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        # Get the input/parameters dictionary from the workflow specification
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
                workflow_parameters[key] = self.arguments[key].get_value()
        # Add all command stings in workflow steps to result after replacing
        # references to parameters
        result = list()
        spec = workflow_spec.get('workflow', {}).get('specification', {})
        for step in spec.get('steps', []):
            env = step.get('environment')
            if tp.is_parameter(env):
                env = workflow_parameters[tp.NAME(env)]
            script = Step(env=env)
            for cmd in step.get('commands', []):
                if tp.is_parameter(cmd):
                    cmd = workflow_parameters[tp.NAME(cmd)]
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
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        return tp.replace_args(
            spec=workflow_spec.get('outputs', {}).get('files', {}),
            arguments=self.arguments,
            parameters=self.template.parameters
        )

    def upload_files(self):
        """Get a list of all input files from the workflow specification that
        need to be uploaded for a new workflow run. This is a wrapper around
        the generic get_upload_files function, specific to the workflow
        template syntax that is supported for serial workflows.

        Returns a list of tuples containing the full path to the source file on
        local disk and the relative target path for the uploaded file.

        Raises errors if (i) an unknown parameter is referenced or (ii) if the
        type of a referenced parameter in the input files section is not of
        type file.

        Returns
        -------
        list((string, string))

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.MissingArgumentError
        flowserv.core.error.UnknownParameterError
        """
        workflow_spec = self.template.workflow_spec
        return tp.get_upload_files(
            template=self.template,
            basedir=self.template.sourcedir,
            files=workflow_spec.get('inputs', {}).get('files', []),
            arguments=self.arguments,
        )
