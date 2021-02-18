# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.


def commands(self) -> List[ContainerStep]:
    """Get expanded commands from template workflow specification. The
    commands within each step of the serial workflow specification are
    expanded for the given set of arguments and appended to the result
    list of commands.

    Returns
    -------
    list(flowserv.model.workflow.serial.Step)

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
        script = ContainerStep(env=env)
        for cmd in step.get('commands', []):
            cmd = tp.expand_value(
                value=cmd,
                arguments=workflow_parameters,
                parameters=self.template.parameters
            )
            script.add(Template(cmd).substitute(workflow_parameters))
        result.append(script)
    return result

def output_files(self) -> List[str]:
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
