# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper functions to execute workflow templates that follow the syntax of the
REANA serial workflow specifications.

The functions in this module can be used by different implementations of the
workflow engine (e.g., for test purposes).
"""

import os
import subprocess

from datetime import datetime
from string import Template

from robcore.controller.command import Command
from robcore.model.workflow.resource import FileResource
from robcore.model.workflow.state import StateError, StateSuccess

import robcore.error as err
import robcore.model.template.util as tmpl
import robcore.controller.io as fileio
import robcore.util as util


def commands(template, arguments):
    """Get expanded commands from template workflow specification. The
    commands within each step of the serial workflow specification are
    expanded for the given set of arguments and appended to the result
    list of commands.

    Parameters
    ----------
    template: robcore.model.template.base.WorkflowTemplate
        Workflow template containing the parameterized specification and the
        parameter declarations
    arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
        Dictionary of argument values for parameters in the template

    Returns
    -------
    list(robcore.controller.command.Command)

    Raises
    ------
    robcore.error.InvalidTemplateError
    robcore.error.MissingArgumentError
    """
    workflow_spec = template.workflow_spec
    # Get the input/parameters dictionary from the workflow specification and
    # replace all references to template parameters with the given arguments
    # or default values
    workflow_parameters = tmpl.replace_args(
        spec=workflow_spec.get('inputs', {}).get('parameters', {}),
        arguments=arguments,
        parameters=template.parameters
    )
    # Add any workflow argument that is not contained in the modified parameter
    # list as a workflow parameter that is available for replacement.
    for key in arguments:
        if not key in workflow_parameters:
            workflow_parameters[key] = arguments[key].get_value()
    # Add all command stings in workflow steps to result after replacing
    # references to parameters
    result = list()
    spec = workflow_spec.get('workflow', {}).get('specification', {})
    for step in spec.get('steps', []):
        env = step.get('environment')
        if tmpl.is_parameter(env):
            env = workflow_parameters[tmpl.get_parameter_name(env)]        
        command = Command(env=env)
        for cmd in step.get('commands', []):
            if tmpl.is_parameter(cmd):
                cmd = workflow_parameters[tmpl.get_parameter_name(cmd)]
            command.add(Template(cmd).substitute(workflow_parameters))
        result.append(command)
    return result


def modify_spec(workflow_spec, tmpl_parameters, add_parameters):
    """Modify a given workflow specification by adding the given parameters
    to a given set of template parameters. If a parameter in the add_parameters
    list already exists the name, index, default value, the value list and the
    required flag of the existing are overwritten by the values of the new
    parameter.

    Returns the modified workflow specification and the modified parameter
    index. Raises an error if the parameter identifier in the resulting
    parameter index are no longer unique.

    Parameters
    ----------
    workflow_spec: dict
        Workflow specification
    tmpl_parameters: dict(robcore.model.template.parameter.base.TemplateParameter)
        Existing template parameters
    add_parameters: dict(robcore.model.template.parameter.base.TemplateParameter)
        Additional template parameters

    Returns
    -------
    dict, dict(robcore.model.template.parameter.base.TemplateParameter)

    Raises
    ------
    robcore.error.InvalidTemplateError
    """
    # Get a copy of the files and parameters sections of the inputs declaration
    inputs = workflow_spec.get('inputs', dict())
    in_files = list(inputs.get('files', list()))
    in_params = dict(inputs.get('parameters', dict()))
    # Ensure that the identifier for all parameters are unique
    para_merge = dict(tmpl_parameters)
    for para in add_parameters.values():
        if para.identifier in para_merge:
            para = para_merge[para.identifier].merge(para)
        para_merge[para.identifier] = para
        # Depending on whether the type of the parameter is a file or not we
        # add a parameter reference to the respective input section
        if para.is_file():
            in_files.append('$[[{}]]'.format(para.identifier))
        else:
            if para.identifier not in in_params:
                in_params[para.identifier] = '$[[{}]]'.format(para.identifier)
    spec = dict(workflow_spec)
    spec['inputs'] = {'files': in_files, 'parameters': in_params}
    return spec, para_merge

def run(run_dir, commands, output_files, verbose=False):
    """Run serial workflow commands. Expects all workflow files in the given run
    directory. Executes each command in the given order. Returns the list of
    generated file resources for successful workflow runs.

    Parameters
    ----------
    run_dir: string
        Base directory for all workflow run files
    commands: list(robcore.controller.command.Command)
        List of expanded commands from a template workflow specification
    output_files: list(string)
        Relative path of output files that are generated by the workflow run
    verbose: bool, optional
        Output executed commands if flag is True

    Returns
    -------
    robcore.model.workflow.state.WorkflowState
    """
    # The serial controller ignores the command environments. We start by
    # creating a list of all command statements
    statements = list()
    for cmd in commands:
        statements.extend(cmd.commands)
    # Run workflow step-by-step
    ts_start = datetime.utcnow()
    for cmd in statements:
        # Print command if verbose
        if verbose:
            print(cmd)
        # Each command is expected to be a shell command that is executed
        # using the subprocess package. The subprocess.run() method is
        # preferred for capturing output to STDERR but it does not exist
        # in Python2.
        try:
            proc = subprocess.run(
                cmd,
                cwd=run_dir,
                shell=True,
                capture_output=True
            )
            if proc.returncode != 0:
                # Return error state. Include STDERR in result
                messages = list()
                messages.append(proc.stderr.decode('utf-8'))
                return StateError(created_at=ts_start, messages=messages)
        except (AttributeError, TypeError) as e:
            try:
                subprocess.check_output(
                    cmd,
                    cwd=run_dir,
                    shell=True,
                    stderr=subprocess.STDOUT
                )
            except subprocess.CalledProcessError as ex:
                return StateError(
                    created_at=ts_start,
                    messages=[str(ex)]
                )
    ts_end = datetime.utcnow()
    # Create dictionary of output files
    files = dict()
    for resource_name in output_files:
        files[resource_name] = FileResource(
            resource_id=util.get_unique_identifier(),
            resource_name=resource_name,
            file_path=os.path.join(run_dir, resource_name)
        )
    # Workflow executed successfully
    return StateSuccess(
        created_at=ts_start,
        started_at=ts_start,
        finished_at=ts_end,
        files=files
    )


def output_files(template, arguments):
    """Replace references to template parameters in the list of output files
    in the workflow specification.

    Parameters
    ----------
    template: robcore.model.template.base.WorkflowTemplate
        Workflow template containing the parameterized specification and the
        parameter declarations
    arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
        Dictionary of argument values for parameters in the template

    Returns
    -------
    list(string)

    Raises
    ------
    robcore.error.InvalidTemplateError
    robcore.error.MissingArgumentError
    """
    return tmpl.replace_args(
        spec=template.workflow_spec.get('outputs', {}).get('files', {}),
        arguments=arguments,
        parameters=template.parameters
    )


def upload_files(template, arguments):
    """Get a list of all input files for a workflow template that need to be
    uploaded for a new workflow run. This is a wrapper around to the generic
    get_upload_files function, specific to the workflow template syntax that
    is supported for serial workflows.

    Returns a list of tuples containing the full path to the source file on
    local disk and the relative target path for the uploaded file.

    Raises errors if (i) an unknown parameter is referenced or (ii) if the type
    of a referenced parameter in the input files section is not of type file.

    Parameters
    ----------
    template: robcore.model.template.base.WorkflowTemplate
        Workflow template containing the parameterized specification and the
        parameter declarations
    arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
        Dictionary of argument values for parameters in the template

    Returns
    -------
    list((string, string))

    Raises
    ------
    robcore.error.InvalidTemplateError
    robcore.error.MissingArgumentError
    robcore.error.UnknownParameterError
    """
    return fileio.get_upload_files(
        template=template,
        base_dir=template.source_dir,
        files=template.workflow_spec.get('inputs', {}).get('files', []),
        arguments=arguments,
    )
