# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Parser for serial workflow templates."""

from typing import Dict, List, Tuple

from flowserv.model.workflow.step import CodeStep, ContainerStep
from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.template.parameter as tp
import flowserv.util as util


def parse_template(template: WorkflowTemplate, arguments: Dict) -> Tuple[List[ContainerStep], Dict, List[str]]:
    """Parse a serial workflow template to extract workflow steps and output
    files.

    The expected schema of the workflow specification is as follows:

    .. code-block:: yaml

        workflow:
            files:
                inputs:
                - "str"
                outputs:
                - "str"
            parameters:
            - name: "scalar"
            steps:
            - name: "str"
              files:
                inputs:
                - "str"
                outputs:
                - "str"
              action: "object depending on the step type"


    The schema for the action specification for a workflow step is dependent on
    the step type. For container steps, the expected schema is:

    .. code-block:: yaml

        action:
            environment: "str"
            commands:
            - "str"

    Expands template parameter references in the workflow argument specification
    and the step inputs list. Returns the modified argument list as part of the
    result.

    Parameters
    ----------
    template: flowserv.model.template.base.WorkflowTemplate
        Template for a serial workflow.

    Returns
    -------
    tuple of list of flowsert.controller.serial.workflow.step.ContainerStep, dict and list of string
    """
    # Get the commands from the workflow specification.
    workflow_spec = template.workflow_spec
    steps = list()
    for doc in workflow_spec.get('steps', []):
        # For each workflow step we expect the elements 'name' and 'action' as
        # well as an optional specification of the input and output files.
        step_id = doc['name']
        action = doc['action']
        input_files = doc.get('files', {}).get('inputs')
        output_files = doc.get('files', {}).get('outputs')
        # The action may either be a reference to an input parameter for the
        # workflow step or a dictionary.
        step = None
        if isinstance(action, str):
            # If the action references a parameter we replace the action object
            # with the parameter value.
            para = template.parameters[tp.get_name(action)]
            if para.name not in arguments and not para.required:
                # Skip this step if no parameter value was provided and is not
                # a required parameter (step).
                continue
            action = para.cast(value=arguments[para.name])
        # If the action is a dictionary, the type of the generated workflow
        # step will depend on the elements in that dictionary.
        if 'environment' in action and 'commands' in action:
            # If the dictionary contains `environment` and `commands` the result
            # is a container step.
            step = ContainerStep(
                identifier=step_id,
                image=action.get('environment'),
                commands=action.get('commands', []),
                inputs=input_files,
                outputs=output_files
            )
        elif 'func' in action:
            step = CodeStep(
                identifier=step_id,
                func=util.import_obj(action['func']),
                arg=action.get('arg'),
                varnames={doc['arg']: doc['var'] for doc in action.get('vars', [])},
                inputs=input_files,
                outputs=output_files
            )
        else:
            raise ValueError(f"invalid action specification '{action}'")
        steps.append(step)
    # Get the workflow arguments that are defined in the workflow template.
    # Expand template parameter references using the given argument set.
    run_args = workflow_spec.get('parameters', {})
    for key in run_args.keys():
        run_args[key] = tp.expand_value(
            value=str(run_args[key]),
            arguments=arguments,
            parameters=template.parameters
        )
    # Return tuple of workflow steps and output file list.
    return steps, run_args, workflow_spec.get('files', {}).get('outputs', {})
