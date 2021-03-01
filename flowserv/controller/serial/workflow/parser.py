# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Parser for serial workflow templates."""

from typing import Dict, List, Tuple

from flowserv.model.workflow.step import ContainerStep
from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.template.parameter as tp


def parse_template(template: WorkflowTemplate, arguments: Dict) -> Tuple[List[ContainerStep], Dict, List[str]]:
    """Parse a serial workflow template to extract workflow steps and output
    files.

    Expands template parameter references in the workflow argument specification
    and returns the modified argument list as part of the result.

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
    for step in workflow_spec.get('workflow', {}).get('specification', {}).get('steps', []):
        # Workflow steps may either be parameter references or dictionaries
        # with `image` and `commands` elements.
        script = None
        if tp.is_parameter(step):
            para = template.parameters[tp.get_name(step)]
            if para.name in arguments:
                script = para.cast(arguments[para.name])
        else:
            script = ContainerStep(image=step.get('environment'))
            for cmd in step.get('commands', []):
                script.add(cmd)
        if script:
            steps.append(script)
    # Get the workflow arguments that are defined in the workflow template.
    # Expand template parameter references using the given argument set.
    run_args = workflow_spec.get('inputs', {}).get('parameters', {})
    for key in run_args.keys():
        run_args[key] = tp.expand_value(
            value=str(run_args[key]),
            arguments=arguments,
            parameters=template.parameters
        )
    # Get the list of output files from the workflow specification. At this
    # point we do not support references to template arguments or parameters.
    output_files = workflow_spec.get('outputs', {}).get('files', {})
    # Return tuple of workflow steps and output file list.
    return steps, run_args, output_files
