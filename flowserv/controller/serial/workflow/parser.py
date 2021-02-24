# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Parser for serial workflow templates."""

from typing import List, Tuple

from flowserv.controller.serial.workflow.step import ContainerStep
from flowserv.model.template.base import WorkflowTemplate


def parse_template(template: WorkflowTemplate) -> Tuple[List[ContainerStep], List[str]]:
    """Parse a serial workflow template to extract workflow steps and output
    files.

    Parameters
    ----------
    template: flowserv.model.template.base.WorkflowTemplate
        Template for a serial workflow.

    Returns
    -------
    tuple of list of flowsert.controller.serial.workflow.step.ContainerStep and list of string
    """
    # Get the commands from the workflow specification.
    workflow_spec = template.workflow_spec
    steps = list()
    for step in workflow_spec.get('workflow', {}).get('specification', {}).get('steps', []):
        script = ContainerStep(image=step.get('environment'))
        for cmd in step.get('commands', []):
            script.add(cmd)
        steps.append(script)
    # Get the list of output files from the workflow specification. At this
    # point we do not support references to template arguments or parameters.
    output_files = workflow_spec.get('outputs', {}).get('files', {})
    # Return tuple of workflow steps and output file list.
    return steps, output_files
