# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Wrapper for workflow templates that follow the syntax of REANA serial
workflow specifications.

This is a helper class that can be used by different implementations of the
workflow engine (e.g., for test purposes).
"""

import os
import subprocess

from datetime import datetime
from string import Template

from robcore.model.workflow.resource import FileResource
from robcore.model.workflow.state.base import StateError, StateSuccess

import robcore.model.template.util as tmpl
import robcore.model.workflow.io as fileio


class SerialWorkflow(object):
    """Instance of a workflow template that follows the REANA serial workflow
    syntax. This class is a wrapper for the parameterized workflow template as
    well as a set of argument values for the template parameters. The class
    methods provide functionality to execute the workflow synchronously
    step-by-step.
    """
    def __init__(self, template, arguments):
        """Initialize the workflow template and dictionary of argument values.

        template: robcore.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        """
        self.template = template
        self.arguments = arguments
        # Shortcut to the workflow parameters and specification
        self.parameters = template.parameters
        self.workflow_spec = template.workflow_spec

    def copy_files(self, source_dir, target_dir):
        """Copy all code and input files to the target directory.

        Parameters
        ----------
        source_dir: string
            Source directory that contains the static template files
        target_dir: string
            Target directory for copied files (e.g., base directory for a
            workflow run)
        """
        fileio.upload_files(
            template=self.template,
            base_dir=source_dir,
            files=self.workflow_spec.get('inputs', {}).get('files', []),
            arguments=self.arguments,
            loader=fileio.FileCopy(target_dir)
        )

    def get_commands(self):
        """Get expanded commands from template workflow specification. The
        commands within each step of the serial workflow specification are
        expanded for the given set of arguments and appended to the result
        list of commands.

        Returns
        -------
        list(string)

        Raises
        ------
        robcore.error.MissingArgumentError
        """
        # Get the input/parameters dictionary from the workflow specification and
        # replace all references to template parameters with the given arguments
        # or default values
        workflow_parameters = tmpl.replace_args(
            spec=self.workflow_spec.get('inputs', {}).get('parameters', {}),
            arguments=self.arguments,
            parameters=self.parameters
        )
        # Add all command stings in workflow steps to result after replacing
        # references to parameters
        result = list()
        spec = self.workflow_spec.get('workflow', {}).get('specification', {})
        for step in spec.get('steps', []):
            for cmd in step.get('commands', []):
                result.append(Template(cmd).substitute(workflow_parameters))
        return result

    def run(self, source_dir, run_dir, verbose=False):
        """Run workflow for a given template and set of argument values.

        Parameters
        ----------
        source_dir: string
            Source directory that contains the static template files
        run_dir: string
            Base directory for all workflow run files
        verbose: bool, optional
            Output executed commands if flag is True

        Returns
        -------
        robcore.model.workflow.state.base.WorkflowState
        """
        # Copy all required code and input files
        self.copy_files(source_dir, run_dir)
        # Run workflow step-by-step
        ts_start = datetime.now()
        for cmd in self.get_commands():
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
                        messages=[ex.output.decode('utf-8')]
                    )
        ts_end = datetime.now()
        # Replace references to template parameters in the list of output
        # files from the workflow specification
        output_files = tmpl.replace_args(
            spec=self.workflow_spec.get('outputs', {}).get('files', {}),
            arguments=self.arguments,
            parameters=self.parameters
        )
        # Create dictionary of output files
        files = dict()
        for file_id in output_files:
            files[file_id] = FileResource(
                identifier=file_id,
                filename=os.path.join(run_dir, file_id)
            )
        # Workflow executed successfully
        return StateSuccess(
            created_at=ts_start,
            started_at=ts_start,
            finished_at=ts_end,
            files=files
        )
