# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow step processor that uses the Python subprocess package to
execute a given list of commands in a container environment.
"""

from typing import Dict, List, Optional

import logging
import subprocess

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.model.workflow.step import ContainerStep
from flowserv.controller.worker.base import ContainerEngine

import flowserv.util as util


class SubprocessWorker(ContainerEngine):
    """Container step engine that uses the subprocess package to execute the
    commands in a workflow step.
    """
    def __init__(self, variables: Optional[Dict] = None, env: Optional[Dict] = None):
        """Initialize the optional mapping with default values for placeholders
        in command template strings.

        Parameters
        ----------
        variables: dict, default=None
            Mapping with default values for placeholders in command template
            strings.
        env: dict, default=None
            Default settings for environment variables when executing workflow
            steps. These settings can get overridden by step-specific settings.
        """
        super(SubprocessWorker, self).__init__(variables=variables, env=env)

    def run(self, step: ContainerStep, env: Dict, rundir: str) -> ExecResult:
        """Execute a list of shell commands in a workflow step synchronously.

        Stops execution if one of the commands fails. Returns the combined
        result from all the commands that were executed.

        Parameters
        ----------
        step: flowserv.controller.serial.workflow.ContainerStep
            Step in a serial workflow.
        env: dict, default=None
            Default settings for environment variables when executing workflow
            steps. May be None.
        rundir: string
            Path to the working directory of the workflow run.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        logging.info('run step with subprocess worker')
        # Keep output to STDOUT and STDERR for all executed commands in the
        # respective attributes of the returned execution result.
        result = ExecResult(step=step)
        try:
            # Run each command in the the workflow step. Each command is
            # expected to be a shell command that is executed using the
            # subprocess package. The subprocess.run() method is preferred for
            # capturing output.
            for cmd in step.commands:
                logging.info('{}'.format(cmd))
                proc = subprocess.run(
                    cmd,
                    cwd=rundir,
                    shell=True,
                    capture_output=True,
                    env=env
                )
                # Append output to STDOUT and STDERR to the respecive lists.
                append(result.stdout, proc.stdout.decode('utf-8'))
                append(result.stderr, proc.stderr.decode('utf-8'))
                if proc.returncode != 0:
                    # Stop execution if the command failed.
                    result.returncode = proc.returncode
                    break
        except Exception as ex:
            logging.error(ex)
            strace = '\n'.join(util.stacktrace(ex))
            logging.debug(strace)
            result.stderr.append(strace)
            result.exception = ex
            result.returncode = 1
        return result


# -- Helper Functions ---------------------------------------------------------

def append(outstream: List[str], text: str):
    """Append the given text to an output stream if the text is not empty."""
    if text:
        outstream.append(text)
