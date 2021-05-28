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
import os
import subprocess

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.model.workflow.step import ContainerStep
from flowserv.controller.worker.base import ContainerWorker

import flowserv.util as util


"""Unique type identifier for SubprocessWorker serializations."""
SUBPROCESS_WORKER = 'subprocess'


class SubprocessWorker(ContainerWorker):
    """Container step engine that uses the subprocess package to execute the
    commands in a workflow step.
    """
    def __init__(
        self, variables: Optional[Dict] = None, env: Optional[Dict] = None,
        identifier: Optional[str] = None, volume: Optional[str] = None
    ):
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
        identifier: string, default=None
            Unique worker identifier. If the value is None a new unique identifier
            will be generated.
        volume: string, default=None
            Identifier for the storage volume that the worker has access to.
            By default, the worker is expected to have access to the default
            volume store for a workflow run.
        """
        super(SubprocessWorker, self).__init__(
            variables=variables,
            env=env,
            identifier=identifier,
            volume=volume
        )

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
        # Windows-specific fix. Based on https://github.com/appveyor/ci/issues/1995
        if 'SYSTEMROOT' in os.environ:
            env = dict(env) if env else dict()
            env['SYSTEMROOT'] = os.environ.get('SYSTEMROOT')
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
            logging.error(ex, exc_info=True)
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
