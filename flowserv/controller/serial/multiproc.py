# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow step processor that uses the Python multiprocessing package to
execute a given list of commands in a container environment.
"""

from typing import Dict, List, Optional

import logging
import subprocess

from flowserv.controller.serial.result import ExecResult

import flowserv.util as util


def exec_step(commands: List[str], rundir: str, env: Optional[Dict] = None) -> ExecResult:
    """Execute a list of shell commands in a workflow step synchronously.

    Stops execution if one of the commands fails. Returns the combined result
    of all executed commands.

    Parameters
    ----------
    commands: list of string
        List of commands that are being executed.
    rundir: string
        Path to the working directory of the workflow run that this step
        belongs to.
    env: dict, default=None
        Mapping of environment variables that is passed to the subprocess run
        method.

    Returns
    -------
    flowserv.controller.serial.result.ExecResult
    """
    # Keep output to STDOUT and STDERR for all executed commands in the
    # respective attributes of the returned execution result.
    result = ExecResult()
    try:
        # Run each command in the the workflow step. Each command is expected
        # to be a shell command that is executed using the subprocess package.
        # The subprocess.run() method is preferred for capturing output.
        for cmd in commands:
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
