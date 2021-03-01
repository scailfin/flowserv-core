# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Execute a workflow code step."""

from typing import Dict

import logging
import os
import sys

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.model.workflow.step import FunctionStep

import flowserv.util as util


class OutputStream(object):
    """Output stream for standard output and standard error streams when
    executing a Python function.
    """
    def __init__(self, stream):
        self.closed = False
        self._stream = stream

    def close(self):
        self.closed = True

    def flush(self):
        pass

    def writelines(self, iterable):
        for text in iterable:
            self.write(text)

    def write(self, text):
        self._stream.append(text)


def exec_func(step: FunctionStep, context: Dict, rundir: str) -> ExecResult:
    """Execute a workflow step of type :class:FunctionStep in a given context.

    Captures output to STDOUT and STDERR and includes them in the returned
    execution result.

    Parameters
    ----------
    step: flowserv.model.workflow.step.FunctionStep
        Code step in a serial workflow.
    context: dict
        Context for the executed code.

    Returns
    -------
    flowserv.controller.serial.workflow.result.ExecResult
    """
    result = ExecResult(step=step)
    out = sys.stdout
    err = sys.stderr
    sys.stdout = OutputStream(stream=result.stdout)
    sys.stderr = OutputStream(stream=result.stderr)
    # Change working direcotry temporarily.
    cwd = os.getcwd()
    os.chdir(rundir)
    try:
        step.exec(context=context)
    except Exception as ex:
        logging.error(ex)
        strace = '\n'.join(util.stacktrace(ex))
        logging.debug(strace)
        result.stderr.append(strace)
        result.exception = ex
        result.returncode = 1
    finally:
        # Make sure to reverse redirection of output streams
        sys.stdout = out
        sys.stderr = err
        # Reset working directory.
        os.chdir(cwd)
    return result
