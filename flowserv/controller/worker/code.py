# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Execute a workflow code step."""

from typing import Dict, Optional

import logging
import os
import sys

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.controller.worker.base import Worker
from flowserv.model.workflow.step import CodeStep
from flowserv.volume.fs import FileSystemStorage

import flowserv.util as util


"""Unique type identifier for CodeWorker serializations."""
CODE_WORKER = "code"


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


class CodeWorker(Worker):
    """Worker to execute workflow steps of type
    :class:`flowserv.model.workflow.step.CodeStep`
    """
    def __init__(self, identifier: Optional[str] = None, volume: Optional[str] = None):
        """Initialize the worker identifier and accessible storage volume.

        Parameters
        ----------
        identifier: string, default=None
            Unique worker identifier. If the value is None a new unique identifier
            will be generated.
        volume: string, default=None
            Identifier for the storage volume that the worker has access to.
            By default, the worker is expected to have access to the default
            volume store for a workflow run.
        """
        super(CodeWorker, self).__init__(identifier=identifier, volume=volume)

    def exec(self, step: CodeStep, context: Dict, store: FileSystemStorage) -> ExecResult:
        """Execute a workflow step of type :class:`flowserv.model.workflow.step.CodeStep`
        in a given context.

        Captures output to STDOUT and STDERR and includes them in the returned
        execution result.

        Note that the code worker expects a file system storage volume.

        Parameters
        ----------
        step: flowserv.model.workflow.step.CodeStep
            Code step in a serial workflow.
        context: dict
            Context for the executed code.
        store: flowserv.volume.fs.FileSystemStorage
            Storage volume that contains the workflow run files.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        result = ExecResult(step=step)
        out = sys.stdout
        err = sys.stderr
        sys.stdout = OutputStream(stream=result.stdout)
        sys.stderr = OutputStream(stream=result.stderr)
        # Change working directory temporarily.
        cwd = os.getcwd()
        os.chdir(store.basedir)
        try:
            step.exec(context=context)
        except Exception as ex:
            logging.error(ex, exc_info=True)
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
