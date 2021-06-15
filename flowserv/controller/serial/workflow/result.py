# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow (step) execution result."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from flowserv.model.workflow.step import WorkflowStep

import flowserv.error as err


@dataclass
class ExecResult:
    """Result of executing a workflow (or a single workflow step). Maintains a
    returncode to signal success (=0) or error (<>0). If an exception was raised
    during execution it is captured in the respective property `.exception`.
    Outputs that were written to standard output and standard error are part of
    the result object. Outputs are captured as lists of strings.
    """
    step: WorkflowStep
    returncode: Optional[int] = 0
    stdout: Optional[List[str]] = field(default_factory=list)
    stderr: Optional[List[str]] = field(default_factory=list)
    exception: Optional[Exception] = None


class RunResult(object):
    """Result for a serial workflow run. For each executed workflow step the
    run result maintains the step itself and the
    :class:`flowserv.controller.serial.workflow.result.ExecResult`. In addition,
    the run result maintains the context of the workflow that is modified by the
    executed workflow steps.

    Provides properties for easy access to the return code of the final workflow
    step and the outputs to STDOUT and STDERR.
    """
    def __init__(self, arguments: Dict):
        """Initialize the run context with the initial set of user-provided
        arguments for the workflow run.

        Parameters
        ----------
        arguments: dict
            Dictionary of user-provided input arguments for the workflow run.
        """
        self.context = dict(arguments)
        self.steps = list()

    def __len__(self) -> int:
        """Get number of executed workflow steps.

        Returns
        -------
        int
        """
        return len(self.steps)

    def add(self, result: ExecResult):
        """Add execution result for a workflow step.

        Parameters
        ----------
        result: flowserv.controller.serial.workflow.result.ExecResult
            Execution result for the workflow step.
        """
        self.steps.append(result)

    @property
    def exception(self) -> Exception:
        """Get the exception from the last workflow step that was executed in
        the workflow run.

        The result is None if no workflow step has been executed yet.

        Returns
        -------
        Exception
        """
        return self.steps[-1].exception if self.steps else None

    def get(self, var: str) -> Any:
        """Get the value for a given variable from the run context.

        Raises a KeyError if the variable is not defined.

        Parameters
        ----------
        var: string
            Variable name

        Returns
        -------
        any
        """
        return self.context[var]

    @property
    def log(self) -> List[str]:
        """Get single list containing the concatenation of STDOUT and STDERR
        messages.

        Returns
        -------
        list of string
        """
        return self.stdout + self.stderr

    def raise_for_status(self):
        """Raise an error if the returncode for this result is not zero.

        Will re-raise a cought exception (if set). Otherwise, raises a
        FlowservError.
        """
        status = self.returncode
        if status is None or status == 0:
            # Do nothing if the returncode is 0.
            return
        if self.exception:
            # Re-raise exception if one was caught.
            raise self.exception
        raise err.FlowservError('\n'.join(self.stderr))

    @property
    def returncode(self) -> int:
        """Get the return code from the last workflow step that was executed in
        the workflow run.

        The result is None if no workflow step has been executed yet.

        Returns
        -------
        int
        """
        return self.steps[-1].returncode if self.steps else None

    @property
    def stderr(self) -> List[str]:
        """Get all lines that were written to STDERR by all executed workflow
        steps.

        Returns
        -------
        list of string
        """
        return [line for result in self.steps for line in result.stderr]

    @property
    def stdout(self) -> List[str]:
        """Get all lines that were written to STDOUT by all executed workflow
        steps.

        Returns
        -------
        list of string
        """
        return [line for result in self.steps for line in result.stdout]
