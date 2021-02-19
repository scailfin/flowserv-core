# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow (step) execution result."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ExecResult:
    """Result of executing a workflow (or a single workflow step). Maintains a
    returncode to signal success (=0) or error (<>0). If an exception was raised
    during execution it is captured in the respective property `.exception`.
    Outputs that were written to standard output and standard error are part of
    the result object. Outputs are captured as lists of strings.
    """
    returncode: Optional[int] = 0
    stdout: Optional[List[str]] = field(default_factory=list)
    stderr: Optional[List[str]] = field(default_factory=list)
    exception: Optional[Exception] = None
