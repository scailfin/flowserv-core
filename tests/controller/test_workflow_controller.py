# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the abstract workflow controller."""

import pytest

from robcore.controller.backend.base import WorkflowController


class TestWorkflowEngine(object):
    """Test abstract workflow engine interface (added for test completeness).
    """
    def test_interface(self):
        """Ensure that interface methods raise NotImplementedError."""
        controller = WorkflowController()
        with pytest.raises(NotImplementedError):
            controller.cancel_run(run_id='ABC')
        with pytest.raises(NotImplementedError):
            controller.exec_workflow(
                run_id='ABC',
                template=None,
                arguments=None
            )
        with pytest.raises(NotImplementedError):
            controller.get_run_state(run_id='ABC')
        with pytest.raises(NotImplementedError):
            controller.remove_run(run_id='ABC')
