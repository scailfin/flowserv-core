# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods and classes for unit test for components of the benchmark
modules.
"""

import os

from robcore.model.workflow.engine.base import WorkflowEngine
from robcore.model.workflow.resource import FileResource
from robcore.model.workflow.state.base import StatePending

import robcore.util as util


class StateEngine(WorkflowEngine):
    """Fake workflow engine. Returns the given workflow state when the execute
    method is called.
    """
    def __init__(self, state=None):
        """Initialize the workflow state that the engine is returning for all
        executed workflows.

        Parameters
        ----------
        state: robcore.model.workflow.state.base.WorkflowState, optional
            Workflow state instance
        """
        self.state = state if not state is None else StatePending()

    def cancel_run(self, run_id):
        """Request to cancel execution of the given run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        """
        pass

    def execute(self, run_id, template, source_dir, arguments):
        """Fake execute method that returns the workflow state that the was
        provided when the object was instantiated. Ignores all given arguments.

        Parameters
        ----------
        run_id: string
            Unique identifier for the workflow run.
        template: robcore.model.template.base.repo.TemplateHandle
            Workflow template containing the parameterized specification and the
            parameter declarations
        source_dir: string
            Source directory that contains the static template files
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robcore.model.workflow.state.base.WorkflowState
        """
        return self.state


# -- Helper Methods ------------------------------------------------------------

def run_workflow(engine, template, submission_id, base_dir, values=None):
    """Run workflow for a given set of result values.

    Parameters
    ----------
    engine: robapi.model.benchmark.engine.BenchmarkEngine
        Benchmark executionengine
    template: robcore.model.template.base.repo.TemplateHandle
        Benchmark workflow template
    submission_id: string
        Unique submission idenifier
    base_dir: string
        Path to base directory for the workflow run
    values: dict
        Dictionary of run result values
    """
    run = engine.start_run(
        submission_id=submission_id,
        template=template,
        source_dir=template.source_dir,
        arguments=dict()
    )
    run_id = run.identifier
    state = run.state.start()
    engine.update_run(run_id=run_id, state=state)
    if not values is None:
        result_file = os.path.join(base_dir, 'run_result.json')
        util.write_object(filename=result_file, obj=values)
        file_id = template.get_schema().result_file_id
        files = {file_id: FileResource(identifier=file_id, filename=result_file)}
    else:
        files = None
    state = state.success(files=files)
    engine.update_run(run_id=run_id, state=state)
