
from typing import Dict, Optional

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.controller.worker.base import Worker
from flowserv.model.workflow.step import NotebookStep
from flowserv.volume.fs import FileSystemStorage


"""Unique type identifier for NotebookEngine serializations."""
NOTEBOOK_WORKER = 'notebook'


class NotebookEngine(Worker):
    """Execution engine for notebook steps in a serial workflow."""
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
        super(NotebookEngine, self).__init__(identifier=identifier, volume=volume)

    def exec(self, step: NotebookStep, context: Dict, store: FileSystemStorage) -> ExecResult:
        """Execute a given notebook workflow step in the current workflow
        context.

        The notebook engine expects a file system storage volume that provides
        access to the notebook file and any other aditional input files.

        Parameters
        ----------
        step: flowserv.model.workflow.step.NotebookStep
            Notebook step in a serial workflow.
        context: dict
            Dictionary of variables that represent the current workflow state.
        store: flowserv.volume.fs.FileSystemStorage
            Storage volume that contains the workflow run files.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        # TODO: Remove these lines. They are only here for test purposes.
        # Get handle for the notebook file. Use f.open() to get a bytes buffer
        # containing the notebook (in case that papermill can accept that).
        # Otherwise, we need to pass the filename f.filename to papermill as
        # the notebook argument.
        f = store.load(step.notebook)
        import os
        assert os.path.isfile(f.filename)
        print(f'Notebook file: {f.filename}')
        # The context dictionary should contain the following values:
        # {'inputfile': 'data/names.txt', 'outputfile': 'results/greetings.txt', 'greeting': 'Hey there'}
        print(f'Context: {context}')
        # TODO: Call execute method of the NotebookEngine to run the notebook
        # with the argument values from the workflow context.
        result = ExecResult(step=step)
