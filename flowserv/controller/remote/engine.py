# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for a workflow controller backend that uses an external
(remote) workflow engine (e.g., an existing REANA cluster) for workflow
execution. The controller provides functionality for workflow creation, start,
stop, and monitpring using an (abstract) client class. For different types of
workflow engines only the RemoteClient class needs to be implements.
"""

from typing import Any, Dict, Optional, Tuple

import logging
import os

from flowserv.controller.base import WorkflowController
from flowserv.controller.remote.client import RemoteClient
from flowserv.model.base import RunObject
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.state import WorkflowState
from flowserv.service.api import APIFactory
from flowserv.volume.base import StorageVolume

import flowserv.controller.remote.config as config
import flowserv.controller.remote.monitor as monitor
import flowserv.error as err
import flowserv.util as util


class RemoteWorkflowController(WorkflowController):
    """Workflow controller that executes workflow templates for a given set of
    arguments using an external workflow engine. Each workflow is monitored by
    a separate process that continuously polls the workflow state.
    """
    def __init__(
        self, client: Optional[RemoteClient] = None, poll_interval: Optional[float] = None,
        is_async: Optional[bool] = None, service: Optional[APIFactory] = None
    ):
        """Initialize the client that is used to interact with the remote
        workflow engine.

        Parameters
        ----------
        client: flowserv.controller.remote.client.RemoteClient, defaulNone
            Engine-specific implementation of the remote client that is used by
            the controller to interact with the workflow engine. If no client is
            given an instance is created based on the values in the environment
            variables FLOWSERV_REMOTE_CLIENT_MODULE and FLOWSERV_REMOTE_CLIENT_CLASS
        poll_interval: int or float, default=None
            Frequency (in sec.) at which the remote workflow engine is polled.
        is_async: bool, default=None
            Flag that determines whether workflows execution is synchronous or
            asynchronous by default.
        service: flowserv.service.api.APIFactory, default=None
            API factory for service callback during asynchronous workflow
            execution.
        """
        self.client = client if client else get_client(env=service)
        self.poll_interval = get_env(
            env=service,
            var=config.FLOWSERV_POLL_INTERVAL,
            value=poll_interval,
            default=config.DEFAULT_POLL_INTERVAL
        )
        self.is_async = get_env(
            env=service,
            var=config.FLOWSERV_ASYNC,
            value=is_async,
            default=config.DEFAULT_ASYNC
        )
        self.service = service
        # Dictionary of all running tasks. Maintains tuples containing the
        # multi-process pool object and the remote workflow identifier for
        # each run.
        self.tasks = dict()

    def cancel_run(self, run_id: str):
        """Request to cancel execution of the given run. This method is usually
        called by the workflow engine that uses this controller for workflow
        execution. It is threfore assumed that the state of the workflow run
        is updated accordingly by the caller.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        """
        # Ensure that the run has not been removed already
        if run_id in self.tasks:
            workflow_id = self.tasks[run_id]
            # Stop workflow execution at the engine. Ignore any errors that
            # may be raised.
            try:
                self.client.stop_workflow(workflow_id)
            except Exception as ex:
                logging.error(ex, exc_info=True)
                logging.debug('\n'.join(util.stacktrace(ex)))
            # Delete the task from the dictionary. The state of the
            # respective run will be updated by the workflow engine that
            # uses this controller for workflow execution
            del self.tasks[run_id]

    def exec_workflow(
        self, run: RunObject, template: WorkflowTemplate, arguments: Dict,
        staticfs: StorageVolume, config: Optional[Dict] = None
    ) -> Tuple[WorkflowState, StorageVolume]:
        """Initiate the execution of a given workflow template for a set of
        argument values. This will start a new process that executes a serial
        workflow asynchronously. Returns the state of the workflow after the
        process is stated (the state will therefore be RUNNING).

        The set of arguments is not further validated. It is assumed that the
        validation has been performed by the calling code (e.g., the run
        service manager).

        If the state of the run handle is not pending, an error is raised.

        Parameters
        ----------
        run: flowserv.model.base.RunObject
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations.
        arguments: dict
            Dictionary of argument values for parameters in the template.
        staticfs: flowserv.volume.base.StorageVolume
            Storage volume that contains the static files from the workflow
            template.
        config: dict, default=None
            Optional configuration settings are currently ignored. Included for
            API completeness.

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState, flowserv.volume.base.StorageVolume
        """
        # Get the run state. Ensure that the run is in pending state.
        if not run.is_pending():
            raise RuntimeError("invalid run state '{}'".format(run.state()))
        try:
            # Create a workflow on the remote engine. This will also upload all
            # necessary files to the remote engine. Workflow execution may not
            # be started (indicated by the state property of the returned
            # handle for the remote workflow).
            workflow = self.client.create_workflow(
                run=run,
                template=template,
                arguments=arguments,
                staticfs=staticfs
            )
            workflow_id = workflow.workflow_id
            # Run the workflow. Depending on the values of the is_async flag
            # the process will either block execution while monitoring the
            # workflow state or not.
            if self.is_async:
                self.tasks[run.run_id] = workflow_id
                # Start monitor tread for asynchronous monitoring.
                monitor.WorkflowMonitor(
                    workflow=workflow,
                    poll_interval=self.poll_interval,
                    service=self.service,
                    tasks=self.tasks
                ).start()
                return workflow.state, workflow.runstore
            else:
                # Run workflow synchronously. This will lock the calling thread
                # while waiting (i.e., polling the remote engine) for the
                # workflow execution to finish.
                state = monitor.monitor_workflow(
                    workflow=workflow,
                    poll_interval=self.poll_interval
                )
                return state, workflow.runstore
        except Exception as ex:
            # Set the workflow runinto an ERROR state
            logging.error(ex, exc_info=True)
            strace = util.stacktrace(ex)
            logging.debug('\n'.join(strace))
            return run.state().error(messages=strace), None


def get_client(env: Dict) -> RemoteClient:
    """Get an instance of the remote client.
    
    The value for the class that is being instantiated is read from the
    environment variables FLOWSERV_REMOTE_CLIENT_MODULE and
    FLOWSERV_REMOTE_CLIENT_CLASS.

    Parameters
    ----------
    env: dict
        Environment variable dictionary. May be None

    Returns
    -------
    flowserv.controller.remote.client.RemoteClient
    """
    module_name = get_env(env=env, var=config.FLOWSERV_REMOTE_CLIENT_MODULE)
    class_name = get_env(env=env, var=config.FLOWSERV_REMOTE_CLIENT_CLASS)
    if module_name is not None and class_name is not None:
        logging.info('Remote client {}.{}'.format(module_name, class_name))
        from importlib import import_module
        module = import_module(module_name)
        return getattr(module, class_name)(env=env)
    raise err.MissingConfigurationError('remote client')


def get_env(env: Dict, var: str, value: Optional[Any] = None, default: Optional[Any] = None) -> Any:
    """Get configuration value from environment.
    
    If the given environment dictionary is None then `os.environ` is
    used.

    Parameters
    ----------
    env: dict
        Environment variable dictionary. May be None
    var: str
        Name of the environment variable
    value: any
        Optional value that was given to override the environment
        variable. This value is returned if not None.
    default: any
        Default value that is returned if the environment variable
        is not set.

    Returns
    -------
    any
    """
    if value:
        return value
    if env is None:
        env = os.environ
    return env.get(var, default)