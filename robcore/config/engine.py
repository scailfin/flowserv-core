# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module defines environment variables that are used to configure the
workflow engine. In addition, the module provides a function to get an instance
of the workflow controller that is specified by the defined environment
variables.

Workflow controllers are specified using the name of the module and the name
of the class that implements the controller. The specified class is imported
dynamically.

Different workflow controllers may define additional environment variables to
control their configuration.
"""

import os
import tempfile

import robcore.config.api as api
import robcore.config.base as config
import robcore.core.error as err


"""Environment variables that are used to create an instance of the workflow
controller that is used by the workflow engine to execute workflows.
"""
# Name of the class that implements the workflow controller interface
ROB_ENGINE_CLASS = 'ROB_ENGINE_CLASS'
# Name of the module that contains the workflow controller implementation
ROB_ENGINE_MODULE = 'ROB_ENGINE_MODULE'


def ENGIN_BASEDIR():
    """Get base directory for workflow engine from the environment. At this
    point we store run files in a sub-folder of the API base directory. If the
    API base directory is not set the local director for temporary files is used.

    Returns
    -------
    string
    """
    base_dir = api.API_BASEDIR(default_value=str(tempfile.gettempdir()))
    return os.path.join(base_dir, 'runs')


def ROB_ENGINE():
    """Get an instance of the workflow controller that is specified by the two
    environment variables 'ROB_ENGINE_MODULE' and 'ROB_ENGINE_CLASS'. It is
    expected that either both variables contain a non-emoty value or none of
    then is set. In the latter case, the synchronous workflow controller is
    returned as the default.

    Returns
    -------
    robcore.controller.backend.base.WorkflowController

    Raises
    ------
    robcore.core.error.MissingConfigurationError
    """
    module_name = config.get_variable(name=ROB_ENGINE_MODULE)
    class_name = config.get_variable(name=ROB_ENGINE_CLASS)
    # If both environment variables are None return the default controller.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    if module_name is None and class_name is None:
        from robcore.controller.backend.sync import SyncWorkflowEngine
        return SyncWorkflowEngine(base_dir=ENGIN_BASEDIR())
    elif not module_name is None and not class_name is None:
        from importlib import import_module
        module = import_module(module_name)
        return getattr(module, class_name)()
    elif module_name is None:
        raise err.MissingConfigurationError(ROB_ENGINE_MODULE)
    else:
        raise err.MissingConfigurationError(ROB_ENGINE_CLASS)
