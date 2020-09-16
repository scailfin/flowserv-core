# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module defines the function that creates an instance of the workflow
controller from the values in the respective environment variables.

Workflow controllers are specified using the name of the module and the name
of the class that implements the controller. The specified class is imported
dynamically.
"""

import logging

from flowserv.config.base import get_variable
from flowserv.service.files import get_filestore

import flowserv.config.backend as config
import flowserv.error as err


def init_backend(raise_error=True):
    """Get an instance of the workflow controller that is specified by the two
    variables 'FLOWSERV_BACKEND_MODULE' and 'FLOWSERV_BACKEND_CLASS'. It is
    expected that either both variables contain a non-empty value or none of
    them is set. In the latter case, the serial workflow controller is returned
    as the default workflow controller.

    Parameters
    ----------
    raise_error: bool, optional
        Flag to indicate whether an error is raised if a value for a
        configuration variable is missing or not.

    Returns
    -------
    flowserv.controller.base.WorkflowController

    Raises
    ------
    flowserv.error.MissingConfigurationError
    """
    # Create a new instance of the file store based on the configuration in the
    # respective environment variables.
    fs = get_filestore()
    module_name = get_variable(name=config.FLOWSERV_BACKEND_MODULE)
    class_name = get_variable(name=config.FLOWSERV_BACKEND_CLASS)
    # If both environment variables are None return the default controller.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    if module_name is None and class_name is None:
        engine = 'flowserv.controller.serial.engine.SerialWorkflowEngine'
        logging.info('API backend {}'.format(engine))
        from flowserv.controller.serial.engine import SerialWorkflowEngine
        return SerialWorkflowEngine(fs=fs)
    elif module_name is not None and class_name is not None:
        logging.info('API backend {}.{}'.format(module_name, class_name))
        from importlib import import_module
        module = import_module(module_name)
        kwargs = {'fs': fs}
        return getattr(module, class_name)(**kwargs)
    elif module_name is None and raise_error:
        raise err.MissingConfigurationError(config.FLOWSERV_BACKEND_MODULE)
    elif raise_error:
        raise err.MissingConfigurationError(config.FLOWSERV_BACKEND_CLASS)
