# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Define the workflow backend as a global variable. This is necessary for the
multi-porcess backend to be able to maintain process state in between API
requests.
"""

from typing import Dict, Optional

import logging

from flowserv.config import env, FLOWSERV_BACKEND_CLASS, FLOWSERV_BACKEND_MODULE
from flowserv.controller.base import WorkflowController

import flowserv.error as err


def init_backend(config: Optional[Dict] = None) -> WorkflowController:
    """Update the global workflow engine based on the current settings in the
    environment.

    Parameters
    ----------
    config: dict
        Configuration object that provides access to configuration parameters
        in the environment.

    Returns
    -------
    flowserv.controller.base.WorkflowController
    """
    # Ensure that the configuration is set.
    config = config if config is not None else env()
    # Create a new instance of the file store based on the configuration in the
    # respective environment variables.
    module_name = config.get(FLOWSERV_BACKEND_MODULE)
    class_name = config.get(FLOWSERV_BACKEND_CLASS)
    # If both environment variables are None return the default controller.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    global backend
    if module_name is None and class_name is None:
        engine = 'flowserv.controller.serial.engine.SerialWorkflowEngine'
        logging.info('API backend {}'.format(engine))
        from flowserv.controller.serial.engine import SerialWorkflowEngine
        backend = SerialWorkflowEngine(config=config)
    elif module_name is not None and class_name is not None:
        logging.info('API backend {}.{}'.format(module_name, class_name))
        from importlib import import_module
        module = import_module(module_name)
        backend = getattr(module, class_name)(config=config)
    else:
        raise err.MissingConfigurationError('workflow backend')
    return backend


backend = init_backend()
