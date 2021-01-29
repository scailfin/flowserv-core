# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory pattern for file stores."""

from typing import Dict

from flowserv.config import FLOWSERV_FILESTORE_MODULE, FLOWSERV_FILESTORE_CLASS
from flowserv.model.files.base import FileStore

import flowserv.error as err


def FS(env: Dict) -> FileStore:
    """Factory pattern to create file store instances for the service API. Uses
    the environment variables FLOWSERV_FILESTORE_MODULE and
    FLOWSERV_FILESTORE_CLASS to create an instance of the file store. If the
    environment variables are not set the FileSystemStore is returned as the
    default file store.

    Parameters
    ----------
    env: dict
        Configuration dictionary that provides access to configuration
        parameters from the environment.

    Returns
    -------
    flowserv.model.files.base.FileStore
    """
    module_name = env.get(FLOWSERV_FILESTORE_MODULE)
    class_name = env.get(FLOWSERV_FILESTORE_CLASS)
    # If both environment variables are None return the default file store.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    if module_name is None and class_name is None:
        from flowserv.model.files.fs import FileSystemStore
        return FileSystemStore(env=env)
    elif module_name is not None and class_name is not None:
        from importlib import import_module
        module = import_module(module_name)
        return getattr(module, class_name)(env=env)
    raise err.MissingConfigurationError('file store')
