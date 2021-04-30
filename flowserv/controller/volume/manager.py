# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Manager for storage volumes. The volume manager is associated with a workflow
run. It maintains information about the files and directories that are available
to the workflow run in the virtual workflow environment.
"""

from typing import Dict, List, Optional

from flowserv.controller.volume.base import StorageVolume

import flowserv.error as err


DEFAULT_STORE = '__default__'


class VolumeManager(object):
    """The volume manager maintains information about the files that are
    available to the workers during workflow execution. The manager is a main
    component in maintaining a virtual runtime environment that ensures that
    workers have access to all the required input files.

    The manager contains a special default storage volume. This volume is used
    as intermediate storage for files that need to be copied (downloaded) from
    one storage volume to another volume (uploaded).
    """
    def __init__(
        self, stores: List[StorageVolume], staticfiles: Optional[Dict[str, List[str]]] = None
    ):
        """Initialize the storage volumes and the initial list of static files
        that are available for the workflow run.

        Expects that the list of storage volumes contains at least the default
        storage volume.

        Parameters
        ----------
        stores: list of flowserv.controller.volume.base.StorageVolume
            List of storage volumes that provide file access for workflow
            workers.
        staticfiles: dict, defualt=None
            Mapping of file names to the list of storage volume identifier for
            the volumes that contain the latest version of a input/output file.
        """
        # Create a mapping of store identifier to the strage volume objects.
        self.stores = {s.identifier: s for s in stores}
        # Ensure that a default storage volume is given.
        if DEFAULT_STORE not in self.stores:
            raise ValueError('missing default storage volume')
        # Set the initial mapping of static files. Ensire that the referenced
        # stores are valid.
        self.files = dict(staticfiles) if staticfiles is not None else dict()
        for _, file_stores in self.files.items():
            for s in file_stores:
                if s not in self.stores:
                    raise err.UnknownObjectError(obj_id=s, type_name='storage volume')

    def prepare(
        self, files: List[str], stores: Optional[List[str]] = None,
        default_store: Optional[str] = None
    ) -> Dict[str, StorageVolume]:
        """Prepare the volume stores for a worker.

        Ensures that the input files that are needed by the worker are available
        in their latest version on at least one of the given volume stores. If
        files need to uploaded the given default_store will be used. If no
        default is specified the first store in the given list of worker stores
        is used.

        Raises a ValueError if a specified input file or storage volume does
        not exist.

        Returns a mapping of the required input files to the storage volume that
        contains them. If a file is available on multiple of the storage volumes
        only one of them is referenced in the result.

        Parameters
        ----------
        files: list of str
            Relative path (keys) of required input files for a workflow step.
        stores: list of string, default=None
            List of storage volumes that the worker has access to. If no vlume
            is given the default volume is used as default.
        default_store: str, default=None
            Default store that is used for file uploads.

        Returns
        -------
        dict
        """
        # Ensure that all files are known files.
        for key in files:
            if key not in self.files:
                raise err.UnknownFileError(key)
        # Ensure that the identifier for the worker stores are valid.
        stores = stores if stores is not None else [DEFAULT_STORE]
        for s in stores:
            if s not in self.stores:
                raise err.UnknownObjectError(obj_id=s, type_name='storage volume')
        # Generate dictionary that maps file identifier to the store they are
        # available at for the worker.
        result = dict()
        for key in files:
            # Get list of storage volumes where the file is available.
            file_stores = self.files[key]
            for s in stores:
                # Find the first store that the worker has access to where the
                # file is available.
                if s in file_stores:
                    result[key] = self.stores[s]
                    break
            if key not in result:
                # If the file is not available on any of the volumes that the
                # worker has access to upload it to the first one of them.
                # Before we upload the file we need to ensure that it is
                # available in the default volume.
                local_file = self.stores[DEFAULT_STORE].abspath(src=key)
                if DEFAULT_STORE not in file_stores:
                    # TODO: This assumes that the default store is a file
                    # store on the local file system. This may cause issues
                    # when running flowserv competely in a remote setting.
                    self.stores[file_stores[0]].download(src=key, dst=local_file)
                    self.files[key].append(DEFAULT_STORE)
                # Upload file from the default storage volume.
                s = default_store if default_store is not None else stores[0]
                self.stores[s].upload(src=local_file, dst=key)
                self.files[key].append(s)
                result[key] = self.stores[s]
        return result

    def update(self, files: List[str], store: Optional[str] = None):
        """Update the availablity index for workflow files.

        The update method is used by a worker to signal the successful execution
        of a workflow step. The given files specify the output files that were
        generated by the worker. The store identifier references the volume
        store that now contains the latest version of these files.

        Raises a ValueError if the specified storage volume does not exist.

        Parameters
        ----------
        files: list of str
            List of relative path (keys) for output files that were generated
            by a successful workflow step.
        store: str, default=None
            Identifier for the storage volume that contains the latest versions
            for the referenced output files.
        """
        # Ensure the store identifier is valid.
        if store is not None:
            if store not in self.stores:
                raise ValueError("unknown storage volume '{}'".format(store))
        else:
            store = DEFAULT_STORE
        # Update stirage volume for all specified files.
        for key in files:
            self.files[key] = [store]
