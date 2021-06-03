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

from flowserv.volume.base import StorageVolume
from flowserv.volume.factory import Volume
from flowserv.volume.fs import FStore  # noqa: F401
from flowserv.volume.gc import GCBucket  # noqa: F401
from flowserv.volume.s3 import S3Bucket  # noqa: F401
from flowserv.volume.ssh import Sftp  # noqa: F401

import flowserv.error as err
import flowserv.util as util


DEFAULT_STORE = '__default__'


class VolumeManager(object):
    """The volume manager maintains information about storage volumes and the
    files that are available to the workers during workflow execution at each
    volume. The volume manager is the main component that maintains a virtual
    runtime environment in which all workers have access to their required input
    files.

    The manager also acts as a factory for volume stores. When the manager is
    instantiated all storage volumes are specified via their dictionary
    serialization. The respective volume instances are only created when they
    are first accessed.
    """
    def __init__(
        self, stores: List[Dict], files: Optional[Dict[str, List[str]]] = None
    ):
        """Initialize the storage volumes specifications and the initial list
        of static files that are available for the workflow run.

        Expects that the list of storage volumes contains at least the default
        storage volume.

        Parameters
        ----------
        stores: list
            List of dictionary serializations for storage volumes.
        files: dict, default=None
            Mapping of file names to the list of storage volume identifier for
            the volumes that contain the latest version of a input/output file.
        """
        # Ensure that a default storage volume is given in the mapping of
        # storage volume specifications.
        self._storespecs = {doc['id']: doc for doc in stores}
        if DEFAULT_STORE not in self._storespecs:
            raise ValueError('missing default storage volume')
        self._stores = dict()
        # Set the initial mapping of static files. Ensure that the referenced
        # stores are valid.
        self.files = dict(files) if files is not None else dict()
        for _, file_stores in self.files.items():
            for s in file_stores:
                if s not in self._storespecs:
                    raise err.UnknownObjectError(obj_id=s, type_name='storage volume')

    def get(self, identifier: str) -> StorageVolume:
        """Get the instance for the storage volume with the given identifier.

        Paramaters
        ----------
        identifier: str
            Unique storage volume identifier.

        Returns
        -------
        flowserv.volume.base.StorageVolume
        """
        # Create storage volume instance from specification if it has not been
        # accessed yet.
        if identifier not in self._stores:
            self._stores[identifier] = Volume(self._storespecs[identifier])
        return self._stores[identifier]

    def prepare(self, store: StorageVolume, inputs: List[str], outputs: List[str]):
        """Prepare the storage volume for a worker.

        Ensures that the input files that are needed by the worker are available
        in their latest version at the given volume store.

        Raises a ValueError if a specified input file does not exist.

        Parameters
        ----------
        store: flowserv.volume.base.StorageVolume
            Storage volume that is being prepared.
        inputs: list of string
            Relative path (keys) of required input files for a workflow step.
        outputs: list of string
            Relative path (keys) of created output files by a workflow step.
        """
        # Generate dictionary that maps all files that are matches to the given
        # query list to the list of storage volume that the files are available
        # at. At this point we perform a search with quadratic time complexity
        # in the number of query files and and files in the workflow context,
        # assuming that neither (or at least the query files) contains a very
        # large number of elements.
        required_files = dict()
        for q in inputs:
            # The comparison depends on whether the specified file name ends
            # with a '/' (indicating that a directory is referenced) or not.
            is_match = prefix_match if q.endswith('/') else exact_match
            for f, fstores in self.files.items():
                if f not in required_files and is_match(f, q):
                    required_files[f] = fstores
        # Copy required files that are currently not available to the worker.
        for f, fstores in required_files.items():
            # Check if the file is available at the target store.
            if store.identifier in fstores:
                continue
            # If the file is not available at the target volume we need to
            # upload it.
            source = self.get(fstores[0])
            # Upload file from the source storage volume to the target
            # volume.
            for key in source.copy(src=f, store=store):
                self.files[key].append(store.identifier)
        # Create folders for output files.
        out_folders = set()
        for file in outputs:
            parent = file if file.endswith('/') else util.join(*file.split('/')[:-1])
            out_folders.add(parent)
        for dirname in out_folders:
            store.mkdir(path=dirname)

    def update(self, store: StorageVolume, files: List[str]):
        """Update the availability index for workflow files.

        The update method is used by a worker to signal the successful execution
        of a workflow step. The given files specify the output files that were
        generated by the worker. The store identifier references the volume
        store that now contains the latest version of these files.

        Raises a ValueError if the specified storage volume does not exist.

        Parameters
        ----------
        store: flowserv.volume.base.StorageVolume
            Storage volume that contains the latest versions for the given
            files.
        files: list of str
            List of relative path (keys) for output files that were generated
            by a successful workflow step.
        """
        for key in files:
            self.files[key] = [store.identifier]


# -- Helper functions ---------------------------------------------------------

def DefaultVolume(basedir: str) -> VolumeManager:
    """Helper method to create a volume manager with a single file system store
    as the default store.

    Parameters
    ----------
    basedir: str
        Base directory for the created file system store.

    Returns
    -------
    flowserv.volume.manager.VolumeManager
    """
    return VolumeManager(stores=[FStore(basedir=basedir, identifier=DEFAULT_STORE)])


def exact_match(s1: str, s2: str) -> bool:
    """Test if two strings are exact matches.

    Parameters
    ----------
    s1: string
        Left side string of the comparison.
    s2: string
        Right side string of the comparison.

    Returns
    -------
    bool
    """
    return s1 == s2


def prefix_match(value: str, prefix: str) -> bool:
    """Test of the given string value starts with a given prefix.

    Parameters
    ----------
    value: str
        Value for which the prefix is evaluated.
    prefix: str
        Prefix string that is tested for the given value.

    Returns
    -------
    bool
    """
    return value.startswith(prefix)
