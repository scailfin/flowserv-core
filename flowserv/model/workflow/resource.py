# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Handles for resources that are created by successful workflow runs. This
includes, for example, files that are created by individual workflow runs or
by workflow post-processing steps.
"""

import io
import os
import tarfile


class WorkflowResource(object):
    """Handle for resources that are created by successful workflow runs. Each
    resource has a unique identifier and a unique key. The resource identifier
    is used to identify a particular version of the resource. Multiple versions
    of a resource may be the result of repeated workflow execution. The
    resource key is assumed to be the human-readable persistent identifier that
    is used by the user to identify the current version of a resource.
    """
    def __init__(self, resource_id, key):
        """Initialize the unique object identifier.

        Parameters
        ----------
        resource_id: string
            Unique object identifier
        key: string
            Human-readable persistent identifier for the resource.
        """
        self.resource_id = resource_id
        self.key = key

    @classmethod
    def from_dict(cls, doc):
        """Get object instance from serialization. The class of the object is
        determined by the value of the 'type' element in the given document.

        Raises ValueError if the object type is unknown.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for workflow resource

        Returns
        -------
        flowserv.model.workflow.WorkflowResource

        Raises
        ------
        ValueError
        """
        return WorkflowResource(
            resource_id=doc['id'],
            key=doc['key']
        )

    def to_dict(self):
        """Create dictionary serialization of the resource handle.

        Returns
        -------
        dict
        """
        return {
            'id': self.resource_id,
            'key': self.key
        }


class ResourceSet(object):
    """Set of workflow resources that have unique identifier and unique keyes.
    Allows retrieval of resources either by key or identifier.
    """
    def __init__(self, resources=None):
        """Initialize the resource listing. Ensures that identifier and keys
        are unique. Raises ValueError if either uniqueness constraint is
        violated.

        Parameters
        ----------
        resources: list(flowserv.model.workflow.resource.WorkflowResource),
                default=None
            List of resource handles

        Raises
        ------
        ValueError
        """
        if resources is not None:
            # Verify that the resource keys and identifiers are unique
            identifiers = set()
            keys = set()
            for r in resources:
                if r.resource_id in identifiers:
                    msg = "duplicate identifier '{}' in resource list"
                    raise ValueError(msg.format(r.resource_id))
                identifiers.add(r.resource_id)
                if r.key in keys:
                    msg = "duplicate key '{}' in resource list"
                    raise ValueError(msg.format(r.key))
                keys.add(r.key)
            self.elements = resources
        else:
            self.elements = list()

    def __iter__(self):
        """Make list of resource descriptors iterable.

        Returns
        -------
        iterator
        """
        return iter(self.elements)

    def __len__(self):
        """Number of resources in the set.

        Returns
        -------
        int
        """
        return len(self.elements)

    def get_resource(self, identifier=None, key=None):
        """Get the resource with the given identifier or key.

        Parameters
        ----------
        identifier: string, default=None
            Unique resource version identifier.
        key: string, default=None
            Unique resource key.

        Returns
        -------
        flowserv.model.workflow.resource.WorkflowResource
        """
        for r in self.elements:
            if identifier is not None:
                match = r.resource_id == identifier
            else:
                match = True
            if match and key is not None:
                match = match and r.key == key
            if match:
                return r

    def targz(self, basedir):
        """Create a gzipped tar file containing all files in the given resource
        set.

        Returns
        -------
        io.BytesIO
        """
        file_out = io.BytesIO()
        tar_handle = tarfile.open(fileobj=file_out, mode='w:gz')
        for r in self.elements:
            filename = os.path.join(basedir, r.key)
            tar_handle.add(name=filename, arcname=r.key)
        tar_handle.close()
        file_out.seek(0)
        return file_out
