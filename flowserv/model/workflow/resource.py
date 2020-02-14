# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Handles for resources that are created by successful workflow runs. This
includes, for example, files that are created by individual workflow runs or
by workflow post-processing steps.
"""

import io
import tarfile

from flowserv.core.files import FileHandle


"""Labels for handle serializations."""
LABEL_FILEPATH = 'path'
LABEL_ID = 'id'
LABEL_NAME = 'name'
LABEL_TYPE = 'type'


# -- Resource Handles ---------------------------------------------------------

class WorkflowResource(object):
    """Abstract handle for resources that are created as the result of
    successful workflow runs. Each resource has a unique identifier and a
    unique name. The resource identifier is used to identify a particular
    version of the resource. Multiple versions of a resource may be the result
    of repeated workflow execution (or post-processing). The resource name
    is assumed to be the human-readable persistent identifier that is used by
    the user to identify the current version of a resource.

    Implementations of this class need to provide a method to serialize class
    instances. For each resource the static from_dict() method in this base
    class should be extended accordingly.
    """
    def __init__(self, identifier, name):
        """Initialize the unique object identifier.

        Parameters
        ----------
        identifier: string
            Unique object identifier
        name: string
            Human-readable persistent identifier for the resource.
        """
        self.identifier = identifier
        self.name = name

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
        r_type = doc.get(LABEL_TYPE)
        if r_type == 'FSObject':
            return FSObject.from_dict(doc)
        else:
            raise ValueError("unknown resource type '{}'".format(r_type))

    def to_dict(self):
        """Create dictionary serialization of the resource handle.

        Returns
        -------
        dict
        """
        return {
            LABEL_ID: self.identifier,
            LABEL_NAME: self.name
        }


class FSObject(FileHandle):
    """Handle for file resources that are created as the result of a workflow
    run. Each workflow specification is expected to contain a list of names
    that identify the files that are generated as the result of a successful
    workflow run. These files are kept in the directory of the respective
    workflow run.

    File resources have a unique internal identifier and a resource name. The
    resource name is a relative file path that identifies the result file in
    the run folder. The associated file path provides access to the file on
    disk. Resource files are maintained by the workflow backend in a persistent
    manner in order to be accessible as long as information about the workflow
    run is maintained by the workflow engine.
    """
    def __init__(self, identifier, name, filename):
        """Initialize the resource identifier, name and the file handle that
        provides access to the file on disk.

        Parameters
        ----------
        identifier: string
            Resource identifier that is unique among all resources that are
            created within a single workflow run.
        name: string
            Relative path name that references the resource file in the run
            directory
        file_path: string
            Path to access the resource file on disk
        """
        super(FSObject, self).__init__(
            identifier=identifier,
            name=name,
            filename=filename
        )

    @classmethod
    def from_dict(cls, doc):
        """Get object instance from serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for workflow resource

        Returns
        -------
        """
        return cls(
            identifier=doc[LABEL_ID],
            name=doc[LABEL_NAME],
            filename=doc[LABEL_FILEPATH]
        )

    def to_dict(self):
        """Create dictionary serialization for the file resource handle.

        Returns
        -------
        dict
        """
        return {
            LABEL_ID: self.identifier,
            LABEL_NAME: self.name,
            LABEL_FILEPATH: self.path,
            LABEL_TYPE: 'FSObject'
        }


# -- Resource Sets ------------------------------------------------------------

class ResourceSet(object):
    """Set of workflow resources that have unique identifier and unique names.
    Allows retrieval of resources either by name or identifier.
    """
    def __init__(self, resources=None):
        """Initialize the resource listing. Ensures that identifier and names
        are unique. Raises ValueError if either uniqueness constraint is
        violated.

        Parameters
        ----------
        resources: list(flowserv.model.workflow.resource.WorkflowResource), optional
            List of resource handles

        Raises
        ------
        ValueError
        """
        if resources is not None:
            # Verify that the resource names and identifiers are unique
            identifiers = set()
            names = set()
            for r in resources:
                if r.identifier in identifiers:
                    msg = "duplicate identifier '{}' in resource list"
                    raise ValueError(msg.format(r.identifier))
                identifiers.add(r.identifier)
                if r.name in names:
                    msg = "duplicate name '{}' in resource list"
                    raise ValueError(msg.format(r.name))
                names.add(r.name)
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

    def get_resource(self, identifier=None, name=None):
        """Get the resource with the given identifier or name.

        Parameters
        ----------
        identifier: string, optional
            unique resource version identifier
        name: string, optional
            Resource name

        Returns
        -------
        flowserv.model.workflow.resource.WorkflowResource
        """
        for r in self.elements:
            if identifier is not None:
                match = r.identifier == identifier
            else:
                match = True
            if match and name is not None:
                match = r.name == name
            if match:
                return r

    def targz(self):
        """Create a gzipped tar file containing all files in the given resource
        set.

        Returns
        -------
        io.BytesIO
        """
        file_out = io.BytesIO()
        tar_handle = tarfile.open(fileobj=file_out, mode='w:gz')
        for r in self.elements:
            tar_handle.add(name=r.filename, arcname=r.name)
        tar_handle.close()
        file_out.seek(0)
        return file_out
