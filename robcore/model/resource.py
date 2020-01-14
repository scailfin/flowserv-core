# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Handles for workflow resources that are created by successful workflow runs.
This includes for example files that are created by individual workflow runs or
by workflow post-processing steps.
"""

import os
import shutil

from robcore.io.files import FileHandle

import robcore.util as util


"""Labels for descriptor serializations."""
LABEL_ID = 'id'
LABEL_NAME = 'name'
LABEL_CONTENTTYPE = 'type'


# -- Descriptors -------------------------------------------------------------

class ResourceDescriptor(object):
    """Descriptor for a workflow resource. Descriptors are part of workflow
    templates. They define metadata of workflow outputs.
    """
    def __init__(self, identifier, name, content_type):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique resource identifier
        name: string
            Descriptive resource name
        content_type: string
            Resource type identifier
        """
        self.identifier = identifier
        self.name = name
        self.content_type = content_type

    @staticmethod
    def from_dict(doc, validate=False):
        """Create descriptor instance from a given dictionary serialization.
        If the validate flag is True a ValueError is raised if the document
        has missing mandatory labels or contains additional labels.

        Parameters
        ----------
        doc: dict
            Dictionary serialization as created by the to_dict() method

        Returns
        -------
        robcore.model.resource.ResourceDescriptor
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory_labels=[LABEL_ID, LABEL_NAME, LABEL_CONTENTTYPE]
            )
        return ResourceDescriptor(
            identifier=doc[LABEL_ID],
            name=doc[LABEL_NAME],
            content_type=doc[LABEL_CONTENTTYPE]
        )

    def to_dict(self):
        """Get dictionary serialization for the resource descriptor.

        Returns
        -------
        dict
        """
        return {
            LABEL_ID: self.identifier,
            LABEL_NAME: self.name,
            LABEL_CONTENTTYPE: self.content_type
        }


# -- Resource Handles ---------------------------------------------------------

class FileResource(object):
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
    def __init__(self, resource_id, resource_name, file_path):
        """Initialize the resource identifier, name and the file handle that
        provides access to the file on disk.

        Parameters
        ----------
        resource_id: string
            Resource identifier that is unique among all resources that are
            created within a single workflow run.
        resource_name: string
            Relative path name that references the resource file in the run
            directory
        file_path: string
            Path to access the resource file on disk
        """
        self.resource_id = resource_id
        self.resource_name = resource_name
        self.file_path = file_path

    def delete(self):
        """Delete the associated file on disk."""
        if os.path.isfile(self.file_path):
            os.remove(self.file_path)
        elif os.path.isdir(self.file_path):
            shutil.rmtree(self.file_path)

    @property
    def filename(self):
        """Short-cut to get complete name for file on disk. Included mainly for
        compatibility with previous implementation.

        Returns
        -------
        string
        """
        return self.file_path

    def file_handle(self):
        """Get handle for the resource file on disk.

        Returns
        -------
        robcore.io.files.FileHandle
        """
        return FileHandle(filepath=self.file_path)
