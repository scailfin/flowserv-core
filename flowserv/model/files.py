# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Classes for files that are part of workflow run inputs and outputs.
Information about these files that are maintained on storage volumes is also
stored in the database.

This module also defines the folder structure on the storage volume for
workflows and their associated resources.

The folder structure is currently as follows:

.. code-block:: console

    /workflows/                  : Base directory
        {workflow_id}            : Folder for individual workflow
            groups/              : Folder for workflow groups
                {group_id}       : Folder for individual group
                    files/       : Uploaded files for workflow group
                        {file_id}: Folder for uploaded file
            runs/                : Folder for all workflow runs
                {run_id}         : Result files for individual runs
            static/
"""

from io import BytesIO
from typing import Dict, IO, List, Union, Optional

import json

from flowserv.volume.base import IOHandle, IOBuffer

import flowserv.util as util


# -- Workflow file objects ----------------------------------------------------

class FileHandle(IOHandle):
    """Handle for a file that is stored in the database. Extends the file object
    with the base file name and the mime type.

    The implementation is a wrapper around a file object to make the handle
    agnostic to the underlying storage mechanism.
    """
    def __init__(self, name: str, mime_type: str, fileobj: IOHandle):
        """Initialize the file object and file handle.

        Parameters
        ----------
        name: string
            File name (or relative file path)
        mime_type: string
            File content mime type.
        fileobj: flowserv.volume.base.IOHandle
            File object providing access to the file content.
        """
        self.name = name
        self.mime_type = mime_type
        self.fileobj = fileobj

    def open(self) -> IO:
        """Get an BytesIO buffer containing the file content. If the associated
        file object is a path to a file on disk the file is being read.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        return self.fileobj.open()

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        return self.fileobj.size()


# -- Helper functions for file storage locations ------------------------------

def group_uploaddir(workflow_id: str, group_id: str) -> str:
    """Get base directory for files that are uploaded to a workflow group.

    Parameters
    ----------
    workflow_id: string
        Unique workflow identifier
    group_id: string
        Unique workflow group identifier

    Returns
    -------
    string
    """
    groupdir = workflow_groupdir(workflow_id, group_id)
    return util.join(groupdir, 'files')


def io_file(data: Union[List, Dict], format: Optional[str] = None) -> IOBuffer:
    """Write simple text to given bytes buffer.

    Parameters
    ----------
    data: list or dict
        List of strings or dictionary.
    format: string, default=None
        Format identifier.

    Returns
    -------
    flowserv.volume.base.IOBuffer
    """
    buf = BytesIO()
    buf.seek(0)
    if format is None or format == util.FORMAT_JSON:
        buf.write(str.encode(json.dumps(data)))
    else:
        for line in data:
            buf.write(str.encode('{}\n'.format(line)))
    return IOBuffer(buf)


def run_basedir(workflow_id: str, run_id: str) -> str:
    """Get path to the base directory for all files that are maintained for
    a workflow run.

    Parameters
    ----------
    workflow_id: string
        Unique workflow identifier
    run_id: string
        Unique run identifier

    Returns
    -------
    string
    """
    workflowdir = workflow_basedir(workflow_id)
    return util.join(workflowdir, 'runs', run_id)


def run_tmpdir() -> str:
    """Get path to a temporary workflow run directory.

    Returns
    -------
    string
    """
    return util.join('tmp', util.get_unique_identifier())


def workflow_basedir(workflow_id: str) -> str:
    """Get base directory containing associated files for the workflow with
    the given identifier.

    Parameters
    ----------
    workflow_id: string
        Unique workflow identifier

    Returns
    -------
    string
    """
    return workflow_id


def workflow_groupdir(workflow_id: str, group_id: str) -> str:
    """Get base directory containing files that are associated with a
    workflow group.

    Parameters
    ----------
    workflow_id: string
        Unique workflow identifier
    group_id: string
        Unique workflow group identifier

    Returns
    -------
    string
    """
    workflowdir = workflow_basedir(workflow_id)
    return util.join(workflowdir, 'groups', group_id)


def workflow_staticdir(workflow_id: str) -> str:
    """Get base directory containing static files that are associated with
    a workflow template.

    Parameters
    ----------
    workflow_id: string
        Unique workflow identifier

    Returns
    -------
    string
    """
    return util.join(workflow_basedir(workflow_id), 'static')
