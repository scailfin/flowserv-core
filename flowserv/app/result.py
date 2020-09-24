# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Classes that wrap serialized run result dictionaries. The aim is to make
it easier for a developer that uses the flowserv application object to access
the results and resources of workflow runs.
"""

from typing import Callable, Dict, IO, List, Optional, Union

from flowserv.model.base import RunFile

import flowserv.model.workflow.state as st
import flowserv.util as util


class ResultFile(object):
    """Wrapper for run result files. Maintains the reference to the file
    object together with the file name and mime type. Depending on the file
    store this object may either be a string (path to a file on disk) or a
    BytesIO buffer.
    """
    def __init__(self, file_handle: RunFile, fileobj: Union[str, IO]):
        """Initialize the file object and file handle.

        Parameters
        ----------
        file_handle: flowserv.model.base.RunFile
            Handle for the run result file.
        fileobj: string or io.BytesIO
            File object providing access to the file content.
        """
        self.fileobj = fileobj
        self.mime_type = file_handle.mime_type
        self.name = file_handle.name

    def load(self) -> IO:
        """Get an BytesIO buffer containing the file content. If the associated
        file object is a path to a file on disk the file is being read.

        Returns
        -------
        io.BytesIO
        """
        if isinstance(self.fileobj, str):
            return util.read_buffer(self.fileobj)
        else:
            return self.fileobj


class RunResult(object):
    """Wrapper around a serialized run result. This class also accepts a
    function that can be used to load any file that is included in the output
    of a successful workflow run.
    """
    def __init__(self, doc: Dict, loader: Callable):
        """initialize the run information and the file loader function. The
        loader function is a callable that takes two arguments, the run
        identifier and the file identifier.

        Parameters
        ----------
        doc: dict
            Serialized run handle.
        loader: callable
            Function that accepts the run identifier and file identifier of a
            run result file and that returns a result file object.
        """
        self.doc = doc
        self.loader = loader

    def get_file(
        self, key: str, raise_error: Optional[bool] = True
    ) -> ResultFile:
        """Get result file object for the file with the given identifier.
        Raises a ValueError if no file with the given key exists and the raise
        error flag is True.

        Parameters
        ----------
        key: string
            User-provided file identifier or the file source name.
        raise_error: bool, default=True
            Raise error if no file with the given key exists.

        Returns
        -------
        flowserv.app.result.ResultFile

        Raises
        ------
        ValueError
        """
        file_id = self.get_file_id(key, raise_error=raise_error)
        return self.loader(run_id=self.run_id, file_id=file_id)

    def get_file_id(
        self, key: str, raise_error: Optional[bool] = True
    ) -> str:
        """Get the unique identifier for a result file object with the given
        key. The key is either the rlative result file path or a user-defined
        key that was specified in the workflow template.

        Raises a ValueError if no file with the given key exists and the raise
        error flag is True.

        Parameters
        ----------
        key: string
            User-provided file identifier or the file source name.
        raise_error: bool, default=True
            Raise error if no file with the given key exists.

        Returns
        -------
        flowserv.app.result.ResultFile

        Raises
        ------
        ValueError
        """
        file_id = None
        if self.is_success():
            for obj in self.doc.get('files', {}):
                if obj['name'] == key:
                    file_id = obj['id']
                    break
        if file_id is None and raise_error:
            raise ValueError("unknown file '{}'".format(key))
        return file_id

    def is_canceled(self) -> bool:
        """Check if the run state is CANCELED.

        Returns
        -------
        bool
        """
        return self.doc['state'] == st.STATE_CANCELED

    def is_error(self) -> bool:
        """Check if the run state is ERROR.

        Returns
        -------
        bool
        """
        return self.doc['state'] == st.STATE_ERROR

    def is_pending(self) -> bool:
        """Check if the run state is PENDING.

        Returns
        -------
        bool
        """
        return self.doc['state'] == st.STATE_PENDING

    def is_running(self) -> bool:
        """Check if the run state is RUNNING.

        Returns
        -------
        bool
        """
        return self.doc['state'] == st.STATE_RUNNING

    def is_success(self) -> bool:
        """Check if the run state is SUCCESS.

        Returns
        -------
        bool
        """
        return self.doc['state'] == st.STATE_SUCCESS

    def messages(self) -> List[str]:
        """Get list of error messages for runs that are in error state or that
        have been canceled. If the run is not in an error state the result is
        an empty list.

        Returns
        -------
        list
        """
        return self.doc.get('messages', [])

    @property
    def run_id(self) -> str:
        """Get the unique run identifier.

        Returns
        -------
        string
        """
        return self.doc['id']
