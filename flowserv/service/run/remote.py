# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for the API service component provides methods that execute,
access, and manipulate workflow runs and their results. The remote service API
provides access to run resources at a RESTful API.
"""

from typing import Dict, IO, List, Optional

from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.remote import delete, download_file, get, post, put
from flowserv.service.run.base import RunService

import flowserv.service.descriptor as route
import flowserv.view.run as default_labels


class RemoteRunService(RunService):
    """API component that provides methods to start, access, and manipulate
    workflow runs and their resources at a remote RESTful API.
    """
    def __init__(self, descriptor: ServiceDescriptor, labels: Optional[Dict] = None):
        """Initialize the Url route patterns from the service descriptor and
        the dictionary of labels for elements in request bodies.

        Parameters
        ----------
        descriptor: flowserv.service.descriptor.ServiceDescriptor
            Service descriptor containing the API route patterns.
        labels: dict, default=None
            Override the default labels for elements in request bodies.
        """
        # Default labels for elements in request bodies.
        self.labels = {
            'CANCEL_REASON': default_labels.CANCEL_REASON,
            'RUN_ARGUMENTS': default_labels.RUN_ARGUMENTS
        }
        if labels is not None:
            self.labels.update(labels)
        # Short cut to access urls from the descriptor.
        self.urls = descriptor.urls

    def cancel_run(self, run_id: str, reason: Optional[str] = None) -> Dict:
        """Cancel the run with the given identifier. Returns a serialization of
        the handle for the canceled run.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        reason: string, optional
            Optional text describing the reason for cancelling the run

        Returns
        -------
        dict
        """
        # Create the request body with the optional reason.
        data = dict()
        if reason is not None:
            data[self.labels['CANCEL_REASON']] = reason
        url = self.urls(route.RUNS_CANCEL, runId=run_id)
        return put(url=url, data=data)

    def delete_run(self, run_id: str) -> Dict:
        """Delete the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to delete the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        """
        return delete(url=self.urls(route.RUNS_DELETE, runId=run_id))

    def get_result_archive(self, run_id: str) -> IO:
        """Get compressed tar-archive containing all result files that were
        generated by a given workflow run. If the run is not in sucess state
        a unknown resource error is raised.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        io.BytesIO
        """
        url = self.urls(route.RUNS_DOWNLOAD_ARCHIVE, runId=run_id)
        return download_file(url=url)

    def get_result_file(self, run_id: str, file_id: str) -> IO:
        """Get file handle for a resource file that was generated as the result
        of a successful workflow run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string
            Unique result file identifier.

        Returns
        -------
        flowserv.model.files.FileHandle
        """
        url = self.urls(route.RUNS_DOWNLOAD_FILE, runId=run_id, fileId=file_id)
        return download_file(url=url)

    def get_run(self, run_id: str) -> Dict:
        """Get handle for the given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        dict
        """
        return get(url=self.urls(route.RUNS_GET, runId=run_id))

    def list_runs(self, group_id: str, state: Optional[str] = None):
        """Get a listing of all run handles for the given workflow group.

        Raises an unauthorized access error if the user does not have read
        access to the workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        state: string, default=None
            State identifier query

        Returns
        -------
        dict
        """
        url = self.urls(route.GROUPS_RUNS, userGroupId=group_id, state=state)
        return get(url=url)

    def start_run(self, group_id: str, arguments: List[Dict]) -> Dict:
        """Start a new workflow run for the given group. The user provided
        arguments are expected to be a list of (key,value)-pairs. The key value
        identifies the template parameter. The data type of the value depends
        on the type of the parameter.

        Returns a serialization of the handle for the started run.

        Raises an unauthorized access error if the user does not have the
        necessary access to modify the workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        arguments: list(dict)
            List of user provided arguments for template parameters.

        Returns
        -------
        dict
        """
        data = {self.labels['RUN_ARGUMENTS']: arguments}
        url = self.urls(route.RUNS_START, userGroupId=group_id)
        return post(url=url, data=data)
