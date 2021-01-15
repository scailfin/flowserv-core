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

from typing import Dict, List

from flowserv.client.app.data import DataFile
from flowserv.service.api import APIFactory

import flowserv.model.workflow.state as st


class Run(object):
    """Wrapper around a serialized run result. Provides access to file objects
    for the result files that are generated by successful workflow runs.
    """
    def __init__(self, doc: Dict, service: APIFactory):
        """Initialize the run information and the API factory that is required
        to load run files.

        Parameters
        ----------
        doc: dict
            Serialized run handle.
        service: flowserv.client.api.APIFactory
            Factory to create instances of the service API.
        """
        self.doc = doc
        self.service = service
        self.run_id = doc['id']
        # Create file isstances for all result files (if any).)
        self._files = dict()
        for obj in self.doc.get('files', {}):
            f = DataFile(run_id=self.run_id, doc=obj, service=service)
            self._files[f.name] = f

    def __str__(self) -> str:
        """The string representation of a run result is the value for the run
        state.

        Returns
        -------
        string
        """
        return self.doc['state']

    def files(self) -> List[DataFile]:
        """Get list of file objects for run result files.

        Returns
        -------
        list of flowserv.client.app.data.File
        """
        return self._files.values()

    def get_file(self, key: str) -> DataFile:
        """Get result file object for the file with the given identifier.

        Parameters
        ----------
        key: string
            User-provided file identifier or the file source name.

        Returns
        -------
        flowserv.client.app.data.File
        """
        return self._files.get(key)

    def is_active(self) -> bool:
        """Check if the run state is in an active state (either PENDING or
        RUNNING).

        Returns
        -------
        bool
        """
        return self.is_pending() or self.is_running()

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