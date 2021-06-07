# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Wrapper that provides access to a workflow via the service API."""

from io import BytesIO, StringIO
from typing import Dict, List, Optional

import time

from flowserv.client.app.run import Run
from flowserv.model.files import FileHandle
from flowserv.model.template.parameter import ParameterIndex
from flowserv.service.api import APIFactory
from flowserv.service.run.argument import serialize_arg, serialize_fh
from flowserv.volume.base import IOHandle, IOBuffer
from flowserv.volume.fs import FSFile

import flowserv.error as err
import flowserv.view.files as filelbls
import flowserv.view.group as glbls
import flowserv.view.workflow as wflbls


class Workflow(object):
    """Wrapper object for a single workflow. Maintains workflow metadata and
    provides functionality to execute and monitor workflow runs via the service
    API.
    """
    def __init__(self, workflow_id: str, group_id: str, service: APIFactory):
        """Initialize the required identifier and the API factory.

        Reads all metadata for the given workflow during intialization and
        maintains a copy in memory.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        group_id: string
            Unique workflow group identifier.
        service: flowserv.client.api.APIFactory
            Factory to create instances of the service API.
        """
        self.workflow_id = workflow_id
        self.group_id = group_id
        self.service = service
        # Get application properties from the database.
        with self.service() as api:
            wf = api.workflows().get_workflow(self.workflow_id)
            grp = api.groups().get_group(group_id=self.group_id)
        self._name = wf.get(wflbls.WORKFLOW_NAME)
        self._description = wf.get(wflbls.WORKFLOW_DESCRIPTION)
        self._instructions = wf.get(wflbls.WORKFLOW_INSTRUCTIONS)
        self._parameters = ParameterIndex.from_dict(grp[glbls.GROUP_PARAMETERS])

    def cancel_run(self, run_id: str, reason: List[str] = None):
        """Cancel the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        reason: string, optional
            Optional text describing the reason for cancelling the run
        """
        with self.service() as api:
            api.runs().cancel_run(run_id=run_id)

    def delete_run(self, run_id: str):
        """Delete the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to delete the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        """
        with self.service() as api:
            api.runs().delete_run(run_id=run_id)

    def description(self) -> str:
        """Get descriptive header for the application.

        Returns
        -------
        string
        """
        return self._description

    def get_file(self, run_id: str, file_id: Optional[str] = None) -> FileHandle:
        """Get handle for run result file. If the file identifier is not given
        the handle for the run result archive is returned.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string, default=None
            Unique file identifier.

        Returns
        -------
        flowserv.model.files.FileHandle
        """
        with self.service() as api:
            if file_id is not None:
                return api.runs().get_result_file(run_id=run_id, file_id=file_id)
            else:
                return api.runs().get_result_archive(run_id=run_id)

    def get_postproc_results(self) -> Run:
        """Get results of a post-processing run. The result is None if no
        entry for a post-porcessing run is found in the workflow handle.

        Returns
        -------
        flowserv.client.app.run.Run
        """
        with self.service() as api:
            doc = api.workflows().get_workflow(workflow_id=self.workflow_id)
            if wflbls.POSTPROC_RUN in doc:
                return Run(doc=doc[wflbls.POSTPROC_RUN], service=self.service)

    @property
    def identifier(self) -> str:
        """Get the identifier of the associated workflow.

        Returns
        -------
        string
        """
        return self.workflow_id

    def instructions(self) -> str:
        """Get instructions text for the application.

        Returns
        -------
        string
        """
        return self._instructions

    def name(self) -> str:
        """Get application title.

        Returns
        -------
        string
        """
        return self._name

    def parameters(self) -> ParameterIndex:
        """Get parameter declaration for application runs.

        Returns
        -------
        flowserv.model.template.parameters.ParameterIndex
        """
        return self._parameters

    def poll_run(self, run_id) -> Run:
        """Get run result handle for a given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.client.app.run.Run
        """
        with self.service() as api:
            return Run(
                doc=api.runs().get_run(run_id=run_id),
                service=self.service
            )

    def start_run(
        self, arguments: Dict, config: Optional[Dict] = None,
        poll_interval: Optional[int] = None
    ) -> Run:
        """Run the associated workflow for the given set of arguments.

        Parameters
        ----------
        arguments: dict
            Dictionary of user-provided arguments.
        config: dict, default=None
            Optional implementation-specific configuration settings that can be
            used to overwrite settings that were initialized at object creation.
        poll_interval: int, default=None
            Optional poll interval that is used to check the state of a run
            until it is no longer in active state.

        Returns
        -------
        flowserv.client.app.run.Run
        """
        arguments = self._parameters.set_defaults(arguments=arguments)
        with self.service() as api:
            # Upload any argument values as files that are either of type
            # StringIO or BytesIO.
            arglist = list()
            for key, val in arguments.items():
                # Convert arguments to the format that is expected by the run
                # manager. We pay special attention to file parameters. Input
                # files may be represented as strings, IO buffers or file
                # objects.
                para = self._parameters.get(key)
                if para is None:
                    raise err.UnknownParameterError(key)
                if para.is_file():
                    # Upload a given file prior to running the application.
                    upload_file = None
                    target = None
                    if isinstance(val, str):
                        upload_file = FSFile(val)
                    elif isinstance(val, StringIO):
                        buf = BytesIO(val.read().encode('utf8'))
                        upload_file = IOBuffer(buf)
                    elif isinstance(val, BytesIO):
                        upload_file = IOBuffer(val)
                    elif isinstance(val, IOHandle):
                        upload_file = val
                    else:
                        msg = 'invalid argument {} for {}'.format(key, val)
                        raise err.InvalidArgumentError(msg)
                    fh = api.uploads().upload_file(
                        group_id=self.group_id,
                        file=upload_file,
                        name=key
                    )
                    val = serialize_fh(fh[filelbls.FILE_ID], target=target)
                arglist.append(serialize_arg(key, val))
            # Execute the run and return the serialized run handle.
            run = api.runs().start_run(
                group_id=self.group_id,
                arguments=arglist,
                config=config
            )
            rh = Run(doc=run, service=self.service)
            # Wait for run to finish if active an poll interval is given.
            while poll_interval and rh.is_active():
                time.sleep(poll_interval)
                rh = self.poll_run(run_id=rh.run_id)
            pprun = self.get_postproc_results()
            if pprun is not None:
                while poll_interval and pprun.is_active():
                    time.sleep(poll_interval)
                    pprun = self.get_postproc_results()
            return rh
