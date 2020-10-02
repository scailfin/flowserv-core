# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow runs."""

from flowserv.view.base import Serializer


class RunSerializer(Serializer):
    """Serializer for workflow runs."""
    def __init__(self, labels=None):
        """Initialize serialization labels.

        Parameters
        ----------
        labels: object, default=None
            Object instance that contains the values for serialization labels.
        """
        super(RunSerializer, self).__init__(
            labels={
                'ARG_ID': 'id',
                'ARG_VALUE': 'value',
                'FILE_ID': 'id',
                'FILE_NAME': 'name',
                'FILE_TITLE': 'title',
                'FILE_CAPTION': 'caption',
                'FILE_WIDGET': 'widget',
                'FILE_FORMAT': 'format',
                'RUN_ARGUMENTS': 'arguments',
                'RUN_CREATED': 'createdAt',
                'RUN_ERRORS': 'messages',
                'RUN_FINISHED': 'finishedAt',
                'RUN_GROUP': 'groupId',
                'RUN_ID': 'id',
                'RUN_LIST': 'runs',
                'RUN_PARAMETERS': 'parameters',
                'RUN_FILES': 'files',
                'RUN_STARTED': 'startedAt',
                'RUN_STATE': 'state',
                'RUN_WORKFLOW': 'workflowId'
            },
            override_labels=labels
        )

    def runid_listing(self, runs):
        """Get serialization for a list of run identifier.

        Parameters
        ----------
        runs: list(string)
            List of run identifier

        Returns
        -------
        dict
        """
        LABELS = self.labels
        return {LABELS['RUN_LIST']: runs}

    def run_descriptor(self, run):
        """Get serialization for a run descriptor. The descriptor contains the
        run identifier, state, timestampls, and the base list of HATEOAS
        references.

        Parameters
        ----------
        run: flowserv.model.base.RunHandle
            Run decriptor

        Returns
        -------
        dict
        """
        LABELS = self.labels
        doc = {
            LABELS['RUN_ID']: run.run_id,
            LABELS['RUN_STATE']: run.state_type,
            LABELS['RUN_CREATED']: run.created_at
        }
        return doc

    def run_handle(self, run, group=None):
        """Get serialization for a run handle. The run handle extends the run
        descriptor with the run arguments, the parameter declaration taken from
        the workflow group handle (since it may differ from the parameter list
        of the workflow), and additional information associated with the run
        state.

        Parameters
        ----------
        run: flowserv.model.base.RunHandle
            Workflow run handle
        group: flowserv.model.base.GroupHandle, optional
            Workflow group handle. Missing for post-processing workflows

        Returns
        -------
        dict
        """
        LABELS = self.labels
        doc = self.run_descriptor(run)
        # Add information about the run workflow and the run group
        doc[LABELS['RUN_WORKFLOW']] = run.workflow_id
        if run.group_id is not None:
            doc[LABELS['RUN_GROUP']] = run.group_id
        # Add run arguments
        doc[LABELS['RUN_ARGUMENTS']] = run.arguments
        # Add group specific parameters
        if group is not None:
            parameters = group.parameters.values()
            doc[LABELS['RUN_PARAMETERS']] = [p.to_dict() for p in parameters]
        # Add additional information from the run state
        if not run.is_pending():
            doc[LABELS['RUN_STARTED']] = run.state().started_at
        if run.is_canceled() or run.is_error():
            doc[LABELS['RUN_FINISHED']] = run.state().stopped_at
            doc[LABELS['RUN_ERRORS']] = run.state().messages
        elif run.is_success():
            doc[LABELS['RUN_FINISHED']] = run.state().finished_at
            output_spec = run.outputs()
            # Serialize file resources. The default serialization contains the
            # file identifier and name. If an output specification is present
            # for the file the values for that specification will be added
            # to the serialization.
            files = list()
            for f in run.files:
                obj = {
                    LABELS['FILE_ID']: f.file_id,
                    LABELS['FILE_NAME']: f.name
                }
                if f.name in output_spec:
                    fspec = output_spec[f.name]
                    obj[LABELS['FILE_NAME']] = fspec.key
                    if fspec.title is not None:
                        obj[LABELS['FILE_TITLE']] = fspec.title
                    if fspec.caption is not None:
                        obj[LABELS['FILE_CAPTION']] = fspec.caption
                    if fspec.widget is not None:
                        obj[LABELS['FILE_WIDGET']] = fspec.widget
                    if fspec.format is not None:
                        obj[LABELS['FILE_FORMAT']] = fspec.format
                files.append(obj)
            doc[LABELS['RUN_FILES']] = files
        return doc

    def run_listing(self, runs, group_id):
        """Get serialization for a list of run handles.

        Parameters
        ----------
        runs: list(flowserv.model.base.RunHandle)
            List of run handles
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        dict
        """
        LABELS = self.labels
        return {LABELS['RUN_LIST']: [self.run_descriptor(r) for r in runs]}
