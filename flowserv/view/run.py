# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow runs."""

import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels


class RunSerializer(object):
    """Serializer for workflow runs."""
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: flowserv.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def run_descriptor(self, run):
        """Get serialization for a run descriptor. The descriptor contains the
        run identifier, state, timestampls, and the base list of HATEOAS
        references.

        Parameters
        ----------
        run: flowserv.model.run.base.RunDescriptor
            Run decriptor

        Returns
        -------
        dict
        """
        run_id = run.identifier
        links = {hateoas.SELF: self.urls.get_run(run_id)}
        if run.is_active():
            url = self.urls.cancel_run(run_id)
            links[hateoas.action(hateoas.CANCEL)] = url
        else:
            url = self.urls.delete_run(run_id)
            links[hateoas.action(hateoas.DELETE)] = url
            if run.is_success():
                url = self.urls.download_result_archive(run_id)
                links[hateoas.RESULTS] = url
        doc = {
            labels.ID: run_id,
            labels.STATE: run.state_type_id,
            labels.CREATED_AT: run.created_at.isoformat(),
            labels.LINKS: hateoas.serialize(links)
        }
        return doc

    def run_handle(self, run, group):
        """Get serialization for a run handle. The run handle extends the run
        descriptor with the run arguments, the parameter declaration taken from
        the workflow group handle (since it may differ from the parameter list
        of the workflow), and additional information associated with the run
        state.

        Parameters
        ----------
        run: flowserv.model.run.base.RunHandle
            Workflow run handle
        group: flowserv.model.group.base.GroupHandle
            Workflow group handle

        Returns
        -------
        dict
        """
        doc = self.run_descriptor(run)
        # Add run arguments
        doc[labels.ARGUMENTS] = [
            {
                labels.ID: key,
                labels.VALUE: run.arguments[key]
            } for key in run.arguments
        ]
        parameters = group.parameters.values()
        doc[labels.PARAMETERS] = [p.to_dict() for p in parameters]
        # Add additional information from the run state
        if not run.is_pending():
            doc[labels.STARTED_AT] = run.state.started_at.isoformat()
        if run.is_canceled() or run.is_error():
            doc[labels.FINISHED_AT] = run.state.stopped_at.isoformat()
            doc[labels.MESSAGES] = run.state.messages
        elif run.is_success():
            doc[labels.FINISHED_AT] = run.state.finished_at.isoformat()
            # Serialize file resources
            resources = list()
            for res in run.list_resources():
                r_url = self.urls.download_result_file(
                    run_id=run.identifier,
                    resource_id=res.resource_id
                )
                resources.append({
                    labels.ID: res.resource_id,
                    labels.NAME: res.resource_name,
                    labels.LINKS: hateoas.serialize({hateoas.SELF: r_url})
                })
            doc[labels.RESOURCES] = resources
        return doc

    def run_listing(self, runs, group_id):
        """Get serialization for a list of run handles.

        Parameters
        ----------
        runs: list(flowserv.model.run.base.RunDescriptor)
            List of run handles
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        dict
        """
        return {
            labels.RUNS: [
                self.run_descriptor(r) for r in runs
            ],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_runs(group_id),
                hateoas.SUBMIT: self.urls.start_run(group_id)
            })
        }
