# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for benchmark runs."""

import robapi.serialize.hateoas as hateoas
import robapi.serialize.labels as labels


class RunSerializer(object):
    """Serializer for benchmark runs."""
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: robapi.service.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def run_descriptor(self, run):
        """Get serialization for a run descriptor. The descriptor contains the
        run identifier, state, timestampls, and the base list of HATEOAS
        references.

        Parameters
        ----------
        run: robapi.model.run.base.RunHandle
            Submission handle

        Returns
        -------
        dict
        """
        links = {hateoas.SELF: self.urls.get_run(run.identifier)}
        if run.is_active():
            url = self.urls.cancel_run(run.identifier)
            links[hateoas.action(hateoas.CANCEL)] = url
        else:
            url = self.urls.delete_run(run.identifier)
            links[hateoas.action(hateoas.DELETE)] = url
        doc = {
            labels.ID: run.identifier,
            labels.STATE: run.state.type_id,
            labels.CREATED_AT: run.state.created_at.isoformat(),
            labels.LINKS: hateoas.serialize(links)
        }
        if not run.is_pending():
            doc[labels.STARTED_AT] = run.state.started_at.isoformat()
        if run.is_canceled() or run.is_error():
            doc[labels.FINISHED_AT] = run.state.stopped_at.isoformat()
            doc[labels.MESSAGES] = run.state.messages
        elif run.is_success():
            doc[labels.FINISHED_AT] = run.state.finished_at.isoformat()
        return doc

    def run_handle(self, run):
        """Get serialization for a run handle. The run handle contains the same
        information than the run descriptor. In addition, the run handle also
        contains the run arguments.

        Parameters
        ----------
        run: robapi.model.run.base.RunHandle
            Submission handle

        Returns
        -------
        dict
        """
        doc = self.run_descriptor(run)
        doc[labels.ARGUMENTS] = [
            {
                labels.KEY: key,
                labels.VALUE: run.arguments[key]
            } for key in run.arguments
        ]
        return doc

    def run_listing(self, runs, submission_id):
        """Get serialization for a list of run handles.

        Parameters
        ----------
        runs: list(robapi.model.run.base.RunHandle)
            List of run handles
        submission_id: string
            Unique submission identifier

        Returns
        -------
        dict
        """
        return {
            labels.RUNS: [
                self.run_descriptor(r) for r in runs
            ],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_runs(submission_id),
                hateoas.SUBMIT: self.urls.submit_run(submission_id)
            })
        }
