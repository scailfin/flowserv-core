# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for benchmark submissions."""

from robcore.view.run import RunSerializer

import robcore.view.hateoas as hateoas
import robcore.view.labels as labels


class SubmissionSerializer(object):
    """Serializer for benchmark submissions."""
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: robcore.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def file_handle(self, submission_id, fh):
        """Get serialization for a file handle.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        fh: robcore.core.files.FileHandle
            File handle

        Returns
        -------
        dict
        """
        del_url = self.urls.delete_file(submission_id, fh.identifier)
        dwnld_url = self.urls.download_file(submission_id, fh.identifier)
        return {
            labels.ID: fh.identifier,
            labels.NAME: fh.name,
            labels.CREATED_AT: fh.created_at.isoformat(),
            labels.FILESIZE: fh.size,
            labels.LINKS: hateoas.serialize({
                hateoas.action(hateoas.DOWNLOAD): dwnld_url,
                hateoas.action(hateoas.DELETE): del_url
            })
        }

    def file_listing(self, submission_id, files):
        """Get serialization for listing of uploaded files for a given
        submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        files: list(robcore.core.files.FileHandle)
            List of file handle

        Returns
        -------
        dict
        """
        return {
            labels.FILES: [self.file_handle(submission_id, fh) for fh in files],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_files(submission_id)
            })
        }

    def submission_descriptor(self, submission):
        """Get serialization for a submission descriptor. The descriptor
        contains the submission identifier, name, and the base list of HATEOAS
        references.

        Parameters
        ----------
        submission: robcore.model.submission.SubmissionHandle
            Submission handle

        Returns
        -------
        dict
        """
        s_id = submission.identifier
        b_id = submission.benchmark_id
        return {
            labels.ID: s_id,
            labels.NAME: submission.name,
            labels.BENCHMARK: submission.benchmark_id,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.get_submission(s_id),
                hateoas.BENCHMARK: self.urls.get_benchmark(b_id),
                hateoas.action(hateoas.UPLOAD): self.urls.upload_file(s_id),
                hateoas.action(hateoas.SUBMIT): self.urls.start_run(s_id)

            })
        }

    def submission_handle(self, submission):
        """Get serialization for a submission handle.

        Parameters
        ----------
        submission: robcore.model.submission.SubmissionHandle
            Submission handle

        Returns
        -------
        dict
        """
        doc = self.submission_descriptor(submission)
        members = list()
        for u in submission.get_members():
            members.append({labels.ID: u.identifier, labels.USERNAME: u.name})
        doc[labels.MEMBERS] = members
        parameters = submission.parameters.values()
        # Include submission specific list of benchmark template parameters
        doc[labels.PARAMETERS] = [p.to_dict() for p in parameters]
        # Include descriptors for all submission runs
        runs = RunSerializer(urls=self.urls)
        doc[labels.RUNS] = [
            runs.run_descriptor(r) for r in submission.get_runs()
        ]
        # Include handles for all uploaded files
        files = submission.list_files()
        doc[labels.FILES] = [self.file_handle(submission.identifier, f) for f in files]

        return doc

    def submission_listing(self, submissions):
        """Get serialization of the submission descriptor list.

        Parameters
        ----------
        submissions: list(robcore.model.submission.SubmissionHandle)
            List of submission handles

        Returns
        -------
        dict
        """
        return {
            labels.SUBMISSIONS: [
                self.submission_descriptor(s) for s in submissions
            ],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_submissions()
            })
        }
