# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes for post-processing client that need to access the run
results that are included in the result folder that is provided as input to
post-processing workflows.
"""

import os

import flowserv.core.util as util
import flowserv.service.postproc.base as base


class Run(object):
    """Run result object containing the unique run identifier, the run name,
    and the list of resource files that were generated by the run and that are
    available to the post-processing workflow.
    """
    def __init__(self, identifier, name, resources):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique run identifier
        name: string
            Unique name of the workflow group that the run belongs to.
        resources: dict
            Dictionary of file resources that were created by the run and that
            are available to the post-processing workflow. Files in the
            dictionary are keyed by their name.
        """
        self.identifier = identifier
        self.name = name
        self.resources = resources

    def get_file(self, name):
        """Get the path to the run resource file with the given name.

        Parameters
        ----------
        name: string
            Unique file name

        Returns
        -------
        string
        """
        return self.resources.get(name)


class Runs(object):
    """List of run result handles that are available to the post-processing
    workflow. The order of runs in the list reflects the order in the result
    ranking.
    """
    def __init__(self, basedir):
        """Read the run result index file in the given base directory to
        initialize the result handles.

        Parameters
        ----------
        basedir: string
            Base directory for run result that have benn made available to the
            post-processing workflow
        """
        self.runs = list()
        doc = util.read_object(filename=os.path.join(basedir, base.RUNS_FILE))
        for obj in doc:
            run_id = obj[base.LABEL_ID]
            name = obj[base.LABEL_NAME]
            resources = dict()
            for filename in obj[base.LABEL_RESOURCES]:
                resources[filename] = os.path.join(basedir, run_id, filename)
            run = Run(identifier=run_id, name=name, resources=resources)
            self.runs.append(run)

    def __iter__(self):
        """Make list of run results iterable.

        Returns
        -------
        iterator
        """
        return iter(self.runs)

    def __len__(self):
        """Number of runs in the list.

        Returns
        -------
        int
        """
        return len(self.runs)

    def at_rank(self, index):
        """Get result handle for run at the given rank.

        Parameters
        ----------
        index: int

        Returns
        -------
        flowserv.service.postproc.client.Run
        """
        return self.runs[index]

    def get_run(self, run_id):
        """Get result handle for the run with the given identifier. Returns
        None if the run identifier is unknown.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.service.postproc.client.Run
        """
        for run in self.runs:
            if run.identifier == run_id:
                return run
