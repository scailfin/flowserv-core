# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module contains base classes that define the components of run result
listings and leader boards.
"""


class ResultRanking(object):
    """The result ranking contains a sorted list of run results. The schema of
    the respective result value dictionary is defined by the given column list.
    """
    def __init__(self, columns, entries=None):
        """Initialize the object properties.

        Parameters
        ----------
        columns: list(flowserv.model.template.schema.ResultColumn)
            List of columns in the result schema
        entrties: list(flowserv.model.ranking.base.RunResult), optional
            List of entries in the result ranking
        """
        self.columns = columns
        self.entries = entries if entries is not None else list()

    def __iter__(self):
        """Make ranking iterable.

        Returns
        -------
        iterator
        """
        return iter(self.entries)

    def __len__(self):
        """Number of elements in the ranking.

        Returns
        -------
        int
        """
        return len(self.entries)

    def get(self, index):
        """Get ranking element at the given position.

        Parameters
        ----------
        index: int
            Index position for ranking element

        Returns
        -------
        flowserv.model.ranking.base.RunResult
        """
        return self.entries[index]

    def names(self):
        """List of unique identifier for columns in the result schema.

        Returns
        -------
        list(string)
        """
        return [col.identifier for col in self.columns]


class RunResult(object):
    """Handle for analytics results of a successful workflow run. Maintains the
    run identifier, run timestamps, group information, and a dictionary
    containing the generated values. The elements in the dictionary are defined
    by the result schema in the respective workflow template.
    """
    def __init__(
        self, run_id, group_id, group_name, created_at, started_at,
        finished_at, values
    ):
        """Initialize the object properties.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        group_id: string
            Unique workflow group identifier
        group_name: string
            Human-readable name of the workflow group
        created_at: datetime.datetime
            Timestamp of workflow creation
        started_at: datetime.datetime
            Timestamp when the workflow started running
        finished_at: datetime.datetime, optional
            Timestamp when workflow execution completed
        values: dict
            Dictionary of analytics results
        """
        self.run_id = run_id
        self.group_id = group_id
        self.group_name = group_name
        self.created_at = created_at
        self.started_at = started_at
        self.finished_at = finished_at
        self.values = values

    def exectime(self):
        """The execution time for the workflow run is the difference between
        the start timestamp and the end timestamp.

        Returns
        -------
        datetime.timedelta
        """
        return self.finished_at - self.started_at

    def get(self, name):
        """Get the result value for the schema attribute with the given name.

        Parameters
        ----------
        name: string
            Name of attribute in the result schema.

        Returns
        -------
        string, int, or float
        """
        return self.values.get(name)
