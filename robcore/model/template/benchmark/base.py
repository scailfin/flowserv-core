# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base classes for benchmarks that are maintained by the API in a benchmark
repository.
"""


class BenchmarkHandle(object):
    """Each benchmark is associated with a workflow template. The handle
    contains information about the benchmark that is maintained in addition to
    the workflow template.

    The workflow template may be loaded on demand through the given template
    repository. The main reason is that for benchmark listings we do not need
    to load the templates for all benchmarks immediately. When loading the
    template on demand it is assumed that the benchmark identifier is the same
    as the template identifier.
    """
    def __init__(
        self, identifier, name=None, description=None, instructions=None,
        template=None, repo=None
    ):
        """Initialize the handle properties. If no name is given the
        identifier is used as a name.

        If both, the template and the repository are None an error is raised
        since it would be impossible to access the associated workflow template.

        Parameters
        ----------
        identifier: string
            Unique benchmark identifier
        name: string, optional
            Descriptive benchmark name
        description: string, optional
            Optional short description for display in benchmark listings
        instructions: string, optional
            Text containing detailed instructions for benchmark participants
        template: robcore.model.template.base.WorkflowTemplate, optional
            Template for the associated workflow
        repo: robcore.model.template.benchmark.repo.BenchmarkRepository, optional
            Template repository to load the template on demand.

        Raises
        ------
        ValueError
        """
        # Raise an error if both, the template and the repository are None
        if template is None and repo is None:
            raise ValueError('no workflow template given')
        self.identifier = identifier
        self.name = name if not name is None else identifier
        self.description = description
        self.instructions = instructions
        self.template = template
        self.repo = repo

    def get_description(self):
        """Get value of description property. If the value of the property is
        None an empty string is returned instead.

        Returns
        -------
        string
        """
        return self.description if not self.description is None else ''

    def get_instructions(self):
        """Get value of instructions property. If the value of the property is
        None an empty string is returned instead.

        Returns
        -------
        string
        """
        return self.instructions if not self.instructions is None else ''

    def get_leaderboard(self, order_by=None, include_all=False):
        """Get current leaderboard for the benchmark. The result is a
        ranking of run results. Each entry contains the run and submission
        information, as well as a dictionary with the results of the respective
        workflow run.

        If the include_all flag is False at most one result per submission is
        included in the result.

        Parameters
        ----------
        order_by: list(robcore.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the schema
            default attribute is used
        include_all: bool, optional
            Include at most one entry per submission in the result if False

        Returns
        -------
        robcore.model.ranking.ResultRanking

        Raises
        ------
        robcore.error.UnknownBenchmarkError
        """
        return self.repo.get_leaderboard(
            benchmark_id=self.identifier,
            order_by=order_by,
            include_all=include_all
        )

    def get_template(self):
        """Get associated workflow template. The template is loaded on-demand
        if necessary.

        Returns
        -------
        robcore.model.template.base.WorkflowTemplate
        """
        # Load template if None
        if self.template is None:
            self.template = self.repo.template_repo.get_template(self.identifier)
        return self.template

    def has_description(self):
        """Shortcut to test of the description attribute is set.

        Returns
        -------
        bool
        """
        return not self.description is None

    def has_instructions(self):
        """Test if the instructions for the benchmark are set.

        Returns
        -------
        bool
        """
        return not self.instructions is None
