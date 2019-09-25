# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes and methods for benchmark repositories."""

class DictRepo(object):
    """Fake template repository for on-demand loading. Maintains a dictionary
    of template objects for on-demand load.
    """
    def __init__(self, templates=None):
        """initialize the dictionary of availab;e templates.

        Parameters
        ----------
        templates, dict, optional
            Dictionary of available template
        """
        self.templates = templates if not templates is None else dict()

    def get_template(self, identifier):
        """Load template with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique template identifier

        Returns
        -------
        any
        """
        return self.templates[identifier]

    @property
    def template_repo(self):
        """Necessary if the benchmark handle expects a benchmark repository."""
        return self
