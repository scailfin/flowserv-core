# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class to access the repository of workflow templates."""

import requests


"""Repository URL."""
URL = 'https://raw.githubusercontent.com/scailfin/flowserv-workflow-repository/master/templates.json'  # noqa: E501


class WorkflowRepository(object):
    """Repository for workflow specifications. The repository is currently
    maintained as a Json file on GitHub. The file contains an array of objects.
    Each object describes an installable workflow template with the following
    elements:

    - id: unique human-readable template identifier
    - description: short description
    - url: Url to a git repository that contains the workflow files
    - manifest: optional (relative) path for manifest file in the workflow
        repository.
    """
    def __init__(self, templates=None):
        """Initialize the list of templates in the global repository. Reads the
        repository file if no list is given.

        Parameters
        ----------
        template: list
            List of dictionaries representing installable workflow templates.
        """
        if templates is None:
            r = requests.get(URL)
            r.raise_for_status()
            templates = r.json()
        self.templates = templates

    def get(self, identifier):
        """Get the URL for the repository entry with the given identifier. If
        no entry matches the identifier it is returned as the function result.

        Parameters
        ----------
        identifier: string
            Workflow template identifier or repository URL.

        Returns
        -------
        string
        """
        for obj in self.templates:
            if obj.get('id') == identifier:
                return obj.get('url')
        return identifier

    def list(self):
        """Get list of tuples containing the template identifier, descriptions,
        and repository URL.

        Returns
        -------
        list
        """
        result = list()
        for obj in self.templates:
            entry = obj.get('id'), obj.get('description'), obj.get('url')
            result.append(entry)
        return result
