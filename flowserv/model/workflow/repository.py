# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class to access the repository of workflow templates."""

import requests

from typing import Dict, List, Tuple


"""Repository URL."""
URL = 'https://raw.githubusercontent.com/scailfin/flowserv-workflow-repository/master/templates.json'


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
    def __init__(self, templates: List[Dict] = None):
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

    def get(self, identifier: str) -> Tuple[str, str, Dict]:
        """Get the URL, the optional (relative) manifest file path, and optional
        additional arguments (e.g., the branch name) for the repository entry
        with the given identifier. If no entry matches the identifier it is
        returned as the function result.

        Returns a tuple of (url, manifestpath, args). If the manifest element
        is not present in the repository entry the second value is None. If
        no arguments are present the result is an empty dictionary.

        Parameters
        ----------
        identifier: string
            Workflow template identifier or repository URL.

        Returns
        -------
        string, string, dict
        """
        for obj in self.templates:
            if obj.get('id') == identifier:
                url = obj.get('url')
                manifestpath = obj.get('manifest')
                args = {arg['key']: arg['value'] for arg in obj.get('args', [])}
                return url, manifestpath, args
        return identifier, None, dict()

    def list(self) -> List[Tuple[str, str, str]]:
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
