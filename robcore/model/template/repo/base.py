# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base classes and interfaces for template repositories. Contains the abstract
repository interface and the template handle that represents entries in a
repository.
"""

from abc import abstractmethod

from robcore.model.template.base import WorkflowTemplate

import robcore.util as util


""" "Default value for max. attempts parameters."""
DEFAULT_MAX_ATTEMPTS = 100


class TemplateHandle(WorkflowTemplate):
    """The template handle extends the workflow template with the path to the
    base directory that contains all the static workflow files.
    """
    def __init__(
        self, workflow_spec, source_dir, identifier=None, parameters=None,
        result_schema=None
    ):
        """Initialize the components of the template handle.

        Parameters
        ----------
        workflow_spec: dict
            Workflow specification object
        source_dir: string
            Path to the base directory that contains the static workflow files
        identifier: string, optional
            Unique template identifier. If no value is given a UUID will be
            assigned.
        parameters: dict(string:robcore.model.template.parameter.base.TemplateParameter), optional
            Dictionary of workflow template parameter declarations keyed by
            their unique identifier.
        result_schema: robcore.model.template.schema.ResultSchema
            Schema of the result for extended templates that define benchmarks.

        Raises
        ------
        robcore.error.InvalidTemplateError
        """
        super(TemplateHandle, self).__init__(
            workflow_spec=workflow_spec,
            identifier=identifier,
            parameters=parameters,
            result_schema=result_schema
        )
        self.source_dir = source_dir


class TemplateRepository(object):
    """The template repository maintains a set of workflow templates. For each
    workflow template a copy of the static files that are used as input in the
    workflow is maintained.

    In addition to workflow templates the results of individual template runs
    are maintained as well.
    """
    def __init__(self, id_func=None, max_attempts=DEFAULT_MAX_ATTEMPTS):
        """Initialize the identifier function that is used to generate unique
        template identifier. By default, short identifier are used.

        Parameters
        ----------
        id_func: func, optional
            Function to generate template folder identifier
        max_attempts: int, optional
            Maximum number of attempts to create a unique folder for a new
            workflow template
        """
        self.id_func = id_func if not id_func is None else util.get_short_identifier
        self.max_attempts = max_attempts

    @abstractmethod
    def add_template(self, src_dir=None, src_repo_url=None, spec_file=None):
        """Create file and folder structure for a new workflow template. Assumes
        that either a workflow source directory or the Url of a remote Git
        repository is given.

        Each template is assigned a unique identifier. Creates a copy of the
        file structure for the static workflow template files.

        The source folder is expected to contain the template specification
        file. If the template_spec file is not given the method will look for a
        file from a default list of file names. If no template file is found in
        the source folder a ValueError is raised.

        Parameters
        ----------
        name: string
            Unique template name for display purposes
        description: string, optional
            Optional short description for display in listings
        instructions: string, optional
            Optional text containing detailed instructions for benchmark
            participants
        src_dir: string, optional
            Directory containing the workflow components, i.e., the fixed
            files and the template specification (optional).
        src_repo_url: string, optional
            Git repository that contains the the workflow components
        spec_file: string, optional
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)

        Returns
        -------
        robcore.model.template.repo.base.TemplateHandle

        Raises
        ------
        robcore.error.InvalidParameterError
        robcore.error.InvalidTemplateError
        ValueError
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_template(self, identifier):
        """Delete all resources that are associated with the given template.
        The result is True if the template existed and False otherwise.

        Parameters
        ----------
        identifier: string
            Unique template identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def exists_template(self, identifier):
        """Test if a template with the given identifier exists.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def get_template(self, identifier):
        """Get handle for the template with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique template identifier

        Returns
        -------
        robcore.model.template.repo.base.TemplateHandle

        Raises
        ------
        robcore.error.UnknownTemplateError
        """
        raise NotImplementedError()

    def get_unique_identifier(self):
        """Create a new unique identifier for a workflow template.

        Returns
        -------
        string

        Raises
        ------
        ValueError
        """
        identifier = None
        attempt = 0
        while identifier is None:
            identifier = self.id_func()
            if self.exists_template(identifier):
                identifier = None
                attempt += 1
                if attempt > self.max_attempts:
                    raise RuntimeError('could not create unique directory')
        return identifier
