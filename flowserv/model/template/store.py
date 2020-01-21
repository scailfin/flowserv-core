# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for managing workflow templates in a repository on the file
system.
"""

import git
import os
import shutil

from flowserv.core.objstore.json import JsonFileStore
from flowserv.model.template.base import WorkflowTemplate

import flowserv.core.error as err
import flowserv.core.util as util


""" "Default values for the max. attempts parameter and the ID generator
function.
"""
DEFAULT_ATTEMPTS = 100
DEFAULT_IDFUNC = util.get_short_identifier


"""Names for files and folders that are used to identify and maintain template
information.
"""
DEFAULT_SPECNAMES = ['benchmark', 'workflow', 'template']
DEFAULT_SPECSUFFIXES = ['.yml', '.yaml', '.json']
STATIC_DIR = 'static'
TEMPLATE_FILENAME = 'template.json'


class TemplateRepository(object):
    """The template repository maintains a set of workflow templates. For each
    workflow template a copy of the static files that are used as input in the
    workflow is maintained.
    """
    def __init__(
        self, basedir, objstore=None, default_filenames=None, idfunc=None,
        max_attempts=None
    ):
        """Initialize the identifier function that is used to generate unique
        template identifier. By default, short identifiers are used.

        Parameters
        ----------
        basedir: string
            Path to the base directory that contains template files
        objstore: flowserv.core.objstore.base.ObjectStore, optional
            Store for workflow template specification files
        default_filenames: list(string), optional
            List of default names for template specification files
        idfunc: func, optional
            Function to generate template folder identifier
        max_attempts: int, optional
            Maximum number of attempts to create a unique folder for a new
            workflow template
        """
        # Ensure that the base directory exists.
        self.basedir = util.create_dir(basedir)
        # Initialize the template store
        if objstore is not None:
            self.objstore = objstore
        else:
            self.objstore = JsonFileStore(
                basedir=self.basedir,
                default_filename=TEMPLATE_FILENAME
            )
        # Initialize the template identifier function and the max. number of
        # attempst that are made to generate a unique identifier.
        self.idfunc = idfunc if idfunc is not None else DEFAULT_IDFUNC
        if max_attempts is not None:
            self.max_attempts = max_attempts
        else:
            self.max_attempts = DEFAULT_ATTEMPTS
        # Initialize the list of default template specification file names
        if default_filenames is not None:
            self.default_filenames = default_filenames
        else:
            self.default_filenames = list()
            for name in DEFAULT_SPECNAMES:
                for suffix in DEFAULT_SPECSUFFIXES:
                    self.default_filenames.append(name + suffix)

    def add_template(self, sourcedir=None, repourl=None, specfile=None):
        """Add a template to the repository. The associated workflow template
        is created in the template repository from either the given source
        directory or Git repository. The template repository will raise an
        error if neither or both arguments are given.

        Creates a new folder with unique name in the base directory of the
        template store. The created folder will contain a copy of the source
        folder or the git repository  (as subfolder named 'static').

        The source folder is expected to contain the template specification
        file. If the specfile is not given the method will look for a file
        using the entries in the list of default file names. If no template
        file is found in the source folder a ValueError is raised.

        Returns the unique identifier for the created template entry and the
        template handle.

        Parameters
        ----------
        sourcedir: string, optional
            Directory containing the workflow components, i.e., the fixed
            files and the template specification (optional).
        repourl: string, optional
            Git repository that contains the the workflow components
        specfile: string, optional
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)

        Returns
        -------
        string, flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        ValueError
        """
        # Exactly one of sourcedir and repourl has to be not None. If both
        # are None (or not None) a ValueError is raised.
        if sourcedir is None and repourl is None:
            raise ValueError('no source folder or repository url given')
        elif sourcedir is not None and repourl is not None:
            raise ValueError('source folder and repository url given')
        # Get unique identifier and create a new folder for the template files
        # and resources
        identifier = self.get_unique_identifier()
        templatedir = util.create_dir(os.path.join(self.basedir, identifier))
        # Copy either the given workflow directory into the created template
        # folder or clone the Git repository.
        try:
            staticdir = os.path.join(templatedir, STATIC_DIR)
            if sourcedir is not None:
                shutil.copytree(src=sourcedir, dst=staticdir)
            else:
                git.Repo.clone_from(repourl, staticdir)
        except (IOError, OSError, git.exc.GitCommandError) as ex:
            # Make sure to cleanup by removing the created template folder
            shutil.rmtree(templatedir)
            raise ex
        # Find template specification file in the template workflow folder.
        # If the file is not found the template directory is removed and a
        # ValueError is raised.
        template = None
        candidates = list()
        for filename in self.default_filenames:
            candidates.append(os.path.join(staticdir, filename))
        if specfile is not None:
            candidates = [specfile] + candidates
        for filename in candidates:
            if os.path.isfile(filename):
                # Read template from file. If no error occurs the folder
                # contains a valid template.
                template = WorkflowTemplate.from_dict(
                    doc=util.read_object(filename),
                    sourcedir=staticdir,
                    validate=True
                )
                # Store serialized template handle on disk
                self.objstore.write(
                    identifier=identifier,
                    obj=template.to_dict()
                )
                break
        # No template file found. Cleanup and raise error.
        if template is None:
            shutil.rmtree(templatedir)
            raise err.InvalidTemplateError('no template file found')
        # Return the template handle
        return identifier, template

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
        # Remove the template directory if it exists
        templatedir = os.path.join(self.basedir, identifier)
        if os.path.isdir(templatedir):
            shutil.rmtree(templatedir)
            return True
        else:
            return False

    def get_template(self, identifier):
        """Get handle for the template with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique template identifier

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.UnknownTemplateError
        """
        # The underlying object store will raise an UnknownObjectError if the
        # template is unknown.
        try:
            return WorkflowTemplate.from_dict(
                doc=self.objstore.read(identifier),
                sourcedir=os.path.join(self.basedir, identifier, STATIC_DIR)
            )
        except err.UnknownObjectError:
            raise err.UnknownTemplateError(identifier)

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
            identifier = self.idfunc()
            if os.path.isdir(os.path.join(self.basedir, identifier)):
                identifier = None
                attempt += 1
                if attempt > self.max_attempts:
                    raise RuntimeError('could not create unique directory')
        return identifier
