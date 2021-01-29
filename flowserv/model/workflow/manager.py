# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow repository maintains information about registered workflow
templates. For each template additional basic information is stored in the
underlying database.
"""

import git
import os
import shutil
import tempfile

from contextlib import contextmanager
from typing import Optional

from flowserv.model.base import WorkflowObject
from flowserv.model.constraint import validate_identifier
from flowserv.model.workflow.manifest import WorkflowManifest
from flowserv.model.workflow.repository import WorkflowRepository
from flowserv.util import get_unique_identifier as unique_identifier

import flowserv.error as err
import flowserv.model.constraint as constraint


class WorkflowManager(object):
    """The workflow manager maintains information that is associated with
    workflow templates in a workflow repository.
    """
    def __init__(
        self, session, fs, idfunc=None, attempts=None, tmpl_names=None
    ):
        """Initialize the database connection, and the generator for workflow
        related file names and directory paths. The optional parameters are
        used to configure the identifier function that is used to generate
        unique workflow identifier as well as the list of default file names
        for template specification files.

        By default, short identifiers are used.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        fs: flowserv.model.files.FileStore
            File store for workflow files.
        """
        self.session = session
        self.fs = fs

    def create_workflow(
        self, source: str, identifier: Optional[str] = None,
        name: Optional[str] = None, description: Optional[str] = None,
        instructions: Optional[str] = None, specfile: Optional[str] = None,
        manifestfile: Optional[str] = None,
        ignore_postproc: Optional[bool] = False
    ) -> WorkflowObject:
        """Add new workflow to the repository. The associated workflow template
        is created in the template repository from either the given source
        directory or a Git repository. The template repository will raise an
        error if neither or both arguments are given.

        The method will look for a workflow description file in the template
        base folder with the name flowserv.json, flowserv.yaml, flowserv.yml
        (in this order). The expected structure of the file is:

        name: ''
        description: ''
        instructions: ''
        files:
            - source: ''
              target: ''
        specfile: '' or workflowSpec: ''

        An error is raised if both specfile and workflowSpec are present in the
        description file.

        Raises an error if no workflow name is given or if a given workflow
        name is not unique.

        Parameters
        ----------
        source: string
            Path to local template, name or URL of the template in the
            repository.
        identifier: string, default=None
            Unique user-defined workflow identifier.
        name: string, default=None
            Unique workflow name.
        description: string, default=None
            Optional short description for display in workflow listings.
        instructions: string, default=None
            File containing instructions for workflow users.
        specfile: string, default=None
            Path to the workflow template specification file (absolute or
            relative to the workflow directory).
        manifestfile: string, default=None
            Path to manifest file. If not given an attempt is made to read one
            of the default manifest file names in the base directory.
        ignore_postproc: bool, default=False
            Ignore post-processing workflow specification if True.

        Returns
        -------
        flowserv.model.base.WorkflowObject

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.InvalidTemplateError
        flowserv.error.InvalidManifestError
        ValueError
        """
        # Validate the given workflow identifier. This will raise a ValueError
        # if the identifier is invalid.
        validate_identifier(identifier)
        # If a repository Url is given we first clone the repository into a
        # temporary directory that is used as the workflow source directory.
        with clone(source) as (sourcedir, manifestpath):
            manifest = WorkflowManifest.load(
                basedir=sourcedir,
                manifestfile=manifestfile if manifestfile else manifestpath,
                name=name,
                description=description,
                instructions=instructions,
                specfile=specfile,
                existing_names=[wf.name for wf in self.list_workflows()]
            )
            template = manifest.template()
            # Create identifier for the workflow template.
            workflow_id = identifier if identifier else unique_identifier()
            staticdir = self.fs.workflow_staticdir(workflow_id)
            # Copy files from the project folder to the template's static file
            # folder. By default all files in the project folder are copied.
            self.fs.store_files(files=manifest.copyfiles(), dst=staticdir)

        # Insert workflow into database and return the workflow handle.
        workflow = WorkflowObject(
            workflow_id=workflow_id,
            name=manifest.name,
            description=manifest.description,
            instructions=manifest.instructions,
            workflow_spec=template.workflow_spec,
            parameters=template.parameters,
            parameter_groups=template.parameter_groups,
            outputs=template.outputs,
            postproc_spec=template.postproc_spec,
            ignore_postproc=ignore_postproc,
            result_schema=template.result_schema
        )
        self.session.add(workflow)
        return workflow

    def delete_workflow(self, workflow_id):
        """Delete the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Raises
        ------
        flowserv.error.UnknownWorkflowError
        """
        # Get the workflow and workflow directory. This will raise an error if
        # the workflow does not exist.
        workflow = self.get_workflow(workflow_id)
        # Delete the workflow from the database and commit changes.
        self.session.delete(workflow)
        self.session.commit()
        # Delete all files that are associated with the workflow if the changes
        # to the database were successful.
        self.fs.delete_folder(key=self.fs.workflow_basedir(workflow_id))

    def get_workflow(self, workflow_id):
        """Get handle for the workflow with the given identifier. Raises
        an error if no workflow with the identifier exists.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        flowserv.model.base.WorkflowObject

        Raises
        ------
        flowserv.error.UnknownWorkflowError
        """
        # Get workflow information from database. If the result is empty an
        # error is raised
        workflow = self.session\
            .query(WorkflowObject)\
            .filter(WorkflowObject.workflow_id == workflow_id)\
            .one_or_none()
        if workflow is None:
            raise err.UnknownWorkflowError(workflow_id)
        return workflow

    def list_workflows(self):
        """Get a list of descriptors for all workflows in the repository.

        Returns
        -------
        list(flowserv.model.base.WorkflowObject)
        """
        return self.session.query(WorkflowObject).all()

    def update_workflow(
        self, workflow_id, name=None, description=None, instructions=None
    ):
        """Update name, description, and instructions for a given workflow.

        Raises an error if the given workflow does not exist or if the name is
        not unique.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        name: string, optional
            Unique workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            Text containing detailed instructions for workflow execution

        Returns
        -------
        flowserv.model.base.WorkflowObject

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownWorkflowError
        """
        # Get the workflow from the database. This will raise an error if the
        # workflow does not exist.
        workflow = self.get_workflow(workflow_id)
        # Update workflow properties.
        if name is not None:
            # Ensure that the name is a valid name.
            constraint.validate_name(name)
            # Ensure that the name is unique.
            wf = self.session\
                .query(WorkflowObject)\
                .filter(WorkflowObject.name == name)\
                .one_or_none()
            if wf is not None and wf.workflow_id != workflow_id:
                msg = "workflow '{}' exists".format(name)
                raise err.ConstraintViolationError(msg)
            workflow.name = name
        if description is not None:
            workflow.description = description
        if instructions is not None:
            workflow.instructions = instructions
        return workflow


# -- Helper Methods -----------------------------------------------------------

@contextmanager
def clone(source, repository=None):
    """Clone a workflow template repository. If source points to a directory on
    local disk it is returned as the 'cloned' source directory. Otherwise, it
    is assumed that source either references a known template in the global
    workflow template repository or points to a git repository. The repository
    is cloned into a temporary directory which is removed when the generator
    resumes after the workflow has been copied to the local repository.

    Returns a tuple containing the path to the resulting template source
    directory on the local disk and the optional path to the template's
    manifest file.

    Parameters
    ----------
    source: string
        The source is either a path to local template directory, an identifer
        for a template in the global template repository, or the URL for a git
        repository.
    repository: flowserv.model.workflow.repository.WorkflowRepository,
            default=None
        Object providing access to the global workflow repository.

    Returns
    -------
    string, string
    """
    if os.path.isdir(source):
        # Return the source if it references a directory on local disk.
        yield source, None
    else:
        # Clone the repository that matches the given source into a temporary
        # directory on local disk.
        if repository is None:
            repository = WorkflowRepository()
        repourl, manifestpath = repository.get(source)
        sourcedir = tempfile.mkdtemp()
        if manifestpath is not None:
            manifestpath = os.path.join(sourcedir, manifestpath)
        try:
            git.Repo.clone_from(repourl, sourcedir)
            yield sourcedir, manifestpath
        except (IOError, OSError, git.exc.GitCommandError) as ex:
            raise ex
        finally:
            # Make sure to cleanup by removing the created teporary folder.
            shutil.rmtree(sourcedir)
