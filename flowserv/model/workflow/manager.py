# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow repository maintains information about registered workflow
templates. For each template additional basic information is stored in the
underlying database.
"""

import errno
import git
import os
import shutil
import tempfile

from contextlib import contextmanager

from flowserv.model.base import WorkflowHandle
from flowserv.model.workflow.manifest import WorkflowManifest
from flowserv.model.workflow.repository import WorkflowRepository

import flowserv.error as err
import flowserv.util as util
import flowserv.model.constraint as constraint

""" "Default values for the max. attempts parameter and the ID generator
function.
"""
DEFAULT_ATTEMPTS = 100
DEFAULT_IDFUNC = util.get_short_identifier


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
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Generattor for file names and directory paths
        idfunc: func, optional
            Function to generate template folder identifier
        attempts: int, optional
            Maximum number of attempts to create a unique folder for a new
            workflow template
        tmpl_names: list(string), optional
            List of default names for template specification files
        """
        self.session = session
        self.fs = fs
        # Initialize the identifier function and the max. number of attempts
        # that are made to generate a unique identifier.
        self.idfunc = idfunc if idfunc is not None else DEFAULT_IDFUNC
        self.attempts = attempts if attempts is not None else DEFAULT_ATTEMPTS

    def create_workflow(
        self, source, name=None, description=None, instructions=None,
        specfile=None, manifestfile=None, ignore_postproc=False
    ):
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
        name: string, default=None
            Unique workflow name
        description: string, default=None
            Optional short description for display in workflow listings
        instructions: string, default=None
            File containing instructions for workflow users.
        specfile: string, default=None
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)
        manifestfile: string, default=None
            Path to manifest file. If not given an attempt is made to read one
            of the default manifest file names in the base directory.
        ignore_postproc: bool, default=False
            Ignore post-processing workflow specification if True.

        Returns
        -------
        flowserv.model.base.WorkflowHandle

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.InvalidTemplateError
        flowserv.error.InvalidManifestError
        ValueError
        """
        # If a repository Url is given we first clone the repository into a
        # temporary directory that is used as the workflow source directory.
        with clone(source) as sourcedir:
            manifest = WorkflowManifest.load(
                basedir=sourcedir,
                manifestfile=manifestfile,
                name=name,
                description=description,
                instructions=instructions,
                specfile=specfile,
                existing_names=[wf.name for wf in self.list_workflows()]
            )
            # Create identifier and folder for the workflow template. Create a
            # sub-folder for static template files that are copied from the
            # project folder.
            func = self.fs.workflow_basedir
            workflow_id, workflowdir = self.create_folder(dirfunc=func)
            staticdir = self.fs.workflow_staticdir(workflow_id)
            template = manifest.template(staticdir)
            # Copy files from the project folder to the template's static file
            # folder. By default all files in the project folder are copied.
            try:
                manifest.copyfiles(targetdir=staticdir)
            except (IOError, OSError, KeyError) as ex:
                shutil.rmtree(workflowdir)
                raise ex
        # Insert workflow into database and return the workflow handle.
        postproc_spec = template.postproc_spec if not ignore_postproc else None
        workflow = WorkflowHandle(
            workflow_id=workflow_id,
            name=manifest.name,
            description=manifest.description,
            instructions=manifest.instructions,
            workflow_spec=template.workflow_spec,
            parameters=template.parameters,
            modules=template.modules,
            outputs=template.outputs,
            postproc_spec=postproc_spec,
            result_schema=template.result_schema
        )
        self.session.add(workflow)
        # Set the static directory for the workflow handle.
        workflow.set_staticdir(staticdir)
        return workflow

    def create_folder(self, dirfunc):
        """Create a new unique folder in a base directory using the internal
        identifier function. The path to the created folder is generated using
        the given directory function that takes a unique identifier as the only
        argument.

        Returns a tuple containing the identifier and the directory. Raises
        an error if the maximum number of attempts to create the unique folder
        was reached.

        Parameters
        ----------
        dirfunc: func
            Function to generate the path for the created folder

        Returns
        -------
        (id::string, subfolder::string)

        Raises
        ------
        ValueError
        """
        identifier = None
        attempt = 0
        while identifier is None:
            # Create a new identifier
            identifier = self.idfunc()
            # Try to generate the subfolder. If the folder exists, set
            # identifier to None to signal failure.
            subfolder = dirfunc(identifier)
            if os.path.isdir(subfolder):
                identifier = None
            else:
                try:
                    os.makedirs(subfolder)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                    else:
                        # Directory must have been created concurrently
                        identifier = None
            if identifier is None:
                # Increase number of failed attempts. If the maximum number of
                # attempts is reached raise an errir
                attempt += 1
                if attempt > self.attempts:
                    raise RuntimeError('could not create unique folder')
        return identifier, subfolder

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
        workflowdir = self.fs.workflow_basedir(workflow_id)
        # Delete the workflow from the database and commit changes.
        self.session.delete(workflow)
        self.session.commit()
        # Delete all files that are associated with the workflow if the changes
        # to the database were successful.
        if os.path.isdir(workflowdir):
            shutil.rmtree(workflowdir)

    def get_workflow(self, workflow_id):
        """Get handle for the workflow with the given identifier. Raises
        an error if no workflow with the identifier exists.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        flowserv.model.base.WorkflowHandle

        Raises
        ------
        flowserv.error.UnknownWorkflowError
        """
        # Get workflow information from database. If the result is empty an
        # error is raised
        workflow = self.session\
            .query(WorkflowHandle)\
            .filter(WorkflowHandle.workflow_id == workflow_id)\
            .one_or_none()
        if workflow is None:
            raise err.UnknownWorkflowError(workflow_id)
        # Set the static directory for the workflow handle.
        workflow.set_staticdir(self.fs.workflow_staticdir(workflow_id))
        return workflow

    def list_workflows(self):
        """Get a list of descriptors for all workflows in the repository.

        Returns
        -------
        list(flowserv.model.base.WorkflowHandle)
        """
        workflows = list()
        for wf in self.session.query(WorkflowHandle).all():
            # Set the static directory for the workflow handle.
            wf.set_staticdir(self.fs.workflow_staticdir(wf.workflow_id))
            workflows.append(wf)
        return workflows

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
        flowserv.model.base.WorkflowHandle

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
                .query(WorkflowHandle)\
                .filter(WorkflowHandle.name == name)\
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

    Returns the path to the resulting template source directory on the local
    disk.

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
    string
    """
    if os.path.isdir(source):
        # Return the source if it references a directory on local disk.
        yield source
    else:
        # Clone the repository that matches the given source into a temporary
        # directory on local disk.
        if repository is None:
            repository = WorkflowRepository()
        repourl = repository.get(source)
        sourcedir = tempfile.mkdtemp()
        try:
            git.Repo.clone_from(repourl, sourcedir)
            yield sourcedir
        except (IOError, OSError, git.exc.GitCommandError) as ex:
            raise ex
        finally:
            # Make sure to cleanup by removing the created teporary folder.
            shutil.rmtree(sourcedir)
