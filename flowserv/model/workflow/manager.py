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
import re
import shutil
import tempfile

from flowserv.model.base import WorkflowHandle
from flowserv.model.template.base import WorkflowTemplate

import flowserv.error as err
import flowserv.util as util
import flowserv.model.constraint as constraint


""" "Default values for the max. attempts parameter and the ID generator
function.
"""
DEFAULT_ATTEMPTS = 100
DEFAULT_IDFUNC = util.get_short_identifier

"""Names for files that are used to identify template specification and project
descriptions.
"""
DEFAULT_SPECNAMES = ['benchmark', 'workflow', 'template']
DEFAULT_SPECSUFFIXES = ['.json', '.yaml', '.yml']
DEFAULT_TEMPLATES = list()
for name in DEFAULT_SPECNAMES:
    for suffix in DEFAULT_SPECSUFFIXES:
        DEFAULT_TEMPLATES.append(name + suffix)
# List of default configuration file names.
CONFIG_FILES = list()
for suffix in DEFAULT_SPECSUFFIXES:
    CONFIG_FILES.append('flowserv' + suffix)

"""Labels for the project description file."""
DESCRIPTION = 'description'
FILES = 'files'
INSTRUCTIONS = 'instructions'
NAME = 'name'
SOURCE = 'source'
SPECFILE = 'specfile'
TARGET = 'target'
WORKFLOWSPEC = 'workflowSpec'


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
        # Initialize the list of default template specification file names
        if tmpl_names is not None:
            self.default_filenames = tmpl_names
        else:
            self.default_filenames = DEFAULT_TEMPLATES

    def create_workflow(
        self, name=None, description=None, instructions=None, sourcedir=None,
        repourl=None, specfile=None, ignore_postproc=False
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
        name: string, optional
            Unique workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            File containing instructions for workflow users.
        sourcedir: string, optional
            Directory containing the workflow static files and the workflow
            template specification.
        repourl: string, optional
            Git repository that contains the the workflow files
        specfile: string, optional
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)
        ignore_postproc: bool, default=False
            Ignore post-processing workflow specification if True.

        Returns
        -------
        flowserv.model.base.WorkflowHandle

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.InvalidTemplateError
        ValueError
        """
        # Exactly one of sourcedir and repourl has to be not None. If both
        # are None (or not None) a ValueError is raised.
        if sourcedir is None and repourl is None:
            raise ValueError('no source folder or repository url given')
        elif sourcedir is not None and repourl is not None:
            raise ValueError('source folder and repository url given')
        # If a repository Url is given we first clone the repository into a
        # temporary directory that is used as the project base directory.
        projectdir = git_clone(repourl) if repourl is not None else sourcedir
        # Read project metadata from description file. Override with given
        # arguments
        try:
            projectmeta = read_description(
                projectdir=projectdir,
                name=name,
                description=description,
                instructions=instructions,
                specfile=specfile
            )
        except (IOError, OSError, ValueError) as ex:
            # Cleanup project directory if it was cloned from a git repository.
            if repourl is not None:
                shutil.rmtree(projectdir)
            raise ex
        # Create identifier and folder for the workflow template. Create a
        # sub-folder for static template files that are copied from the project
        # folder.
        workflow_id, workflowdir = self.create_folder(self.fs.workflow_basedir)
        staticdir = self.fs.workflow_staticdir(workflow_id)
        # Find template specification file in the template workflow folder.
        # If the file is not found, the workflow directory is removed and an
        # error is raised.
        template = read_template(
            projectmeta=projectmeta,
            projectdir=projectdir,
            templatedir=staticdir,
            default_filenames=self.default_filenames
        )
        if template is None:
            shutil.rmtree(workflowdir)
            raise err.InvalidTemplateError('no template file found')
        # Ensure that the workflow name is not empty, not longer than 512
        # character, and unique.
        try:
            get_unique_name(
                projectmeta=projectmeta,
                sourcedir=sourcedir,
                repourl=repourl,
                existing_names=[wf.name for wf in self.list_workflows()]
            )
        except err.ConstraintViolationError as ex:
            cleanup(
                workflowdir=workflowdir,
                projectdir=projectdir if repourl is not None else None
            )
            raise ex
        # Copy files from the project folder to the template's static file
        # folder. By default all files in the project folder are copied.
        try:
            copy_files(
                projectmeta=projectmeta,
                projectdir=projectdir,
                templatedir=staticdir
            )
            # Remove the project folder if it was created from a git repository
            if repourl is not None:
                shutil.rmtree(projectdir)
        except (IOError, OSError, KeyError) as ex:
            cleanup(
                workflowdir=workflowdir,
                projectdir=projectdir if repourl is not None else None
            )
            raise ex
        # Insert workflow into database and return the workflow handle.
        postproc_spec = template.postproc_spec if not ignore_postproc else None
        workflow = WorkflowHandle(
            workflow_id=workflow_id,
            name=projectmeta.get(NAME),
            description=projectmeta.get(DESCRIPTION),
            instructions=projectmeta.get(INSTRUCTIONS),
            workflow_spec=template.workflow_spec,
            parameters=template.parameters,
            modules=template.modules,
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

def cleanup(workflowdir, projectdir):
    """Remove created workflow directory in case of an error. If the project
    directory is given it contains a cloned git repository. In that case, the
    project directory will also be deleted.

    Parameters
    ----------
    workflowdir: string
        Path to created workflow template directory.
    projectdir: string
        Path to cloned git repository directory. Will be None if the project
        was not cloned.
    """
    shutil.rmtree(workflowdir)
    if projectdir is not None:
        shutil.rmtree(projectdir)


def copy_files(projectmeta, projectdir, templatedir):
    """Copy all template files from the project base folder to the template
    target folder in the repository. If the 'files' element in the project
    metadata is given, only the files referenced in the listing will be copied.
    By default, the whole project folder is copied.

    Parameters
    ----------
    projectmeta: dict
        Project metadata information from the project description file.
    projectdir: string
        Path to the base directory containing the project resource files.
    templatedir: string

    Raises
    ------
    IOError, KeyError, OSError
    """
    # Create a list of (source, target) pairs for the file copy statement.
    files = list()
    if FILES not in projectmeta:
        # If no files listing is present in the project metadata dictionary
        # copy the whole project directory to the source.
        for filename in os.listdir(projectdir):
            files.append((os.path.join(projectdir, filename), filename))
    else:
        for fspec in projectmeta.get(FILES, [{SOURCE: ''}]):
            source = os.path.join(projectdir, fspec[SOURCE])
            target = fspec.get(TARGET, '')
            if not os.path.isdir(source):
                if os.path.isdir(os.path.join(templatedir, target)):
                    # Esure that the target is a file and not a directory if
                    # the source is a file. If the target element in fspec is
                    # not set, the copy target would be a directory instead of
                    # a file.
                    target = os.path.join(target, os.path.basename(source))
            files.append((source, target))
    util.copy_files(files, templatedir)


def get_unique_name(projectmeta, sourcedir, repourl, existing_names):
    """Ensure that the workflow name in the project metadata is not empty, not
    longer than 512 character, and unique.

    Parameters
    ----------
    projectmeta: dict
        Project metadata information from the project description file.
    sourcedir: string
        Directory containing the workflow static files and the workflow
        template specification.
    repourl: string
        Git repository that contains the the workflow files
    existing_names: set
        Set of names for existing projects.

    Raises
    ------
    flowserv.error.ConstraintViolationError
    """
    name = projectmeta.get(NAME)
    # Ensure that the name is not None.
    if name is None:
        if sourcedir is not None:
            name = os.path.basename(sourcedir)
        else:
            name = repourl.split('/')[-1]
        if '.' in name:
            name = name[:name.find('.')]
        if '_' in name or '-' in name:
            name = ' '.join([t.capitalize() for t in re.split('[_-]', name)])
        else:
            name = name.capitalize()
    # Validate that the name is not empty and not too long.
    constraint.validate_name(name)
    # Ensure that the name is unique.
    if name in existing_names:
        # Find a a unique name that matches the template name (int)
        name_templ = name + ' ({})'
        count = 1
        while name_templ.format(count) in existing_names:
            count += 1
        name = name_templ.format(count)
        # Re-validate that the name is not too long.
        constraint.validate_name(name)
    projectmeta[NAME] = name


def git_clone(repourl):
    """Clone a git repository from a given Url into a temporary folder on the
    local disk.

    Parameters
    ----------
    repourl: string
        Url to git repository.

    Returns
    -------
    string
    """
    # Create a temporary folder for the git repository
    projectdir = tempfile.mkdtemp()
    try:
        git.Repo.clone_from(repourl, projectdir)
    except (IOError, OSError, git.exc.GitCommandError) as ex:
        # Make sure to cleanup by removing the created project folder
        shutil.rmtree(projectdir)
        raise ex
    return projectdir


def read_description(projectdir, name, description, instructions, specfile):
    """Read the project description file from the project folder. Looks for a
    file with the following names: flowserv.json, flowserv.yaml, flowserv.yml.

    Replaces properties with the given arguments. Raises a ValueError if no
    name or no workflow specification is present in the resulting metadata
    dictionary.

    Parameters
    ----------
    projectdir: string
        Path to the base directory containing the project resource files.
    name: string
        Unique workflow name
    description: string
        Optional short description for display in workflow listings
    instructions: string
        File containing instructions for workflow users.
    specfile: string
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)

    Returns
    -------
    dict

    Raises
    ------
    IOError, OSError, ValueError
    """
    doc = dict()
    for filename in CONFIG_FILES:
        filename = os.path.join(projectdir, filename)
        if os.path.isfile(filename):
            doc = util.read_object(filename)
            break
    # Raise an error if both the specification file and workflow specification
    # are present in the project description.
    if SPECFILE in doc and WORKFLOWSPEC in doc:
        msg = 'invalid project description: {} and {} given'
        raise ValueError(msg.format(SPECFILE, WORKFLOWSPEC))
    # Override metadata with given arguments
    if name is not None:
        doc[NAME] = name
    if description is not None:
        doc[DESCRIPTION] = description
    if instructions is not None:
        doc[INSTRUCTIONS] = instructions
    if specfile is not None:
        # Set the specification file if given. Remove a workflow specification
        # in the project description if it exists.
        doc[SPECFILE] = specfile
        if WORKFLOWSPEC in doc:
            del doc[WORKFLOWSPEC]
    # Read the instructions file if specified
    if INSTRUCTIONS in doc:
        with open(os.path.join(projectdir, doc[INSTRUCTIONS]), 'r') as f:
            doc[INSTRUCTIONS] = f.read().strip()
    return doc


def read_template(projectmeta, projectdir, templatedir, default_filenames):
    """Read the template specification file in the template workflow folder.
    If the file is not found None is returned.

    Parameters
    ----------
    projectmeta: dict
        Project metadata information from the project description file.
    projectdir: string
        Path to the base directory containing the project resource files.
    templatedir: string
        Path to the target directory for template resource files.
    default_filenames: list(string)
        List of default file names for workflow templates.

    Returns
    -------
    flowserv.model.template.base.WorkflowTemplate
    """
    if WORKFLOWSPEC in projectmeta:
        # If the project metadata contains the workflow specification we can
        # return immediately
        return WorkflowTemplate.from_dict(
            doc=projectmeta.get(WORKFLOWSPEC),
            sourcedir=templatedir,
            validate=True
        )
    # The list of candidate file names depends on whether the project metadata
    # contains a reference to the specification file or if we are looking for
    # a default file.
    if SPECFILE in projectmeta:
        candidates = [projectmeta[SPECFILE]]
    else:
        candidates = list()
        for filename in default_filenames:
            candidates.append(filename)
    for filename in candidates:
        filename = os.path.join(projectdir, filename)
        if os.path.isfile(filename):
            # Read template from file. If no error occurs the folder
            # contains a valid template.
            return WorkflowTemplate.from_dict(
                doc=util.read_object(filename),
                sourcedir=templatedir,
                validate=True
            )
