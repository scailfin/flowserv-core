# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper functions to read workflow manifest files."""

import os
import re

from typing import List, Tuple

from flowserv.model.template.base import WorkflowTemplate

import flowserv.error as err
import flowserv.model.constraint as constraint
import flowserv.util as util


"""Names for files that are used to identify template specification and project
descriptions.
"""
# Order of recognized suffixes for template specification fles and flowserv
# manifests.
DEFAULT_SUFFIXES = ['.json', '.yaml', '.yml']

# Workflow template specification file names.
DEFAULT_SPECNAMES = ['benchmark', 'workflow', 'template']
TEMPLATE_FILES = list()
for name in DEFAULT_SPECNAMES:
    for suffix in DEFAULT_SUFFIXES:
        TEMPLATE_FILES.append('{}{}'.format(name, suffix))

# Flowserv manifest file names..
MANIFEST_FILES = ['flowserv{}'.format(suffix) for suffix in DEFAULT_SUFFIXES]

"""Regular expression for file includes in markdown."""
REGEX_INCLUDE = r'\{\{(.*?)\}\}'


class WorkflowManifest(object):
    """The workflow manifest contains the workflow specification, the workflow
    metadata (name, description and optional instructions), as well as the list
    of files that need to be copied when creating a local copy of a workflow
    template in a repository.
    """
    def __init__(
        self, basedir, name, workflow_spec, description=None,
        instructions=None, files=None
    ):
        """Initialize the workflow metadata and the workflow specification.

        Parameters
        ----------
        basedir: string
            Base directory containing workflow template source files.
        name: string
            Workflow name.
        workflow_spec: dict
            Workflow specification.
        descriptions: string, default=None
            Descriptive workflow text.
        instructions: string, default=None
            Detailed instructions for running the workflow.
        files" list, default=None
        """
        self.basedir = basedir
        self.name = name
        self.workflow_spec = workflow_spec
        self.description = description
        self.instructions = instructions
        self.files = files

    def copyfiles(self, dst: str) -> List[Tuple[str, str]]:
        """Get list of all template files from the base folder that need to be
        copied to the template folder of a workflow repository.

        The result is a list of tuples specifying the relative file source and
        target path. The target path for each file is a concatenation of the
        given destination base directory and the specified target path for the
        file or folder. If the list of files is undefined in the manifest, the
        result is a  tuple (None, dst) indicating that the full base directory
        is to be copied to the destination.

        Returns
        ------
        list of (string, string)
        """
        if self.files is None:
            return [(None, dst)]
        result = list()
        for f in self.files:
            source = f['source']
            target = f.get('target', source)
            target = target if dst is None else util.join(dst, target)
            result.append((source, target))
        return result

    @staticmethod
    def load(
        basedir, manifestfile=None, name=None, description=None,
        instructions=None, specfile=None, existing_names=set()
    ):
        """Read the workflow manifest from file. By default, an attempt is made
        to read a file with one the following names in the basedir (in the
        given order): flowserv.json, flowserv.yaml, flowserv.yml. If the
        manifest file parameter is given the specified file is being read
        instead.

        The parameters name, description, instructions, and specfile are used
        to override the respective properties in the manifest file.

        Raises a ValueError if no manifest file is found or if no name or
        workflow specification is present in the resulting manifest object.

        Parameters
        ----------
        basedir: string
            Path to the base directory containing the workflow files. This
            directory is used when reading the manifest file (if not given as
            argument) and the instructions file (if not given as argument).
        manifestfile: string, default=None
            Path to manifest file. If not given an attempt is made to read one
            of the default manifest file names in the base directory.
        name: string
            Unique workflow name
        description: string
            Optional short description for display in workflow listings
        instructions: string
            File containing instructions for workflow users.
        specfile: string
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)
        existing_names: set, default=set()
            Set of names for existing projects.

        Returns
        -------
        flowserv.model.workflow.manifest.WorkflowManifest

        Raises
        ------
        IOError, OSError, ValueError, flowserv.error.InvalidManifestError
        """
        doc = dict()
        if manifestfile is not None:
            doc = util.read_object(manifestfile)
        else:
            # Attempt to read default manifest files.
            for filename in MANIFEST_FILES:
                filename = os.path.join(basedir, filename)
                if os.path.isfile(filename):
                    doc = util.read_object(filename)
                    break
        # Validate the the manifest file.
        try:
            util.validate_doc(
                doc,
                optional=[
                    'name',
                    'description',
                    'instructions',
                    'files',
                    'specfile'
                ]
            )
            for obj in doc.get('files', []):
                util.validate_doc(
                    obj,
                    mandatory=['source'],
                    optional=['target']
                )
        except ValueError as ex:
            raise err.InvalidManifestError(str(ex))
        # Override metadata with given arguments
        if name is not None:
            doc['name'] = name
        if description is not None:
            doc['description'] = description
        # Raise error if no name or no workflow specification is present.
        if 'name' not in doc:
            raise err.InvalidManifestError('missing name')
        if 'specfile' not in doc and specfile is None:
            raise err.InvalidManifestError('missing workflow specification')
        # Ensure that the name is valid an unique.
        doc['name'] = unique_name(doc['name'], existing_names)
        # Read the instructions file if specified.
        if instructions is not None or 'instructions' in doc:
            filename = getfile(
                basedir=basedir,
                manifest_value=doc.get('instructions'),
                user_argument=instructions
            )
            doc['instructions'] = read_instructions(filename)
        # Get the workflow specification file.
        filename = getfile(
            basedir=basedir,
            manifest_value=doc.get('specfile'),
            user_argument=specfile
        )
        return WorkflowManifest(
            basedir=basedir,
            name=doc['name'],
            workflow_spec=util.read_object(filename),
            description=doc.get('description'),
            instructions=doc.get('instructions'),
            files=doc.get('files')
        )

    def template(self):
        """Get workflow template instance for the workflow specification that
        is included in the manifest.

        Returns
        -------
        flowserv.model.template.base.Workflowtemplate
        """
        return WorkflowTemplate.from_dict(
            doc=self.workflow_spec,
            validate=True
        )


# -- Helper Methods -----------------------------------------------------------

def getfile(basedir, manifest_value, user_argument):
    """Get name for a file that is referenced in a workflow manifest. If the
    user argument is given it overrides the respective value in the manifest.
    For user arguments we first assume that the path references a file on disk,
    either as absolute path or as a path relative to the current working
    directory. If no file exists at the specified location an attempt is made
    to read the file relative to the base directory. For manifest values, they
    are always assumed to be relative to the base directory.

    Parameters
    ----------
    basedir: string
    manifest_value: string
        Relative path to the file in the base directory.
    user_argument: string
        User provided value that overrides the manifest value. This value can
        be None.

    Returns
    -------
    string
    """
    if user_argument is not None:
        if os.path.isfile(user_argument):
            # If the user argument points to an existing file that file is
            # returned.
            return user_argument
        # Assume that the user argument points to a file relative to the base
        # directory.
        return os.path.join(basedir, user_argument)
    return os.path.join(basedir, manifest_value)


def read_instructions(filename: str) -> str:
    """Read instruction text from a given file. If the filename is None the
    result will be None as well.

    Returns
    -------
    string
    """
    # Read instructions from file if given.
    instruction_text = None
    if filename is not None:
        with open(filename, 'r') as f:
            instruction_text = f.read().strip()
        parent = os.path.dirname(os.path.abspath(filename))

        # Replace function for file imports.
        def replace_include(match):
            """Function to replace references to files in markdown text.

            All file names should be relative to the path of the main document that
            imports the file.

            Parameters
            ----------
            match: re.MatchObject
                Regular expression match object.

            Returns
            -------
            string
            """
            ref = match.group()
            # Strip expression of import reference syntax.
            expr = ref[2:-2]
            # Read file and return content.
            return read_instructions(os.path.join(parent, expr))

        return re.sub(REGEX_INCLUDE, replace_include, instruction_text)


def unique_name(name, existing_names):
    """Ensure that the workflow name in the project metadata is not empty, not
    longer than 512 character, and unique.

    Parameters
    ----------
    name: string
        Workflow name in manifest or given by user.
    existing_names: set
        Set of names for existing projects.

    Raises
    ------
    flowserv.error.ConstraintViolationError
    """
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
    return name
