# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Utilities for workflow template backends. Provides functions to copy files
for workflow runs.
"""

import errno
import os
import shutil

from flowserv.core.files import FileHandle, InputFile
from flowserv.model.parameter.value import TemplateArgument

import flowserv.core.error as err
import flowserv.model.template.util as tmpl


def get_upload_files(template, basedir, files, arguments):
    """Get a list of all input files for a workflow template that need to be
    uploaded for a new workflow run. The list of files corresponds, for example,
    to the entries in the 'inputs.files' section of a REANA workflow
    specification.

    Returns a list of tuples containing the full path to the source file on
    local disk and the relative target path for the uploaded file.

    Raises errors if (i) an unknown parameter is referenced or (ii) if the type
    of a referenced parameter in the input files section is not of type file.

    Parameters
    ----------
    template: flowserv.model.template.base.WorkflowTemplate
        Workflow template containing the parameterized specification and the
        parameter declarations
    basedir: string
        Path to the base directory of the template folder containing static
        template files
    files: list(string)
        List of file references
    arguments: dict(flowserv.model.parameter.value.TemplateArgument)
        Dictionary of argument values for parameters in the template

    Returns
    -------
    list((string, string))

    Raises
    ------
    flowserv.core.error.InvalidTemplateError
    flowserv.core.error.MissingArgumentError
    flowserv.core.error.UnknownParameterError
    """
    result = list()
    for val in files:
        # Set source and target values depending on whether the list
        # entry references a template parameter or not
        if tmpl.is_parameter(val):
            var = tmpl.get_parameter_name(val)
            # Raise error if the type of the referenced parameter is
            # not file
            para = template.get_parameter(var)
            if not para.is_file():
                raise err.InvalidTemplateError('expected file parameter for \'{}\''.format(var))
            arg = arguments.get(var)
            if arg is None:
                if para.default_value is None:
                    raise err.MissingArgumentError(var)
                else:
                    # Set argument to file handle using the default value
                    # (assuming that the default points to a file in the
                    # template base directory).
                    if para.has_constant() and not para.as_input():
                        target_path = para.get_constant()
                    else:
                        target_path = para.default_value
                    arg = TemplateArgument(
                        parameter=para,
                        value=InputFile(
                            f_handle=FileHandle(
                                filename=os.path.join(
                                    basedir,
                                    para.default_value
                                )
                            ),
                            target_path=target_path
                        )
                    )
            # Get path to source file and the target path from the input
            # file handle
            source = arg.value.source()
            target = arg.value.target()
        else:
            source = os.path.join(basedir, val)
            target = val
        # Upload source file
        result.append((source, target))
    return result


def copy_files(files, target_dir):
    """Copy all code and input files to the target directory. Expects a list
    of tuples that contain the path to the source file on local disk and the
    relative target path for the file in the given target directory.

    Parameters
    ----------
    files: list((string, string))
        List of source,target path pairs for files that are being copied
    target_dir: string
        Target directory for copied files (e.g., base directory for a
        workflow run)
    """
    for source, target in files:
        dst = os.path.join(target_dir, target)
        # If the source references a directory the whole directory tree is
        # copied
        if os.path.isdir(source):
            shutil.copytree(src=source, dst=dst)
        else:
            # Based on https://stackoverflow.com/questions/2793789/create-destination-path-for-shutil-copy-files/3284204
            try:
                shutil.copy(src=source, dst=dst)
            except IOError as e:
                # ENOENT(2): file does not exist, raised also on missing dest
                # parent dir
                if e.errno != errno.ENOENT or not os.path.isfile(source):
                    raise
                # try creating parent directories
                os.makedirs(os.path.dirname(dst))
                shutil.copy(src=source, dst=dst)
