# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for the reproducible open benchmark platform. Provides
methods to (i) read and write files in JSON and YAML format, (ii) create
directories, (iii) validate dictionaries, and (iv) to create of unique
identifiers.
"""

import io
import json
import os
import shutil
import yaml

from yamlinclude import YamlIncludeConstructor
from typing import Dict, IO, List, Optional, Union


"""Identifier for supported data formats."""
FORMAT_JSON = 'JSON'
FORMAT_YAML = 'YAML'


def cleardir(directory: str):
    """Remove all files in the given directory.

    Parameters
    ----------
    directory: string
        Path to directory that is being created.
    """
    # If the directory does not exist there is nothing that needs to be cleared.
    if not os.path.isdir(directory):
        return
    for filename in os.listdir(directory):
        # Distinguish between files and subfolders.
        file = os.path.join(directory, filename)
        if os.path.isfile(file) or os.path.islink(file):
            os.unlink(file)
        else:
            shutil.rmtree(file)


def dirname(key: str) -> str:
    """Get the parent directory for a given file identifier.

    Parameters
    ----------
    key: str
        Relative file path expression.

    Returns
    -------
    str
    """
    return '/'.join(key.split('/')[:-1]) if key else None


def filepath(key: str, sep: Optional[str] = os.sep) -> str:
    """Convert a given file path to a local path.

    Replaces the default path separator '/' with the OS-specific separator if
    it is different from the default one.

    Parameters
    ----------
    key: str
        Relative file path expression.
    sep: string, default=OS file path separator
        OS-specific file path separator.

    Returns
    -------
    str
    """
    if key and sep != '/':
        key = key.replace('/', sep)
    return key


def join(*args) -> str:
    """Concatenate a list of values using the key path separator '/'.

    Parameters
    ----------
    args: list
        List of argument values.

    Returns
    -------
    str
    """
    return '/'.join([str(v) for v in args if v])


def read_buffer(filename: str) -> IO:
    """Read content from specified file into a BytesIO buffer.

    Parameters
    ----------
    filename: string
        Path tpo file on disk.

    Returns
    -------
    io.BytesIO
    """
    buf = io.BytesIO()
    with open(filename, 'rb') as f:
        buf.write(f.read())
    buf.seek(0)
    return buf


def read_object(filename: str, format: Optional[str] = None) -> Dict:
    """Load a Json object from a file. The file may either be in Yaml or in
    Json format.

    Parameters
    ----------
    filename: string or io.BytesIO
        Path to file on disk
    format: string, optional
        Optional file format identifier. The default is YAML.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    # If the file is of type BytesIO we cannot guess the format from the file
    # name. In this case the format is expected to be given as a parameter.
    # By default, JSON is assumed.
    if isinstance(filename, io.BytesIO):
        if format == FORMAT_YAML:
            return yaml.load(filename, Loader=yaml.FullLoader)
        else:
            return json.load(filename)
    # Guess format based on file suffix if not given
    if format is None:
        if filename.endswith('.json'):
            format = FORMAT_JSON
        else:
            format = FORMAT_YAML
    if format.upper() == FORMAT_YAML:
        YamlIncludeConstructor.add_to_loader_class(
            loader_class=yaml.FullLoader,
            base_dir=os.path.dirname(os.path.abspath(filename))
        )
        with open(filename, 'r') as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)
    elif format.upper() == FORMAT_JSON:
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        raise ValueError('unknown data format \'' + str(format) + '\'')


def write_object(
    filename: str, obj: Union[Dict, List], format: Optional[str] = None
):
    """Write given dictionary to file as Json object.

    Parameters
    ----------
    filename: string
        Path to output file
    obj: dict
        Output object

    Raises
    ------
    ValueError
    """
    if format is None:
        if filename.endswith('.json'):
            format = FORMAT_JSON
        else:
            format = FORMAT_YAML
    if format.upper() == FORMAT_YAML:
        with open(filename, 'w') as f:
            yaml.dump(obj, f)
    elif format.upper() == FORMAT_JSON:
        with open(filename, 'w') as f:
            json.dump(obj, f)
    else:
        raise ValueError('unknown data format \'' + str(format) + '\'')
