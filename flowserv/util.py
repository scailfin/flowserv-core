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

import datetime
import io
import json
import os
import shutil
import traceback
import uuid
import yaml

from dateutil.parser import isoparse
from dateutil.tz import UTC
from typing import Any, Dict, IO, List, Optional, Type, Union


"""Identifier for supported data formats."""
FORMAT_JSON = 'JSON'
FORMAT_YAML = 'YAML'


# -- Datetime -----------------------------------------------------------------

def to_datetime(timestamp: str) -> datetime.datetime:
    """Converts a timestamp string in ISO format into a datatime object.

    Parameters
    ----------
    timstamp : string
        Timestamp in ISO format

    Returns
    -------
    datetime.datetime
        Datetime object
    """
    # Assumes a string in ISO format (with or without milliseconds)
    for format in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']:
        try:
            return datetime.datetime.strptime(timestamp, format)
        except ValueError:
            pass
    return isoparse(timestamp)


def utc_now() -> str:
    """Get the current time in UTC timezone as a string in ISO format.

    Returns
    -------
    string
    """
    return datetime.datetime.now(UTC).isoformat()


# -- I/O ----------------------------------------------------------------------

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


def create_directories(basedir: str, files: List[str]):
    """Create top-level folder for all files in a given list. The file list
    contains the path names of (result) files relative to a given base
    directory. All directories are created under the base directory.

    Parameters
    ----------
    basedir: string
        Base directory under which new directories are created
    files: list(string)
        Relative path names for (expected) result files.
    """
    for filename in files:
        dirname = os.path.dirname(filename)
        os.makedirs(os.path.join(basedir, dirname), exist_ok=True)


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
        Optional file format identifier. The default is YAML

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


# -- Misc ---------------------------------------------------------------------

def get_unique_identifier() -> str:
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')


def jquery(doc: Dict, path: List[str]) -> Any:
    """Json query to extract the value at the given path in a nested dictionary
    object.

    Returns None if the element that is specified by the path does not exist.

    Parameters
    ----------
    doc: dict
        Nested dictionary
    path: list(string)
        List of elements in the query path

    Returns
    -------
    any
    """
    if not path or not doc or not isinstance(doc, dict):
        # If the path or document is empty or the document is not a dictionary
        # return None.
        return None
    elif len(path) == 1:
        # If there is only one element in the path return the assocuated value
        # or None if the element does not exist
        return doc.get(path[0])
    else:
        # Recursively traverse the document
        return jquery(doc=doc.get(path[0], dict()), path=path[1:])


def stacktrace(ex):
    """Get list of strings representing the stack trace for a given exception.

    Parameters
    ----------
    ex: Exception
        Exception that was raised by flowServ code

    Returns
    -------
    list(string)
    """
    try:
        st = traceback.format_exception(type(ex), ex, ex.__traceback__)
    except (AttributeError, TypeError):  # pragma: no cover
        st = [str(ex)]
    return [line.strip() for line in st]


def validate_doc(
    doc: Dict,
    mandatory: Optional[List[str]] = None,
    optional: Optional[List[str]] = None,
    exception: Optional[Type] = ValueError
):
    """Raises error if a dictionary contains labels that are not in the given
    label lists or if there are labels in the mandatory list that are not in
    the dictionary. Returns the given dictionary (if valid).

    Paramaters
    ----------
    doc: dict
        Dictionary serialization of an object
    mandatory: list(string), default=None
        List of mandatory labels for the dictionary serialization
    optional: list(string), optional
        List of optional labels for the dictionary serialization
    exception: Error, default=ValueError
        Error class that is raised if validation fails. By default, a ValueError
        is raised.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    # Ensure that all mandatory labels are present in the dictionary
    labels = mandatory if mandatory is not None else list()
    for key in labels:
        if key not in doc:
            raise exception("missing element '{}'".format(key))
    # Raise error if additional elements are present in the dictionary
    if optional is not None:
        labels = labels + optional
    for key in doc:
        if key not in labels:
            raise exception("unknown element '{}'".format(key))
    return doc
