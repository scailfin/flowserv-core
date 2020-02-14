# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for the reproducible open benchmark platform. Provides
methods to (i) read and write files in JSON and YAML format, (ii) create
directories, (iii) validate dictionaries, and (iv) to create of unique
identifiers.
"""

import abc
import datetime
import errno
import json
import os
import shutil
import time
import traceback
import uuid
import yaml


"""ABCMeta alternative."""
# compatible with Python 2 *and* 3:
# based on https://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})


"""Identifier for supported data formats."""
FORMAT_JSON = 'JSON'
FORMAT_YAML = 'YAML'


def copy_files(files, target_dir):
    """Copy list of files to a target directory. Expects a list of tuples that
    contain the path to the source file on local disk and the relative target
    path for the file in the given target directory.

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


def create_dir(directory, abs=False):
    """Safely create the given directory path if it does not exist.

    Parameters
    ----------
    directory: string
        Path to directory that is being created.
    abs: boolean, optional
        Return absolute path if true

    Returns
    -------
    string
    """
    # Based on https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    if abs:
        return os.path.abspath(directory)
    else:
        return directory


def get_unique_identifier():
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')


def get_short_identifier():
    """Create a unique identifier that contains only eigth characters. Uses the
    prefix of a unique identifier as the result.

    Returns
    -------
    string
    """
    return get_unique_identifier()[:8]


def from_utc_datetime(utc_datetime):
    """Convert a timestamp in UTC time to local time. This code is based on
    https://stackoverflow.com/questions/4770297/convert-utc-datetime-string-to-local-datetime

    Parameters
    ----------
    utc_datetime: datetime.datetime
        Timestamp in UTC timezone

    Returns
    -------
    datetime.datetime
    """
    now_timestamp = time.time()
    ts_now = datetime.datetime.fromtimestamp(now_timestamp)
    ts_utc = datetime.datetime.utcfromtimestamp(now_timestamp)
    offset = ts_now - ts_utc
    return utc_datetime + offset


def jquery(doc, path):
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


def read_object(filename, format=None):
    """Load a Json object from a file. The file may either be in Yaml or in Json
    format.

    Parameters
    ----------
    filename: string
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
    except (AttributeError, TypeError):
        st = [str(ex)]
    return [line.strip() for line in st]


def to_datetime(timestamp):
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
    # Do nothing if the timestamp is None
    if timestamp is None:
        return None
    # Assumes a string in ISO format (with or without milliseconds)
    try:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')


def to_localstr(date=None, text=None):
    """Convert a date or string representation of a timestamp in UTC timezone
    to local timezone. Removes the milli-seconds from the returned string.

    Parameters
    ----------
    date: datetime.datetime, optional
        Timestamp as datetime object
    text: string, optional
        Timestamp as string

    Returns
    -------
    string
    """
    if date is not None:
        ts = from_utc_datetime(date)
    elif text is not None:
        ts = from_utc_datetime(to_datetime(text))
    return str(ts)[:-7]


def validate_doc(doc, mandatory=None, optional=None):
    """Raises error if a dictionary contains labels that are not in the given
    label lists or if there are labels in the mandatory list that are not in
    the dictionary. Returns the given dictionary (if valid).

    Paramaters
    ----------
    doc: dict
        Dictionary serialization of an object
    mandatory: list(string)
        List of mandatory labels for the dictionary serialization
    optional: list(string), optional
        List of optional labels for the dictionary serialization

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    # Ensure that all mandatory labels are present in the dictionary
    if mandatory is not None:
        for key in mandatory:
            if key not in doc:
                raise ValueError("missing element '{}'".format(key))
    # Raise error if additional elements are present in the dictionary
    labels = mandatory if mandatory is not None else list()
    if optional is not None:
        labels = labels + optional
    for key in doc:
        if key not in labels:
            raise ValueError("unknown element '{}'".format(key))
    return doc


def write_object(filename, obj, format=None):
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
