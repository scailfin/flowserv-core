# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for the reproducible open benchmark platform. Provides
methods to (i) read and write files in JSON and YAML format, (ii) create
directories, (iii) validate dictionaries, and (iv) to create of unique
identifiers.
"""

import datetime
import errno
import io
import json
import os
import shutil
import tarfile
import time
import traceback
import uuid
import yaml

from dateutil.tz import UTC


"""Identifier for supported data formats."""
FORMAT_JSON = 'JSON'
FORMAT_YAML = 'YAML'


def archive_files(files):
    """Create a gzipped tar file containing all files in the given list. The
    input is expected to be a list of 2-tupes of (filename, archive-name).

    Parameters
    ----------
    files: list
        List of (filename, archive-name) for files that are added to the
        returned archive.

    Returns
    -------
    io.BytesIO
    """
    file_out = io.BytesIO()
    tar_handle = tarfile.open(fileobj=file_out, mode='w:gz')
    for filename, arcname in files:
        tar_handle.add(name=filename, arcname=arcname)
    tar_handle.close()
    file_out.seek(0)
    return file_out


def copy_files(files, target_dir, overwrite=True, raise_error=False):
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
    overwrite: bool, default=True
        Do not copy files if flag is False and the destination exists.
    raise_error: bool, default=False
        Raise an error when an existing target file is encountered if this flag
        is true and overwrite is False.
    """
    for source, target in files:
        # The target path is relative to the target directory. Create the
        # absolute path to the target destination. Ensure that the path does
        # not end with '/' since this confuses the dirname() method.
        dst = os.path.join(target_dir, target)
        while dst.endswith('/'):
            dst = dst[:-1]
        # Skip or raise an error if the destination exists and the overwrite
        # flag is False.
        if not overwrite and os.path.exists(dst):
            if raise_error:
                raise ValueError('{} exists'.format(dst))
            continue
        # Ensure that the parent directory of the target exists.
        dst_parent = os.path.dirname(dst)
        if dst_parent and not os.path.isdir(dst_parent):
            os.makedirs(dst_parent)
        # If the source references a directory the whole directory tree is
        # copied. Otherwise, a single file is copied.
        if os.path.isdir(source):
            # If we are overwriting an existing directory we need to remove the
            # directory first.
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src=source, dst=dst)
        else:
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
    # Based on https://stackoverflow.com/questions/273192/
    # how-can-i-safely-create-a-nested-directory
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:  # pragma: no cover
            if e.errno != errno.EEXIST:
                raise
    if abs:
        return os.path.abspath(directory)
    else:
        return directory


def create_directories(basedir, files):
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
        # Create the directory if it does not exist
        parentdir = os.path.join(basedir, dirname)
        if not os.path.isdir(parentdir):
            os.makedirs(parentdir)


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
    https://stackoverflow.com/questions/4770297/
    convert-utc-datetime-string-to-local-datetime

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
    """Load a Json object from a file. The file may either be in Yaml or in
    Json format.

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
    except (AttributeError, TypeError):  # pragma: no cover
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
    # Assumes a string in ISO format (with or without milliseconds)
    try:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')


def utc_now():
    """Get the current time in UTC timezone as a string in ISO format.

    Returns
    -------
    string
    """
    return datetime.datetime.now(UTC).isoformat()


def validate_doc(doc, mandatory=None, optional=None):
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
            raise ValueError("missing element '{}'".format(key))
    # Raise error if additional elements are present in the dictionary
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
