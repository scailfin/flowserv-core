# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for the reproducible open benchmark platform. Provides
methods to (i) read and write files in JSON and YAML format, (ii) create
directories, (iii) validate dictionaries, and (iv) to create of unique
identifiers.
"""

import datetime
import errno
import json
import os
import time
import uuid
import yaml


"""Identifier for supported data formats."""
FORMAT_JSON = 'JSON'
FORMAT_YAML = 'YAML'


def create_dir(directory):
    """Safely create the given directory path if it does not exist.

    Parameters
    ----------
    directory: string
        Path to directory that is being created.

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
    offset = datetime.datetime.fromtimestamp(now_timestamp) - datetime.datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset


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
    if not date is None:
        ts = from_utc_datetime(date)
    elif not text is None:
        ts = from_utc_datetime(to_datetime(text))
    return str(ts)[:-7]


def validate_doc(doc, mandatory_labels=None, optional_labels=None):
    """Raises error if a dictionary contains labels that are not in the given
    label lists or if there are labels in the mandatory list that are not in the
    dictionary. Returns the given dictionary (if valid).

    Paramaters
    ----------
    doc: dict
        Dictionary serialization of an object
    mandatory_labels: list(string)
        List of mandatory labels for the dictionary serialization
    optional_labels: list(string), optional
        List of optional labels for the dictionary serialization

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    # Ensure that all mandatory labels are present in the dictionary
    if not mandatory_labels is None:
        for key in mandatory_labels:
            if not key in doc:
                raise ValueError('missing element \'{}\''.format(key))
    # Raise error if additional elements are present in the dictionary
    labels = mandatory_labels if not mandatory_labels is None else list()
    if not optional_labels is None:
        labels = labels + optional_labels
    for key in doc:
        if not key in labels:
            raise ValueError('unknown element \'{}\''.format(key))
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
