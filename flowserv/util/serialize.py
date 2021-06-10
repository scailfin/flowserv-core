# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for object serialization and deserialization."""

from typing import Dict, List


def to_dict(args: List[Dict]) -> Dict:
    """Convert a list of serialized key-value pairs into a dictionary that maps
    the keys to their values.

    Parameters
    ----------
    args: list
        List of dictionary serializations for key-value pairs.

    Returns
    -------
    dict
    """
    return {a['key']: a['value'] for a in args}


def to_kvp(key: str, value: str) -> Dict:
    """Serialize a key-value pair into a dictionary.

    Parameters
    ----------
    key: string
        Key value
    value: string
        Associate value for the key.

    Returns
    -------
    dict
    """
    return {'key': key, 'value': value}
