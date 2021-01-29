# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper functions for run arguments."""

from typing import Any, Dict, Optional, Tuple

import flowserv.util as util


# -- Serialize arguments ------------------------------------------------------

def deserialize_arg(doc: Dict) -> Tuple[str, Any]:
    """Get the parameter name and argument value from a run argument.

    Parameters
    ----------
    doc: dict
        User-provided argument value for a run parameter.

    Returns
    -------
    string, any
    """
    try:
        return doc['name'], doc['value']
    except KeyError as ex:
        raise ValueError('missing element {}'.format(str(ex)))


def serialize_arg(name: str, value: Any) -> Dict:
    """Get serialization for a run argument.

    Parameters
    ----------
    name: string
        Unique parameter identifier.
    value: any
        Argument value.

    Returns
    -------
    dict
    """
    return {'name': name, 'value': value}


# -- Serialize input file handles ---------------------------------------------

def deserialize_fh(doc: Dict) -> Tuple[str, str]:
    """Get the file identifier and optional target path from an input file
    argument.

    Parameters
    ----------
    doc: dict
        Input file argument value.

    Returns
    -------
    string, string
    """
    try:
        value = doc['value']
        return value['fileId'], value.get('targetPath')
    except KeyError as ex:
        raise ValueError('missing element {}'.format(str(ex)))


def is_fh(value: Any) -> bool:
    """Check whether an argument value is a serialization of an input file.
    Expects a dictionary with the following schema:

    {'type': '$file', 'value': {'fileId': 'string', 'targetPath': 'string'}}

    The target path is optional.

    Parameters
    ----------
    value: any
        User provided argument value.

    Returns
    -------
    bool
    """
    if not isinstance(value, dict):
        return False
    try:
        util.validate_doc(value, mandatory=['type', 'value'])
        assert value['type'] == '$file'
        util.validate_doc(
            value['value'],
            mandatory=['fileId'],
            optional=['targetPath']
        )
        return True
    except ValueError:
        pass
    return False


def serialize_fh(file_id: str, target: Optional[str] = None) -> Dict:
    """Get the file identifier and optional target path from an input file
    argument.

    Parameters
    ----------
    doc: dict
        Input file argument value.

    Returns
    -------
    dict
    """
    value = {'fileId': file_id}
    if target is not None:
        value['targetPath'] = target
    return {'type': '$file', 'value': value}
