# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper functions for run arguments."""

import flowserv.util as util


# -- General arguments --------------------------------------------------------

def ARG(para_id, value):
    """Get serialization for a run argument.

    Parameters
    ----------
    para_id: string
        Unique parameter identifier.
    value: any
        Argument value.

    Returns
    -------
    dict
    """
    return {'id': para_id, 'value': value}


def GET_ARG(doc):
    """Get the parameter identifier and argument value from a run argument.

    Parameters
    ----------
    doc: dict
        User-provided argument value for a run parameter.

    Returns
    -------
    string, any
    """
    try:
        return doc['id'], doc['value']
    except KeyError as ex:
        raise ValueError('missing element {}'.format(str(ex)))


# -- Input files --------------------------------------------------------------

def FILE(file_id, target=None):
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


def GET_FILE(doc):
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


def IS_FILE(value):
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
    if isinstance(value, dict):
        try:
            util.validate_doc(value, mandatory=['type', 'value'])
            if value['type'] == '$file':
                util.validate_doc(
                    value['value'],
                    mandatory=['fileId'],
                    optional=['targetPath']
                )
                return True
        except ValueError:
            pass
    return False
