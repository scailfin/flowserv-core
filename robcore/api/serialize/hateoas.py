# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Constants and methods to serialize 'Hypermedia As The Engine Of Application
State' (HATEOAS) references that are included in API responses.
"""

import robapi.serialize.labels as labels


"""Definition of common HATEOAS link relationship types."""
ACTIVATE = 'activate'
ADD = 'add'
BENCHMARKS = 'benchmarks'
BENCHMARK = 'benchmark'
CANCEL = 'cancel'
CREATE = 'create'
DELETE = 'delete'
DOWNLOAD = 'download'
JOIN = 'join'
LEADERBOARD = 'leaderboard'
LEAVE = 'leave'
LIST = 'list'
LOGIN = 'login'
LOGOUT = 'logout'
REGISTER = 'register'
SELF = 'self'
SERVICE = 'service'
SUBMISSION = 'submission'
SUBMIT = 'submit'
TEAMS = 'teams'
UPDATE = 'update'
UPLOAD = 'upload'
WHOAMI = 'whoami'


# ------------------------------------------------------------------------------
# Reference categories
# ------------------------------------------------------------------------------
def action(rel):
    """Add relationship category prefix for references that transform a web
    reource.

    Parameters
    ----------
    rel: string
        Link relationship identifier

    Returns
    -------
    string
    """
    return 'self:{}'.format(rel)


# ------------------------------------------------------------------------------
# Helper methods for serialization
# ------------------------------------------------------------------------------
def deserialize(links):
    """Deserialize a list of HATEOAS reference objects into a dictionary.

    Parameters
    ----------
    links: list(dict)
        List of HATEOAS references in default serialization format

    Returns
    -------
    dict
    """
    result = dict()
    for link in links:
        result[link[labels.REL]] = link[labels.REF]
    return result


def serialize(links):
    """Serialize a given set of HATEOAS references. Each reference is an entry
    in the given dictionary. The key defines the HATEOAS relationship type for
    the link and the assiciated value is the link target Url.

    Parameters
    ----------
    links: dict()
        Dictionary of link relationship and link target entries

    Returns
    -------
    dict
    """
    return [{labels.REL: key, labels.REF: links[key]} for key in links]
