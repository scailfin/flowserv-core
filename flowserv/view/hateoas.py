# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Constants and methods to serialize 'Hypermedia As The Engine Of Application
State' (HATEOAS) references that are included in API responses.
"""

import flowserv.view.labels as labels


"""Definition of common HATEOAS link relationship types."""
ACTIVATE = 'activate'
ADD = 'add'
BENCHMARKS = 'benchmarks'
BENCHMARK = 'benchmark'
CANCEL = 'cancel'
CREATE = 'create'
DELETE = 'delete'
DOWNLOAD = 'download'
GROUP = 'group'
GROUPS = 'groupss'
JOIN = 'join'
LEADERBOARD = 'leaderboard'
LEAVE = 'leave'
LIST = 'list'
LOGIN = 'login'
LOGOUT = 'logout'
REGISTER = 'register'
RESOURCES = 'resources'
RESULTS = 'results'
SELF = 'self'
SERVICE = 'service'
SUBMIT = 'submit'
TEAMS = 'teams'
UPDATE = 'update'
UPLOAD = 'upload'
WHOAMI = 'whoami'


# ------------------------------------------------------------------------------
# Reference categories
# ------------------------------------------------------------------------------
def action(rel, resource='self'):
    """Add relationship category prefix for references that transform a web
    reource. If the resource identifier is not given 'self' is assumed.

    Parameters
    ----------
    rel: string
        Link relationship identifier
    resource: string, optional
        Identifier (or type) of the resource that is being modified.

    Returns
    -------
    string
    """
    return '{}:{}'.format(resource, rel)


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
