# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of general utility functions."""

import json
import traceback
import uuid

from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Type, Union


def get_unique_identifier() -> str:
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')


def import_obj(import_path: str) -> Union[Callable, Type]:
    """Import an object (function or class) from a given package path.

    Parameters
    ----------
    import_path: string
        Full package target path for the imported object. Assumes that path
        components are separated by '.'.

    Returns
    -------
    callable or class
    """
    pos = import_path.rfind('.')
    module_name = import_path[:pos]
    class_name = import_path[pos + 1:]
    module = import_module(module_name)
    return getattr(module, class_name)


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


def stacktrace(ex) -> List[str]:
    """Get list of strings representing the stack trace for a given exception.

    Parameters
    ----------
    ex: Exception
        Exception that was raised by flowServ code

    Returns
    -------
    list of string
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

    Parameters
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
            raise exception("missing element '{}' in:\n{}".format(key, json.dumps(doc, indent=4)))
    # Raise error if additional elements are present in the dictionary
    if optional is not None:
        labels = labels + optional
    for key in doc:
        if key not in labels:
            raise exception("unknown element '{}' in:\n{}".format(key, json.dumps(doc, indent=4)))
    return doc
