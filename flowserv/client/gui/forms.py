# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Methods to render input forms for the different types of input parameters
that can be defined for flowServ workflow templates.
"""

import streamlit as st

from typing import Callable, Dict, List, Tuple

from flowserv.model.parameter.base import Parameter


@st.cache(allow_output_mutation=True)
def enum_options(values: List[Dict]) -> Tuple[List[str], int, Callable]:
    """Enumerate options of a enum parameter for display in a select box.
    Returns a 3-tuple containing a list of option value, the list index of
    the default option, and a function that provides a mapping from option
    values to their names (identifier).

    Parameters
    ----------
    values: list of dict
        List of enumeration values from the parameter declaration.

    Returns
    -------
    (list, int, callable)
    """
    options = list()
    default_index = 0
    mapping = dict()
    for i, obj in enumerate(values):
        identifier = obj['value']
        options.append(identifier)
        mapping[identifier] = obj['name']
        if obj.get('isDefault', False):
            default_index = i

    def mapfunc(value: str) -> str:
        """Mapping for option values to thier identifier."""
        return mapping[value]

    return options, default_index, mapfunc


def show_form(parameters: List[Parameter]) -> Tuple[bool, Dict]:
    """Display input controlls for the different parameters in a workflow
    template. Returns the value for the submit button and a mapping of
    parameter identifier to the values that are returned by the respective
    controlls.

    Parameters
    ----------
    parameters: list of flowserv.model.parameter.base.ParameterBase
        List of parameter declarations in a workflow template.

    Returns
    -------
    (bool, dict)
    """
    # Collect return values for the rendered controlls for each of the
    # parameters. This is a mapping from parameter identifier to the value
    # that was provided by the user via the rendered input form element.
    arguments = dict()
    # flowServ currently distinguishes five main types of parameters: bool,
    # enumeration, file, numeric and text.
    for para in parameters:
        if para.is_actor():
            # Render a checkbox for Boolean parameters.
            atype, aconfig = para.default if para.default else ('container', dict())
            if atype == 'container':
                image = st.text_input(para.label + ' (Docker Image)', aconfig.get('image', ''))
                commands = st.text_area(
                    para.label + ' (Commands)',
                    '\n'.join(aconfig.get('commands', [])).strip()
                )
                val = ('container', {'image': image, 'commands': commands.split('\n')})
            else:
                raise ValueError("invalid actor type '{}'".format(atype))
        elif para.is_bool():
            # Render a checkbox for Boolean parameters.
            checked = para.default if para.default else False
            val = st.checkbox(label=para.label, value=checked)
        elif para.is_select():
            # Render a selct box for all the options in an enumeration
            # parameter.
            options, index, mapfunc = enum_options(para.values)
            val = st.selectbox(
                label=para.label,
                options=options,
                index=index,
                format_func=mapfunc
            )
        elif para.is_file():
            # Render a file uploader for input files.
            val = st.file_uploader(label=para.label)
        elif para.is_numeric():
            # For numeric parameters we either render a text box or a slider if
            # the parameter has a range constraint.
            constraint = para.constraint
            default_value = para.default
            if constraint is not None and constraint.is_closed():
                if default_value is None:
                    default_value = constraint.min_value()
                val = st.slider(
                    label=para.label,
                    min_value=constraint.min_value(),
                    max_value=constraint.max_value(),
                    value=default_value
                )
            else:
                val = st.text_input(para.label, para.default)
        else:
            # Render a text box for all other parameter types.
            val = st.text_input(para.label, para.default)
        arguments[para.name] = val
    submit = st.button('Run')
    return submit, arguments
