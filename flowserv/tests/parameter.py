# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes to instantiate template parameters."""

from flowserv.model.parameter.base import TemplateParameter

import flowserv.model.parameter.declaration as pd


def StringParameter(identifier):
    """Get an template parameter instance with the given identifier. The
    parameter type is DT_STRING.

    Parameters
    ----------
    identifier: string
        Unique parameter identifier

    Returns
    -------
    flowserv.model.parameter.base.TemplateParameter
    """
    return TemplateParameter(obj=pd.parameter_declaration(
        identifier=identifier,
        data_type=pd.DT_STRING
    ))
