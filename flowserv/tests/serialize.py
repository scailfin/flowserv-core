# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to test object serialization."""

import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels
import flowserv.core.util as util


def validate_links(doc, keys):
    """Ensure that the given list of HATEOAS references contains the mandatory
    relationship elements.

    Parameters
    ----------
    doc: dict
        Dictionary serialization of a HATEOAS reference listing
    keys: list(string)
        List of mandatory relationship keys in the reference set
    """
    # We assume that the given document contains the links key
    doc = doc[labels.LINKS]
    util.validate_doc(
        doc=hateoas.deserialize(doc),
        mandatory_labels=keys
    )
