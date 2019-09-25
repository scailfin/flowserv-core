# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to test object serialization."""

import robapi.serialize.hateoas as hateoas
import robapi.serialize.labels as labels
import robcore.util as util


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
    # If the document contains the links key we assume that the user wants to
    # validate that element
    if labels.LINKS in doc:
        doc = doc[labels.LINKS]
    util.validate_doc(
        doc=hateoas.deserialize(doc),
        mandatory_labels=keys
    )
