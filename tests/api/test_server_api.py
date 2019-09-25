# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test API methods for the servie descriptor."""

import os

from robapi.service.server import Service

import robcore.config.api as config
import robapi.serialize.hateoas as hateoas
import robapi.serialize.labels as labels
import robcore.tests.serialize as serialize
import robapi.version as version
import robcore.util as util


RELS = [
    hateoas.SELF,
    hateoas.LOGIN,
    hateoas.LOGOUT,
    hateoas.REGISTER,
    hateoas.BENCHMARKS
]


class TestServiceDescriptor(object):
    """Unit test for the service descriptor."""
    def test_descriptor(self):
        """Test the service descriptor serialization."""
        # Clear environment variable if set
        if config.ROB_API_NAME in os.environ:
            del os.environ[config.ROB_API_NAME]
        r = Service().service_descriptor()
        util.validate_doc(
            doc=r,
            mandatory_labels=[labels.NAME, labels.LINKS, labels.VERSION]
        )
        serialize.validate_links(r, RELS)
        assert r[labels.NAME] == config.DEFAULT_NAME
        assert r[labels.VERSION] == version.__version__
