# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of workflow resource descriptors."""

import pytest

from flowserv.model.resource import ResourceDescriptor, LABEL_NAME


def test_descriptor_serialization():
    """Unit test for resource descriptor (de-)serialization."""
    r1 = ResourceDescriptor(
        identifier='A',
        name='Descriptor'
    )
    r2 = ResourceDescriptor.from_dict(r1.to_dict())
    assert r2.identifier == 'A'
    assert r2.name == 'Descriptor'
    assert r1.identifier == r2.identifier
    assert r1.name == r2.name
    # Invalid serializations.
    doc1 = r2.to_dict()
    del doc1[LABEL_NAME]
    doc2 = r2.to_dict()
    doc2['notavalidlabel'] = 0
    # No error is raised when deserializing a document with additional labels
    # using the default validate settings
    ResourceDescriptor.from_dict(doc2)
    # Errors are raised by the document validator
    with pytest.raises(ValueError):
        ResourceDescriptor.from_dict(doc1, validate=True)
    with pytest.raises(ValueError):
        ResourceDescriptor.from_dict(doc2, validate=True)
