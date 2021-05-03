# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

from jsonschema import Draft7Validator, RefResolver

import importlib.resources as pkg_resources
import yaml
import os


"""Create schema validator for API requests."""
# Make sure that the path to the schema file is a valid URI. Otherwise, errors
# occur (at least on MS Windows environments). Changed based on:
# https://github.com/Julian/jsonschema/issues/398#issuecomment-385130094
schemafile = 'file:///{}'.format(os.path.abspath(os.path.join(__file__, 'flowserv.yaml')))
schema = yaml.load(pkg_resources.open_text(__package__, 'flowserv.yaml'), Loader=yaml.FullLoader)
resolver = RefResolver(schemafile, schema)


def validator(key: str) -> Draft7Validator:
    """Get Json schema validator for a specific API resource. The resource is
    identified by its unique key.

    Parameters
    ----------
    key: string
        Unique resource key in the schema definition.

    Returns
    -------
    jsonschema.Draft7Validator
    """
    return Draft7Validator(schema=schema['definitions'][key], resolver=resolver)
