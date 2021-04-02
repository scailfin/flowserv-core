# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Validator for engine configuration documents."""

from __future__ import annotations
from jsonschema import Draft7Validator, RefResolver

import importlib.resources as pkg_resources
import json
import os


"""Create schema validator for API requests."""
schemafile = os.path.abspath(os.path.join(__file__, 'config.json'))
schema = json.load(pkg_resources.open_text(__package__, 'config.json'))
resolver = RefResolver(schemafile, schema)
validator = Draft7Validator(schema=schema['definitions']['engineConfig'], resolver=resolver)
