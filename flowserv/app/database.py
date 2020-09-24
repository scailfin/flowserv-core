# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Global variable for the flowapp database instance that is to be (re-)used
by all requests to the web app and API. This database instance is anticipated
to be used in a web server setting. We therefore set the wep_app flag to True
in order to use scoped sessions for web applications.
"""

from flowserv.model.database import DB


"""Global database instance for all requests."""
flowdb = DB(web_app=True)
