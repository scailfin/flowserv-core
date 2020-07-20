# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Define the workflow backend as a global variable. This is necessary for the
multi-porcess backend to be able to maintain process state in between API
requests.
"""

from flowserv.controller.init import init_backend


backend = init_backend()
