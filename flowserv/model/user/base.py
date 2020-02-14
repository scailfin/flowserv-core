# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Handle for registered users."""


class UserHandle(object):
    """Each user that registers with the application has a unique identifier
    and a unique user name associated with them. Additional information about
    the user may be maintained by different user registration modules.

    For users that are logged into the system the user handle contains the API
    key that was assigned during login..
    """
    def __init__(self, identifier, name, api_key=None):
        """Initialize the user properties.

        Parameters
        ----------
        identifier: string
            Unique user identifier
        name: string
            User-provided name
        """
        self.identifier = identifier
        self.name = name
        self.api_key = api_key

    def is_logged_in(self):
        """Test if the user API key is set as an indicator of whether the user
        is currently logged in or not.

        Returns
        -------
        bool
        """
        return self.api_key is not None
