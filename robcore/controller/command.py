# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow commands. Each command has an environment associated with it and
it contains a list of command line statements that are executed in the
specified environment.
"""

class Command(object):
    """List of commands that are executed in a given environment. The
    environment identifies for example a Docker image.
    """
    def __init__(self, env, commands=None):
        """Initialize the object properties.

        Parameters
        ----------
        env: string
            Execution environment name
        commands: list(string), optional
            List of command line statements
        """
        self.env = env
        self.commands = commands if commands is not None else list()

    def add(self, cmd):
        """Add a given command line statement to the list of commands.

        Parameters
        ----------
        cmd: string
            Command line statement

        Returns
        -------
        robcore.controller.command.Command
        """
        self.commands.append(cmd)
        return self
