# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command information for steps in a serial workflow. Each step has an
environment associated with it. The step contains a list of command line
statements that are executed in the specified environment.
"""


"""Labels for object serialization."""
LABEL_COMMANDS = 'commands'
LABEL_ENV = 'environment'


class Step(object):
    """List of command line statements that are executed in a given
    environment. The environment can, for example, specify a Docker image.
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
        """Append a given command line statement to the list of commands in the
        workflow step.

        Parameters
        ----------
        cmd: string
            Command line statement

        Returns
        -------
        flowserv.model.template.step.Step
        """
        self.commands.append(cmd)
        return self
