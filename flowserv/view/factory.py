# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory pattern for resource serializers. This module defines the abstract
view factory class as well as the default implementation for the flowServ
view.
"""

from abc import ABCMeta, abstractmethod

from flowserv.view.files import UploadFileSerializer
from flowserv.view.group import WorkflowGroupSerializer
from flowserv.view.run import RunSerializer
from flowserv.view.server import ServiceSerializer
from flowserv.view.user import UserSerializer
from flowserv.view.workflow import WorkflowSerializer


# -- Abstract class -----------------------------------------------------------

class ViewFactory(metaclass=ABCMeta):
    """Abstract factory class for serializers of the different API components.
    """
    @abstractmethod
    def files(self):  # pragma: no cover
        """Serializer for uploaded files.

        Returns
        -------
        flowserv.view.files.UploadFileSerializer
        """
        raise NotImplementedError()

    @abstractmethod
    def groups(self):  # pragma: no cover
        """Serializer for workflow groups.

        Returns
        -------
        flowserv.view.group.WorkflowGroupSerializer
        """
        raise NotImplementedError()

    @abstractmethod
    def runs(self):  # pragma: no cover
        """Serializer for workflow runs.

        Returns
        -------
        flowserv.view.run.RunSerializer
        """
        raise NotImplementedError()

    @abstractmethod
    def server(self):  # pragma: no cover
        """Serializer for the service descriptor.

        Returns
        -------
        flowserv.view.server.ServiceSerializer
        """
        raise NotImplementedError()

    @abstractmethod
    def users(self):  # pragma: no cover
        """Serializer for users.

        Returns
        -------
        flowserv.view.user.UserSerializer
        """
        raise NotImplementedError()

    @abstractmethod
    def workflows(self):  # pragma: no cover
        """Serializer for workflow templates.

        Returns
        -------
        flowserv.view.workflow.WorkflowSerializer
        """
        raise NotImplementedError()


# -- Default implementation ---------------------------------------------------

class DefaultView(ViewFactory):
    """Default implementatin for the serializers factory."""
    def __init__(self, labels=None):
        """Initialize the definition of labels that override the default labels
        for different components. Expects a dictionary of dictionaries with the
        following top-level elements:
            - FILES
            - GROUPS
            - RUNS
            - SERVER
            - USERS
            - WORKFLOWS
        """
        self.labels = labels if labels is not None else dict()

    def files(self):
        """Serializer for uploaded files.

        Returns
        -------
        flowserv.view.files.UploadFileSerializer
        """
        return UploadFileSerializer(labels=self.labels.get('FILES'))

    def groups(self):
        """Serializer for workflow groups.

        Returns
        -------
        flowserv.view.group.WorkflowGroupSerializer
        """
        return WorkflowGroupSerializer(
            files=self.files(),
            labels=self.labels.get('GROUPS')
        )

    def runs(self):
        """Serializer for workflow runs.

        Returns
        -------
        flowserv.view.run.RunSerializer
        """
        return RunSerializer(labels=self.labels.get('RUNS'))

    def server(self):
        """Serializer for the service descriptor.

        Returns
        -------
        flowserv.view.server.ServiceSerializer
        """
        return ServiceSerializer(labels=self.labels.get('SERVER'))

    def users(self):
        """Serializer for users.

        Returns
        -------
        flowserv.view.user.UserSerializer
        """
        return UserSerializer(labels=self.labels.get('USERS'))

    def workflows(self):
        """Serializer for workflow templates.

        Returns
        -------
        flowserv.view.workflow.WorkflowSerializer
        """
        return WorkflowSerializer(
            runs=self.runs(),
            labels=self.labels.get('WORKFLOWS')
        )
