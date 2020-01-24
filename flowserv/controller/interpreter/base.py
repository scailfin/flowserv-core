# This file is part of Flowserv.
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.


"""Interface for interpreters that handle specific workflow specification
language. This interface defines the base functionality that the workflow
engine requires in order to execute a workflow specified in a specific workflow
language.
"""

from abc import ABCMeta, abstractmethod


class WorkflowTemplateInterpreter(metaclass=ABCMeta):
    """Interface defining the methods that are required by the workflow engine
    to execute a workflow in a specific workflow language. The interpreter is
    expected to be a wrapper around a workflow template.
    """
    def __init__(self, template):
        """Initialize the workflow template.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template handle.
        """
        self.template = template

    @abstractmethod
    def modify_template(self, parameters):
        """Modify the workflow specification in the template by adding a given
        set of parameters to the existing template parameter set.

        Returns the modified workflow specification and the modified parameter
        index. Raises an error if the parameter identifier in the resulting
        parameter index are no longer unique.

        Parameters
        ----------
        parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        dict, dict(flowserv.model.parameter.base.TemplateParameter)

        Raises
        ------
        flowserv.core.error.DuplicateParameterError
        flowserv.core.error.InvalidTemplateError
        """
        raise NotImplementedError()
