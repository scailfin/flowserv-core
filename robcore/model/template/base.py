# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""A workflow template contains a workflow specification and an optional list
of template parameters. The workflow specification may contain references to
template parameters.

Template parameters are common to different backends that excute a workflow. The
syntax and structure of the workflow specification is engine specific. The only
common part here is that template parameters are referenced using the $[[..]]
syntax from within the specifications.

The template parameters can be used to render front-end forms that gather user
input for each parameter. Given an association of parameter identifiers to
user-provided values, the workflow backend is expected to be able to execute
the modified workflow specification in which references to template parameters
have been replaced by parameter values.
"""

from robcore.model.template.schema import ResultSchema

import robcore.error as err
import robcore.util as util
import robcore.model.template.parameter.util as wfputil
import robcore.model.template.util as tmplutil


"""Top-level elements of dictionary serialization for template handles."""
LABEL_ID = 'id'
LABEL_PARAMETERS = 'parameters'
LABEL_RESULTS = 'results'
LABEL_WORKFLOW = 'workflow'


class WorkflowModuleHandle(object):
    """Handle for specifications of workflow modules that are used to group
    workflow parameters.
    """
    def __init__(self, identifier, name, index):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique module identifier
        name: string
            Human-readable module name
        index: int
            Module sort order index
        """
        self.identifier = identifier
        self.name = name
        self.index = index


class WorkflowTemplate(object):
    """Workflow templates are parameterized workflow specifications. Each
    template has a unique identifier.

    The template contains the workflow specification. The syntax and
    structure of the specification is not further inspected.

    The template for a parameterized workflow contains a dictionary of template
    parameter declarations. Parameter declarations are keyed by their unique
    identifier in the dictionary.

    For benchmark templates, a result schema is defined. This schema is used to
    store the results of different benchmark runs in a database and to the
    generate a benchmark leader board.
    """
    def __init__(
        self, workflow_spec, source_dir, identifier=None, parameters=None,
        result_schema=None, modules=None
    ):
        """Initialize the components of the workflow template. A ValueError is
        raised if the identifier of template parameters are not unique.

        Parameters
        ----------
        workflow_spec: dict
            Workflow specification object
        source_dir: string
            Path to the base directory that contains the static workflow files
        identifier: string, optional
            Unique template identifier. If no value is given a UUID will be
            assigned.
        parameters: dict(string:robcore.model.template.parameter.base.TemplateParameter), optional
            Dictionary of workflow template parameter declarations keyed by
            their unique identifier.
        result_schema: robcore.model.template.schema.ResultSchema
            Schema of the result for extended templates that define benchmarks.
        modules: list(robcore.module.template.base.WorkflowModuleHandle), optional
            List of workflow modules that group template parameters

        Raises
        ------
        robcore.error.InvalidTemplateError
        """
        # Set the unique identifier. If no identifier is given an new one is
        # created.
        if not identifier is None:
            self.identifier = identifier
        else:
            self.identifier = util.get_unique_identifier()
        # Workflow specification. Interpretation of the specification is left
        # to the different implementations of the workflow engine.
        self.workflow_spec = workflow_spec
        # Source directory for static workflow files
        self.source_dir = source_dir
        # Ensure that the parameters property is not None.
        if not parameters is None:
            if isinstance(parameters, list):
                self.parameters = dict()
                for para in parameters:
                    # Ensure that the identifier of all parameters are unique
                    p_id = para.identifier
                    if p_id in self.parameters:
                        msg = 'parameter \'{}\' not unique'.format(p_id)
                        raise err.InvalidTemplateError(msg)
                    self.parameters[p_id] = para
            else:
                self.parameters = parameters
                for key in self.parameters:
                    p_id = self.parameters[key].identifier
                    if key != p_id:
                        msg ='invalid key \'{}\' for \'{}\''.format(key, p_id)
                        raise err.InvalidTemplateError(msg)
        else:
            self.parameters = dict()
        # Schema declaration for benchmark results. The schema may be None.
        self.result_schema = result_schema
        self.modules = modules

    @staticmethod
    def from_dict(doc, source_dir, identifier=None, validate=True):
        """Create an instance of the workflow template for a dictionary
        serialization. The structure of the dictionary is expected to be the
        same as generated by the to_dict() method of this class. The only
        mandatory element in the dictionary is the workflow specification.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of a workflow template
        source_dir: string
            Path to the base directory that contains the static workflow files
        identifier: string, optional
            Unique template identifier. This value will override the value in
            the document.
        validate: bool, optional
            Validate template parameter declarations against the parameter
            schema if this flag is True.

        Returns
        -------
        robcore.model.template.base.WorkflowTemplate

        Raises
        ------
        robcore.error.InvalidTemplateError
        robcore.error.UnknownParameterError
        """
        # Ensure that the mandatory elements are present. At this point, only
        # the workflow specification is mandatory.
        if not LABEL_WORKFLOW in doc:
            msg = 'missing element \'{}\''.format(LABEL_WORKFLOW)
            raise err.InvalidTemplateError(msg)
        # Get identifier if present in document
        if LABEL_ID in doc:
            identifier = doc[LABEL_ID]
        # Workflow specification
        workflow_spec = doc[LABEL_WORKFLOW]
        # Add given parameter declarations to the parameter list. Ensure that
        # all default values are set
        if LABEL_PARAMETERS in doc:
            parameters = wfputil.create_parameter_index(
                doc[LABEL_PARAMETERS],
                validate=validate
            )
        else:
            parameters = dict()
        # Ensure that the workflow specification does not reference undefined
        # parameters if validate flag is True.
        if validate:
            for key in tmplutil.get_parameter_references(workflow_spec):
                if not key in parameters:
                    raise err.UnknownParameterError(key)
        # Get schema object from serialization if present
        schema = None
        if LABEL_RESULTS in doc:
            try:
                schema = ResultSchema.from_dict(doc[LABEL_RESULTS])
            except ValueError as ex:
                raise err.InvalidTemplateError(str(ex))
        # Return template instance
        return WorkflowTemplate(
            identifier=identifier,
            workflow_spec=workflow_spec,
            source_dir=source_dir,
            parameters=parameters,
            result_schema=schema
        )

    def get_parameter(self, identifier):
        """Short-cut to access the declaration for a parameter with the given
        identifier.

        Parameters
        ----------
        identifier: string
            Unique parameter declaration identifier

        Returns
        -------
        robcore.model.template.parameter.base.TemplateParameter
        """
        return self.parameters.get(identifier)

    def get_schema(self):
        """Short-cut to access the result schema specification.

        Returns
        -------
        bool
        """
        return self.result_schema

    def has_schema(self):
        """Test if the result schema is set.

        Returns
        -------
        bool
        """
        return not self.result_schema is None

    def list_parameters(self):
        """Get a sorted list of parameter declarations. Elements are sorted by
        their index value. Ties are broken using the unique parameter
        identifier.

        Returns
        -------
        list(robcore.model.template.parameter.base.TemplateParameter)
        """
        return sorted(
            self.parameters.values(),
            key=lambda p: (p.index, p.identifier)
        )

    def to_dict(self):
        """Get dictionary serialization for the workflow template.

        Returns
        -------
        dict
        """
        # The workflow specificatiom is the only mandatory element
        doc = {LABEL_ID:  self.identifier, LABEL_WORKFLOW: self.workflow_spec}
        # Add optional elements if present
        if len(self.parameters) > 0:
            doc[LABEL_PARAMETERS] = [
                p.to_dict() for p in self.parameters.values()
            ]
        if not self.result_schema is None:
            doc[LABEL_RESULTS] = self.result_schema.to_dict()
        # Return the template serialization
        return doc

    def validate_arguments(self, arguments):
        """Ensure that the workflow can be instantiated using the given set of
        arguments. Raises an error if there are template parameters for which
        the argument set does not provide a values and that do not have a
        default value defined.

        Parameters
        ----------
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Raises
        ------
        robcore.error.MissingArgumentError
        """
        for para in self.parameters.values():
            if para.is_required and para.default_value is None:
                if not para.identifier in arguments:
                    raise err.MissingArgumentError(para.identifier)
