# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""A workflow template contains a workflow specification and an optional list
of template parameters. The workflow specification may contain references to
template parameters.

Template parameters are common to different backends that excute a workflow.
The syntax and structure of the workflow specification is engine specific.
The only common part here is that template parameters are referenced using
the $[[..]] syntax from within the specifications.

The template parameters can be used to render front-end forms that gather user
input for each parameter. Given an association of parameter identifiers to
user-provided values, the workflow backend is expected to be able to execute
the modified workflow specification in which references to template parameters
have been replaced by parameter values.
"""

from flowserv.model.parameter.base import ParameterGroup
from flowserv.model.template.schema import ResultSchema

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.parameter.base as pb
import flowserv.model.template.parameter as tp


"""Top-level elements of dictionary serialization for template handles."""
LABEL_MODULES = 'modules'
LABEL_PARAMETERS = 'parameters'
LABEL_POSTPROCESSING = 'postproc'
LABEL_RESULTS = 'results'
LABEL_WORKFLOW = 'workflow'
"""Labels for post-processing workflows."""
PPLBL_FILES = 'files'
PPLBL_INPUTS = 'inputs'
PPLBL_OUTPUTS = 'outputs'
PPLBL_RUNS = 'runs'
PPLBL_WORKFLOW = 'workflow'


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
        self, workflow_spec, sourcedir, parameters=None, modules=None,
        postproc_spec=None, result_schema=None
    ):
        """Initialize the components of the workflow template. A ValueError is
        raised if the identifiers for template parameters are not unique.

        Parameters
        ----------
        workflow_spec: dict
            Workflow specification object
        sourcedir: string
            Path to the base directory that contains the static workflow files
        parameters: list or dict(flowserv.model.parameter.base.TemplateParameter), optional
            Dictionary of workflow template parameter declarations keyed by
            their unique identifier.
        modules: list(flowserv.module.parameter.base.ParameterGroup), optional
            List of workflow modules that group template parameters
        postproc_spec: dict, optional
            Optional post-processing workflow specification
        result_schema: flowserv.model.template.schema.ResultSchema
            Schema of the result for extended templates that define benchmarks.

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        """
        # Workflow specification. Interpretation of the specification is left
        # to the different implementations of the workflow engine.
        self.workflow_spec = workflow_spec
        # Source directory for static workflow files
        self.sourcedir = sourcedir
        # Ensure that the parameters property is not None.
        if parameters is not None:
            if isinstance(parameters, list):
                self.parameters = dict()
                for para in parameters:
                    # Ensure that the identifier of all parameters are unique
                    p_id = para.identifier
                    if p_id in self.parameters:
                        msg = "parameter '{}' not unique".format(p_id)
                        raise err.InvalidTemplateError(msg)
                    self.parameters[p_id] = para
            else:
                self.parameters = parameters
                for key in self.parameters:
                    p_id = self.parameters[key].identifier
                    if key != p_id:
                        msg = "invalid key '{}' for '{}'".format(key, p_id)
                        raise err.InvalidTemplateError(msg)
        else:
            self.parameters = dict()
        # Optional components (may be None)
        self.modules = modules
        self.postproc_spec = postproc_spec
        self.result_schema = result_schema

    @classmethod
    def from_dict(cls, doc, sourcedir, validate=True):
        """Create an instance of the workflow template for a dictionary
        serialization. The structure of the dictionary is expected to be the
        same as generated by the to_dict() method of this class. The only
        mandatory element in the dictionary is the workflow specification.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of a workflow template
        sourcedir: string
            Path to the base directory that contains the static workflow files
        validate: bool, optional
            Validate template parameter declarations against the parameter
            schema if this flag is True.

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.UnknownParameterError
        """
        # Ensure that the mandatory elements are present. At this point, only
        # the workflow specification is mandatory.
        if LABEL_WORKFLOW not in doc:
            msg = "missing element '{}'".format(LABEL_WORKFLOW)
            raise err.InvalidTemplateError(msg)
        # -- Workflow specification -------------------------------------------
        workflow_spec = doc[LABEL_WORKFLOW]
        # -- Parameter declarations -------------------------------------------
        # Add given parameter declarations to the parameter list. Ensure that
        # all default values are set
        if LABEL_PARAMETERS in doc:
            parameters = pb.create_parameter_index(
                doc[LABEL_PARAMETERS],
                validate=validate
            )
        else:
            parameters = dict()
        # Ensure that the workflow specification does not reference undefined
        # parameters if validate flag is True.
        if validate:
            for key in tp.get_parameter_references(workflow_spec):
                if key not in parameters:
                    raise err.UnknownParameterError(key)
        # -- Post-processing task ---------------------------------------------
        postproc_spec = None
        if LABEL_POSTPROCESSING in doc:
            postproc_spec = doc[LABEL_POSTPROCESSING]
            if validate:
                util.validate_doc(
                    doc=postproc_spec,
                    mandatory=[PPLBL_WORKFLOW],
                    optional=[PPLBL_INPUTS, PPLBL_OUTPUTS]
                )
                if PPLBL_INPUTS in postproc_spec:
                    inputs = postproc_spec[PPLBL_INPUTS]
                    util.validate_doc(
                        doc=inputs,
                        mandatory=[PPLBL_FILES],
                        optional=[PPLBL_RUNS]
                    )
        # -- Parameter module information -------------------------------------
        modules = None
        if LABEL_MODULES in doc:
            modules = list()
            for m in doc[LABEL_MODULES]:
                modules.append(ParameterGroup.from_dict(m))
        # -- Result schema ---------------------------------------------------
        try:
            schema = ResultSchema.from_dict(doc.get(LABEL_RESULTS))
        except ValueError as ex:
            raise err.InvalidTemplateError(str(ex))
        # Return template instance
        return cls(
            workflow_spec=workflow_spec,
            postproc_spec=postproc_spec,
            sourcedir=sourcedir,
            parameters=parameters,
            result_schema=schema,
            modules=modules
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
        flowserv.model.parameter.base.TemplateParameter
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
        return self.result_schema is not None

    def list_parameters(self):
        """Get a sorted list of parameter declarations. Elements are sorted by
        their index value. Ties are broken using the unique parameter
        identifier.

        Returns
        -------
        list(flowserv.model.parameter.base.TemplateParameter)
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
        doc = {LABEL_WORKFLOW: self.workflow_spec}
        # Add optional elements if present
        if len(self.parameters) > 0:
            doc[LABEL_PARAMETERS] = [
                p.to_dict() for p in self.parameters.values()
            ]
        if self.postproc_spec is not None:
            doc[LABEL_POSTPROCESSING] = self.postproc_spec
        if self.modules is not None:
            doc[LABEL_MODULES] = [m.to_dict() for m in self.modules]
        if self.result_schema is not None:
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
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Raises
        ------
        flowserv.core.error.MissingArgumentError
        """
        for para in self.parameters.values():
            if para.is_required and para.default_value is None:
                if para.identifier not in arguments:
                    raise err.MissingArgumentError(para.identifier)
