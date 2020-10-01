# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
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
from flowserv.model.template.files import WorkflowOutputFile
from flowserv.model.template.parameter import ParameterIndex
from flowserv.model.template.schema import ResultSchema

import flowserv.error as err
import flowserv.util as util
import flowserv.model.template.parameter as tp


class WorkflowTemplate(object):
    """Workflow templates are parameterized workflow specifications. The
    template contains the workflow specification, template parameters, and
    information about benchmark results and post-processing steps.

    The syntax and structure of the workflow specification(s) is not further
    inspected. It is dependent on the workflow controller that is used to
    execute workflow runs.

    The template for a parameterized workflow contains a dictionary of template
    parameter declarations. Parameter declarations are keyed by their unique
    identifier in the dictionary.

    For benchmark templates, a result schema is defined. This schema is used to
    store the results of different benchmark runs in a database and to the
    generate a benchmark leader board.
    """
    def __init__(
        self, workflow_spec, parameters, modules=None, outputs=None,
        postproc_spec=None, result_schema=None
    ):
        """Initialize the components of the workflow template.

        Parameters
        ----------
        workflow_spec: dict
            Workflow specification object
        parameters: flowserv.model.template.parameter.ParameterIndex
            Dictionary of workflow template parameter declarations keyed by
            their unique identifier.
        modules: list(flowserv.model.parameter.base.ParameterGroup),
                default=None
            List of workflow modules that group template parameters
        outputs: list(flowserv.model.template.files.WorkflowOutputFile),
                default=None
            List of specifications for workflow output files.
        postproc_spec: dict, default=None
            Optional post-processing workflow specification
        result_schema: flowserv.model.template.schema.ResultSchema,
                default=None
            Schema of the result for extended templates that define benchmarks.
        """
        self.workflow_spec = workflow_spec
        self.parameters = parameters
        # Optional components (may be None)
        self.modules = modules
        self.outputs = outputs
        self.postproc_spec = postproc_spec
        self.result_schema = result_schema

    @classmethod
    def from_dict(cls, doc, validate=True):
        """Create an instance of the workflow template for a dictionary
        serialization. The structure of the dictionary is expected to be the
        same as generated by the to_dict() method of this class. The only
        mandatory element in the dictionary is the workflow specification.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of a workflow template
        validate: bool, optional
            Validate template parameter declarations against the parameter
            schema if this flag is True.

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.error.InvalidTemplateError
        flowserv.error.UnknownParameterError
        """
        # Ensure that the mandatory elements are present. At this point, only
        # the workflow specification is mandatory.
        if validate:
            if 'workflow' not in doc:
                msg = "missing element '{}'".format('workflow')
                raise err.InvalidTemplateError(msg)
        # -- Workflow specification -------------------------------------------
        workflow_spec = doc['workflow']
        # -- Parameter declarations -------------------------------------------
        # Add given parameter declarations to the parameter list. Ensure that
        # all default values are set
        parameters = ParameterIndex.from_dict(
            doc.get('parameters', dict()),
            validate=validate
        )
        # Ensure that the workflow specification does not reference
        # undefined parameters if validate flag is True.
        if validate:
            for key in tp.get_parameter_references(workflow_spec):
                if key not in parameters:
                    raise err.UnknownParameterError(key)
        # -- Post-processing task ---------------------------------------------
        postproc_spec = None
        if 'postproc' in doc:
            postproc_spec = doc['postproc']
            if validate:
                util.validate_doc(
                    doc=postproc_spec,
                    mandatory=['workflow'],
                    optional=['inputs', 'outputs']
                )
                util.validate_doc(
                    doc=postproc_spec.get('inputs', {'files': ''}),
                    mandatory=['files'],
                    optional=['runs']
                )
        # -- Parameter module information -------------------------------------
        modules = None
        if 'modules' in doc:
            modules = list()
            for m in doc['modules']:
                modules.append(ParameterGroup.from_dict(m, validate=validate))
        # -- Output file specifications --------------------------------------
        outputs = None
        if 'outputs' in doc:
            outputs = [WorkflowOutputFile.from_dict(
                f,
                validate=validate
            ) for f in doc['outputs']]
        # -- Result schema ---------------------------------------------------
        schema = ResultSchema.from_dict(doc.get('results'), validate=validate)
        # Return template instance
        return cls(
            workflow_spec=workflow_spec,
            postproc_spec=postproc_spec,
            parameters=parameters,
            result_schema=schema,
            modules=modules,
            outputs=outputs
        )

    def to_dict(self):
        """Get dictionary serialization for the workflow template.

        Returns
        -------
        dict
        """
        # The workflow specificatiom is the only mandatory element.
        doc = {'workflow': self.workflow_spec}
        # Add optional elements if present
        doc['parameters'] = self.parameters.to_dict()
        if self.postproc_spec is not None:
            doc['postproc'] = self.postproc_spec
        if self.modules is not None:
            doc['modules'] = [m.to_dict() for m in self.modules]
        if self.result_schema is not None:
            doc['results'] = self.result_schema.to_dict()
        # Return the template serialization
        return doc

    def validate_arguments(self, arguments):
        """Ensure that the workflow can be instantiated using the given set of
        arguments. Raises an error if there are template parameters for which
        the argument set does not provide a value and that do not have a
        default value.

        Parameters
        ----------
        arguments: dict
            Dictionary of argument values for parameters in the template

        Raises
        ------
        flowserv.error.MissingArgumentError
        """
        for para in self.parameters.values():
            if para.is_required and para.default_value is None:
                if para.para_id not in arguments:
                    raise err.MissingArgumentError(para.para_id)
