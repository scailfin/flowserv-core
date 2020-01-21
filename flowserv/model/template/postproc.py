# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command information for steps in a sequential workflow. Each step has an
environment associated with it. The step contains a list of command line
statements that are executed in the specified environment.
"""

from flowserv.model.template.step import Step

import flowserv.core.error as err


"""Labels for object serialization."""
LABEL_INPUTS = 'inputs'
LABEL_MOUNTS = 'mount'
LABEL_OUTPUTS = 'outputs'


class PostProcessingStep(Step):
    """Step in a sequence of post-processing actions for workflows. Each step
    is executed within a given environment, e.g. a Docker container image.

    This class extends the serial workflow step with information about the
    volumes (directories) that are mounted into the container, the required
    input files as well as the generated output files.
    """
    def __init__(self, env, commands=None, mounts=None, inputs=None, outputs=None):
        """Initialize the object properties.

        Parameters
        ----------
        env: string
            Execution environment name
        commands: list(string), optional
            List of command line statements
        mounts: list(string), optional
            List of directories in the workflow run folder that are made
            available inside the execution environment (e.g., maounted as
            volumes into a Docker container)
        inputs: list(string), optional
            List of resource names for workflow resources that are generated by
            successful workflow runs and provided to the post-processing step
            as inputs
        outputs: list(string), optional
            Names of resources that are generated as outputs by the
            post-processing step
        """
        super(PostProcessingStep, self).__init__(env=env, commands=commands)
        self.mounts = mounts if mounts is not None else list()
        self.inputs = inputs if inputs is not None else list()
        self.outputs = outputs if outputs is not None else list()

    @staticmethod
    def from_dict(doc):
        """Create class instance from given dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for post-processing step

        Returns
        -------
        flowserv.model.template.step.PostProcessingStep
        """
        step = Step.from_dict(doc)
        return PostProcessingStep(
            env=step.env,
            commands=step.commands,
            mounts=doc.get(LABEL_MOUNTS),
            inputs=doc.get(LABEL_INPUTS),
            outputs=doc.get(LABEL_OUTPUTS)
        )

    def to_dict(self):
        """Get dictionary serialization for the post-processing step.

        Returns
        -------
        dict
        """
        doc = super(PostProcessingStep, self).to_dict()
        doc[LABEL_MOUNTS] = self.mounts
        doc[LABEL_INPUTS] = self.inputs
        doc[LABEL_OUTPUTS] = self.outputs
        return doc
