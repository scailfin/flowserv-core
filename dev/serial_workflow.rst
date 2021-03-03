============================
Serial Workflows in flowServ
============================

`Serial workflows <https://github.com/scailfin/flowserv-core/blob/47d383f88669af0ab79c6af61d0d48c8cb7448a9/flowserv/controller/serial/workflow/base.py>`_ in **flowServ** are sequences of workflow steps. Workflow steps are classes that inherit from the base class `WorkflowStep <https://github.com/scailfin/flowserv-core/blob/47d383f88669af0ab79c6af61d0d48c8cb7448a9/flowserv/model/workflow/step.py>`_.

Sub-classes for `WorkflowStep` capture all the information that is required to execute the workflow step. **flowServ** currently supports two types of workflow steps:

- `ContainerStep <https://github.com/scailfin/flowserv-core/blob/47d383f88669af0ab79c6af61d0d48c8cb7448a9/flowserv/model/workflow/step.py#L83>`_: Execute a list of commands in a Docker container-like environment. THe class maintains the identifier of the container image and the list of commands that are executed.
- `FunctionStep <https://github.com/scailfin/flowserv-core/blob/47d383f88669af0ab79c6af61d0d48c8cb7448a9/flowserv/model/workflow/step.py#L127>`_: Execute a given Python function directly in the current Python environment (thread). Maintains the function and mappings of input arguments and outputs for that function.

When the workflow is run the individual steps are executed in order. Workflow steps are executed by dedicated `Workers <https://github.com/scailfin/flowserv-core/blob/master/flowserv/controller/worker/base.py>`_. For different step type different workers are used to e execute the steps.

The input to a workflow run is a dictionary of serialized user-arguments. These arguments form the *context* for the workflow. The context is passed on to each worker as it executes a workflow step. Execution of workflow steps may modify the context, e.g., to add new elements that are generated as the result of executing the workflow step.

In order to add a new workflow step type the following is necessary:

- Implement a sub-class of `flowserv.model.workflow.step.WorkflowStep` to capture all the information that is necessary to run workflow steps of the new type. Make sure to add a `is_mytype_step()` method to the `WorkflowStep` base class to be able to distinguish the different workflow step types.

- Implement a sub-class of `flowserv.controller.worker.base.Worker` that is capable of executing steps of the new type in a given workflow context.
- Modify the `flowserv.controller.serial.engine.runner.exec_workflow` function to account for the new step type.
