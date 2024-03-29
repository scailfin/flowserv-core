======================
Serial Workflow Engine
======================

The **serial workflow engine** (``flowserv.controller.serial.engine.base.SerialWorkflowEngine``) is the default implementation of the **flowServ** workflow controller (``flowserv.controller.base.WorkflowEngine``).

**Serial workflows** are sequences of workflow steps that operate on a global workflow state. Each step defines an action that modifies the workflow state. The workflow state is given as (i) a set of files that are available to (and manipulated by) each workflow step, and (ii) a dictionary of workflow parameters and the user-provided parameter values (referred to as the *context*).

The serial workflow engine orchestrates the execution of a serial workflow. The engine maintains a set of workers that are used to execute individual workflow steps (``flowserv.controller.worker.base.Worker``). Each workers is configured for a particular type of workflow step  (see below). The worker has access to a storage volumes (``flowserv.volume.base.StorageVolume``) that provides read and write access to files that the worker needs to access.


Templates for Serial Workflows
==============================

The workflow specification for serial workflows, as part of a **flowServ** workflow template, has three main parts: (i) the specification of input and output files, (ii) the list of template parameters, and (iii) the list of workflow steps. An example specification for a serial workflow is shown below:


.. code-block:: yaml

    workflow:
        files:
            inputs:
            - "code/"
              "$[[names]]"
            outputs:
            - "results/score.json"
              "results/greetings.txt"
        parameters:
        - names: "$[[names]]"
        - greetings: "$[[greetings]]"
        - sleep: "$[[sleep_time]]"
        steps:
        - name: "unique workflow step identifier"
          files:
            inputs:
            - "code/helloworld.py"
            - "${names}"
            outputs:
            - "results/greetings.txt"
          action:
            environment: "python:3.7"
            commands:
            - "${python} code/helloworld.py \
                --input=\"${names}\" \
                --greeting=\"${greetings}\" \
                --sleep=${sleep} \
                --output=results/greetings.txt"


Workflow Input and Output Files
-------------------------------

The optional ``files`` section defines (a) the static input files that are available to a workflow run, and (b) the output files that are generated by a successful workflow run. The input files section defines the static files and directories that are part of the workflow template. These files together with the files that are defined as workflow parameters form the context (*environment*) for the workflow that are available to a workflow at the start of a workflow run. Output files are files that are retained after the workflow run completes successfully. Note that **all file names are relative path names** using ``/`` as the path delimiter.


Template Parameters
-------------------

The optional ``parameters`` section defines the template parameters that are available in the environment for a workflow run. The parameter values are given as a dictionary to the workers for individual workflow steps. The values are for example used to replace variable references in command line statements with values that a user provided (e.g., via a GUI) when initiating the workflow run.


Workflow Steps
--------------

The mandatory ``steps`` section defines the list of individual workflow steps. Each workflow step has an optional ``files`` section. This section is similar to the global files specification for the workflow. It defines the required inputs for the worker that executes the workflow step, as well as the outputs that should be retained and made available to the following workflow steps or that are part of the workflow result. One difference to the global files list is that the files list for a workflow step can contain references to template variables. If the files section is not given, no guarantees are made about the files that are available for a worker. If all workers access the same storage volume then all run files may be available to each worker. However, it is good practice to explicitly specify the inputs and outputs for each workflow step if required by a worker.

The mandatory ``action`` part of the workflow step defines the action of the step. The format is dependent on the type of worker that is used for the step. This part can also be replaced with a user-defined step, i.e., a reference to a template parameter. The serial workflow engine currently supports the following types of workflow steps:

- **Container Step**: A container step defines an action that includes one or more command-line statements that are executed in a specified (container) environment. The specification for a container step has mandatory elements ``environment`` and ``commands``. The environment defines the container image and the commands gives the list of command-line statements. Note that container steps are not necessarily executed in a Docker container. The workers configuration (see below) allows the user to specify the particular type of container worker, e.g., to execute the commands in the Python environment that runs the **flowServ** application using a ``flowserv.controller.worker.subprocess.SubprocessWorker`` that uses Python's ``multiprocess`` module to run the commands.
- **Code Step**: A code step is used to execute a Python function directly in the same environment that is running the **flowServ** API. The function is specified in absolute terms by the package and the function name (e.g., ``my.package.myfunc``). This is referred to as the function import name. The function is imported dynamically when the workflow step is executed. When the function is called it will receive its arguments from the workflow context dictionary. The specification of a code step includes the mandatory ``func`` element that contains the function import name and optional elements ``arg`` and ``variables``. The ``arg`` element specifies the key by which the function result is stored in the workflow context. The ``variables`` element is a list of mappings from function argument names (``arg``) to names of variables (parameters) (``var``) in the workflow context. The mapping is used when generating the input arguments for the function call (in case that the variable names in the function signature do not match the names of variables in the workflow context).
- **Notebook Step**: A notebook step executes code that is given in form of a Jupyter Notebook. The specification for a notebook step contains the mandatory element ``notebook`` that contains the relative path to the Jupyter Notebook (e.g., in the folders for static workflow files), and the optional elements ``output``, ``params`` and ``requirements``. The ``output`` element specifies the (relative) path for the generated output notebook. If not given, the name of the output notebook will be the name of the input notebook with the suffix ``.ipynb`` replaced by ``.out.ipynb``. The ``params`` element lists the parameters from thw workflow context that are being passed to the notebook when running the notebook using ``papermill``. The ``requirements`` element lists the additional Python packages that need to be installed for running the notebook. Note that the latter will only be taken into consideration when running the notebook using *papermill* inside a Docker container.

Engine and Workflow Configuration
==================================

The serial workflow engine is associated with a set of workers for executing workflow steps, and a set of storage volumes that provide access to input and output files for the different workers. The configuration is read when the serial workflow engine is instantiated from the file that is referenced by the environment variable *FLOWSERV_SERIAL_ENGINECONFIG*.


Storage Volumes
---------------

Storage volumes maintain files that are accessed by different workflow steps. These files form the main part of the workflow execution context. The list of available storage volumes is maintained by a volume manager (``flowserv.volume.manager.VolumeManager``). The volume manager not only maintains instances of different storage volumes but also an index that keeps track of the files is are available at the different storage volumes.

The definition of storage volumes is part of the configuration for the workflow engine (``volumes`` section). For each storage volume the configuration contains a dictionary with the mandatory elements ``name`` and ``type`` and the optional elements ``args`` and ``files``. Each storage volume has a unique identifier (``name``) and a ``type`` that specifies the implementing class. the following volume types are currently supported:

- **fs**: Storage on the local file system (``flowserv.volume.fs.FileSystemStorage``)
- **gc**: Google Cloud Storage (``flowserv.volume.gc.GCVolume``)
- **s3**: AWS S3 Bucket Store (``flowserv.volume.s3.S3Volume``)
- **sftp**: Remote file system storage via stfp (``flowserv.volume.ssh.RemoteStorage``)

The optional ``args`` element of the volume specification contains implementation-specific key-value pairs that are passed on to the implementing volume class constructor as *kwargs* when the class is instantiated. The list of ``files`` specifies the relative path (key) of all files that are available (e.g., pre-loaded) at the storage volume.

A file that is part of the workflow execution context may be stored on several different volumes. Each worker that is used to execute an individual workflow step has access to one or more storage volumes. During workflow execution the workflow engine (via the volume manager) ensures that all files that are specified in the ``inputs`` section of the step specification are available on at least one of the storage volumes that the worker that executes the workflow step has access to.

The serial engine is associated with a dedicated storage volume for workflow run files. By default, the storage volume is the same volume that is used by the **flowServ** API. At the beginning of a workflow execution, a run directory is created on that storage volume. This is a physical directory that contains all input files that are defined by the workflow specification. The run directory can be accessed via the volume manager using the identifier ``__default__``. At the end of the workflow run, this default storage volume will contain all generated output files. From here, the files that are specified in the ``workflow/files/outputs`` section of the workflow specification will then be copied to the persistent run store of the **flowServ** API.


Workers
-------

The workflow engine has access to a set of dedicated workers. Workers are responsible for initiating and controlling the execution of workflow steps.

Workers are classified based on the type of the workflow step that they can handle, e.g., a container step worker (``flowserv.controller.worker.base.ContainerWorker``). For each class of workers there may exist several implementations for different execution backends or environments. For example, a container step worker may either execute a workflow step as a sub-process from the Python environment (``flowserv.controller.worker.subprocess.SubprocessWorker``) or using a Docker engine (``flowserv.controller.worker.docker.DockerWorker``).

Workers are specified as part of the workflow engine configuration (``workers`` section). The workers are instantiated and maintained by a worker manager (``flowserv.controller.worker.manager.WorkerPool``) that is associated with the workflow engine. The specification for each worker is a dictionary that contains the two mandatory elements ``name`` and ``type`` and three optional elements ``env``, ``variables``, and ``volume``.

Each worker has a unique identifier (``name``) and a workflow ``type`` that is used to get an instance of this worker from the worker factory. The ``type`` specifies the implementation of the worker interface (``flowserv.controller.worker.base.Worker``). The worker factory currently supports the following types:

- **docker**: Container worker that uses the Docker engine to execute container steps (``flowserv.controller.worker.docker.DockerWorker``).
- **notebook**: Worker that uses ``papermill`` to execute workflow steps that are implemented as Jupyter Notebooks (``flowserv.controller.worker.notebook.NotebookEngine``).
- **nb_docker**: Worker that runs ``papermill`` inside a Docker container to execute a Jupyer Notebook. This worker will create a new Docker image using the optional requirements that the user can specify as part of a notebook step.
- **subprocess**: Container worker that executes container steps in the Python environment that runs thw **flowServ** application (``flowserv.controller.worker.subprocess.SubprocessWorker``).

The optional ``env`` and ``variables`` elements in the worker specification contain key-value pairs that define values for environment variables and template string variables, respectively. The values for these elements are passed to the constructor of the worker class implementation as dictionaries during instantiation.

The ``volume`` elements specifies the identifier of the storage volume that the worker has access to. If the element is not present for a worker, by default the worker has access to the ``__default__`` storage volume.

Note that the type of the worker determines the type of the expected storage volume that the worker uses. For both, container worker and code worker, the expected storage volume is a file system storage volume (``flowserv.volume.fs.FileSystemStorage``).


Engine Configuration
--------------------

The specification of volumes and workers form the configuration for the serial workflow engine. The general structure of the configuration document is shown below:

.. code-block:: yaml

    volumes:
        - name: 'unique volume id'
          type: 'volume type'
          args:
            - key: 'implementation-specific key-value pairs'
              value: ''
          files:
            - 'list of file keys'
    workers:
        - name: 'unique worker id'
          type: 'worker type'
          env:
            - key: 'environment variable key-value pairs'
              value: ''
          vars:
            - key: 'template variable key-value pairs'
              value: ''
          volume: 'volume identifier'
    workflow:
        - step: 'workflow step identifier'
          worker: 'worker identifier'


The configuration for the serial workflow engine is expected to be stored in a file that is accessible via the the storage volume that is associated with the workflow engine. This file is either a JSON or YAML file with the type being determined by the file key suffix (`.json`` for JSON files and ``.yml`` or ``.yaml`` for YAML files). The relative file key for the configuration file is specified via the environment variable *SERIAL_ENGINE_CONFIG*. If the variable is not set the default workers and storage volume are used for workflow execution.

Workflow Configuration
----------------------

When executing a serial workflow, the default engine configuration can be modified by passing an optional configuration dictionary to the ``exec_workflow`` method of the workflow engine. This dictionary may contain the elements ``volumes`` and ``workers` that will override the definition of volume and workers that were used to configure the engine when the it was instantiated. In addition, the configuration dictionary may contain a ``workflow`` section that defines a mapping of workflow steps to the dedicated workers that are used to execute the workflow step. This mapping is given as a list of dictionaries containing the elements ``step`` and ``worker`` that reference the unique step identifier and worker identifier, respectively.


Workflow Execution
==================

The workflow is executed step-by-step in sequential order. For each workflow step, the engine first gets the worker that is responsible for the step execution. This is either (i) the worker that has been mapped to the workflow step in the ``workflow`` section of the configuration object, or (ii) a default worker that is dependent on the step type. For container steps, the default worker is a ``flowserv.controller.worker.subprocess.SubprocessWorker``. For code steps there currently only exists one type of worker (``flowserv.controller.worker.code.CodeWorker``).

The workflow engine then instructs the volume manager to ensure that the worker has access to all the required files (as specified in the ``files.inputs`` section of the step specification). The volume manager will copy all required files to the storage volume that the worker has access to.

When the storage volume is prepared, the worker initiates the execution of the workflow step. Once execution is completed successfully, the generated output files are registered with the volume manager for further use by other workflow steps. In case that step execution is not successful, execution of the workflow will terminate.
