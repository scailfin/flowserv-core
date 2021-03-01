============================
Packaging Workflow Templates
============================

This file describes how to structure your project and the necessary files that make it easy to add your workflow to the flowServ repository.

Here is a sample layout based on the `Hello World Demo <https://github.com/scailfin/rob-demo-hello-world>`_ as an example.

.. code-block:: console

    template/
        code/
            helloworld.py
        data/
            names.txt
    flowserv.yaml
    instructions.txt
    benchmark.yaml


The `template` folder contains all the code and data files that are provided to the user to run the workflow. The `workflow.yaml` file contains the `template specification <https://github.com/scailfin/flowserv-core/blob/master/docs/workflow.rst>`_. User instructions for running the workflow are in the markdown file `instructions.md`. The `flowserv.yaml` file contains all the necessary metadata that is required when adding the workflow template to a flowServ repository.



Project Manifest File
---------------------

The project manifest file contains the necessary information when adding a workflow template to a flowServ repository. A template is added to the flowServ repository by specifying the base folder or git repository for the project. The template loader will look for a manifest with name `flowserv.json`, `flowserv.yaml`, or `flowserv.yml` (in that order) in the project folder. The structure of the manifest file is shown below:

.. code-block:: yaml

    name: 'Hello World Demo'
    description: 'Hello World Demo for ROB'
    instructions: 'instructions.txt'
    files:
        - source: 'template/code'
          target: 'code'
    specfile: 'benchmark.yaml'

The `name` and `description` define the project title and a description for display, e.g., in the `ROB UI <https://github.com/scailfin/rob-ui>`_. The `instructions` element refers to the user instructions file. Note that all file references in the description file are relative to the project base folder (e.g., the folder that contains the cloned git repository).

The list of `files` defines the source files (and folders) and their target paths that are copied from the project folder to the template repository. If the `target` element is omitted the empty string is used as the default value. The files that are copied to the template repository define the environment that is created each time the user runs the template workflow.

The `specfile` element points to the workflow specification file.

All elements (except for `files`) in the description file can be overridden by command line arguments when adding the template to the repository. If no `files` element (or no description file is given) all files and folders in the project base folder are copied recursively to the template repository.
