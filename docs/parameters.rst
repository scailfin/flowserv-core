===================
Template Parameters
===================

Template parameters are used to define the variable parts of workflow templates. One of the primary use cases for parameter declarations is to render input forms to collect user input data.



Parameter Declarations
======================

The main elements of parameter declarations are:

- **id**: Unique identifier for the parameter. This identifier is used in the template workflow specification to reference the parameter value.
- **name**: Descriptive short-name for the parameter (to be displayed in a front-end input form).
- **description**: Additional descriptive information about the parameter (to be displayed in a front-end input form).
- **datatype**: Type of the expected value. Valid data types are ``bool``, ``decimal``, ``file``, ``int``, ``list``, ``record``, and ``string``.
- **defaultValue**: Default value for the parameter
- **values**: List of allowed values. Value lists are for example used to render drop-down elements in the front-end input form.
- **required**: Boolean flag indicating whether the user is required to provide a value for the parameter or not.
- **as**: Specify an alternate target value for the user-provided value. This property is primarily intended for parameters of type ``file``. It provides flexibility with respect to renaming the file that a user uploads (see below).
- **index**: The index defines the order in which parameters are presented in the front-end input form.
- **parent**: Identifier of the parent element for parameters that are part of a list or record.

Only the parameter ``id`` is mandatory. All other elements are optional. If no ``name`` is given the ``id`` is used as the name. If no ``description`` is given the ``name`` is used as description. The default ``datatype`` is ``string``.

The data types ``bool``, ``decimal``, ``int``, and ``string`` represent the standard raw data types for scalar values. A parameter of type ``file`` expects the user to upload a file. Data type ``record`` is used to group parameters. The elements of the record have to reference the record element as their ``parent``. Note that data types ``list`` and ``record`` are included for future reference. These types are not supported properly by current implementations of the benchmark engine.



Example
=======

The following example declares two input parameters for a workflow template. Within the workflow specification the parameter values are referenced as ``$[[outputFormat]]`` and ``$[[repeatCount]]``, respectively.

.. code-block:: yaml

    parameters:
        - id: outputFormat
          name: 'Output file format'
          description: 'Format of the generated output file (JSON or YAML files are currently supported)'
          datatype: int
          values:
            - name: JSON
              value: 0
              isDefault: true
            - name: YAML
              value: 1
          index: 0
        - id: threshold
          name: 'Threshold'
          description: 'Threshold that is relevant for a parameterized workflow step'
          datatype: decimal
          defaultValue: 4.2
          index: 1


The first parameter ``outputFormat`` defines a list of possible values. For each value the ``name`` defines the displayed name while ``value`` defines the resulting value if the respective entry is selected. Only one of the values in the list can be declared as the default value. An input form would render the output format parameter before the threshold parameter.

See the `Workflow & Benchmark Templates <https://github.com/scailfin/flowserv-core/blob/master/docs/workflow.rst>`_ document for more examples of template parameters and their usage within workflow templates.



File Parameters
===============

Template parameters of type ``file`` are intended to allow users to upload files from their local computer to the server that executes a workflow template. File parameter specifications may include the ``as`` property to specify the target path of the uploaded file. Consider the example below. in this example, the file that the user uploads will available at the path ``data/names.txt`` in the environment where the workflow is executed.

.. code-block:: yaml

    parameters:
        - id: names
          name: 'Input file'
          datatype: file
          defaultValue: 'input/names.txt'
          as: 'data/names.txt'


 If the ``as`` property is not set, the ``defaultValue`` will be used as the target path. Note that the ``defaultValue`` will be the source path for the file if no argument value for the parameter ``names`` is given. That is, the file at path ``input/names.txt`` will be used as input if the user does not upload a file for ``names``. The target path will be ``data/names.txt``.

The ``as`` property may take the special value ``$input``. In this case, the target path is not predefined but provided by the user it as part of the input. The use of ``$input`` is intended to allow flexibility around the type of environment (i.e., Docker image) the user-provided code runs in. Consider the following modification of the *Hello World* example where the user can provide their own implementation of the code that generates the greetings:

.. code-block:: yaml

    workflow:
        version: 0.3.0
        inputs:
          files:
            - $[[code]]
            - data/names.txt
          parameters:
            codefile: $[[code]]
            inputfile: data/names.txt
            outputfile: results/greetings.txt
            sleeptime: $[[sleeptime]]
            greeting: $[[greeting]]
        workflow:
          type: serial
          specification:
            steps:
              - environment: $[[env]]
                commands:
                  - $[[cmd]]
        outputs:
          files:
           - results/greetings.txt
    parameters:
        - id: code
          name: 'Code file'
          description: 'File containing the executable code to run Hello World'
          datatype: file
          as: $input
          defaultValue: 'code/helloworld.py'
        - id: env
          name: 'Docker Image'
          description: 'Docker image that runs the executable'
          datatype: string
          defaultValue: 'python:3.7'
        - id: cmd
          name: 'Command line'
          datatype: string
          defaultValue: 'python code/helloworld.py
                  --inputfile "${inputfile}"
                  --outputfile "${outputfile}"
                  --sleeptime ${sleeptime}
                  --greeting ${greeting}'
        - id: sleeptime
          datatype: int
          defaultValue: 10
        - id: greeting
          datatype: string
          defaultValue: 'Hello'

In this example the user is not required to provide a Python implementation like helloworld.py but could use other programming languages like Java. Assume that the user has a Jar-File named ``HelloWorld.jar`` that takes four command line arguments input file, output file, sleep time, and greeting phrase. In this case they could set the ``as`` value of the ``code`` parameter to ``code/HelloWorld.jar`` when uploading the file, user ``java:8`` as the value for ``env``, and provide the command line ``java -jar code/HelloWorld.jar "${inputfile}" "${outputfile}" ${sleeptime} ${greeting}`` to run their code (as value for parameter ``cmd``).



Parameter Declaration Schema
============================

The JSON schema for template parameters is shown below:

.. code-block:: yaml

    properties:
      as:
        type: string
      datatype:
        type: string
      defaultValue:
        oneOf:
        - type: boolean
        - type: string
        - type: number
      description:
        type: string
      id:
        type: string
      index:
        type: number
      name:
        type: string
      parent:
        type: string
      required:
        type: boolean
      values:
        items:
          properties:
            isDefault:
              type: boolean
            name:
              type: string
            value:
              oneOf:
              - type: boolean
              - type: string
              - type: number
          required:
          - value
          type: object
        type: array
    required:
    - id
    type: object
