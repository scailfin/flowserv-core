==================
Workflow Templates
==================


**Workflow Templates** are parameterized workflow specifications for the *Reproducible and Reusable Data Analysis Workflow Server* (**flowServ**). Workflow templates are motivated by the goal to allow users to run pre-defined data analytics workflows while providing their own input data, parameters, as well as their own code modules. Workflow templates are inspired by, but not limited to, workflow specifications for the `Reproducible Research Data Analysis Platform (REANA) <http://www.reanahub.io/>`_.



Motivation for Parameterized Workflow Templates
===============================================

Consider the `REANA Hello World Demo <https://github.com/reanahub/reana-demo-helloworld>`_. The demo workflow takes as input a file ``data/names.txt`` containing a list of person names and a timeout parameter ``sleeptime``. For each line in ``data/names.txt`` the workflow writes a line "Hello *name*!" to an output file ``results/greetings.txt``. For each line that is written to the output file, program execution is delayed by a number of seconds defined by the `sleeptime` parameter.

Workflow specifications in REANA are serialized in YAML or JSON format. The names of the input and output files as well as the value for the sleep period are currently hard-coded in the workflow specification file ( e.g.  `reana.yaml <https://raw.githubusercontent.com/reanahub/reana-demo-helloworld/master/reana.yaml>`_ ).

.. code-block:: yaml

    inputs:
      files:
        - code/helloworld.py
        - data/names.txt
      parameters:
        helloworld: code/helloworld.py
        inputfile: data/names.txt
        outputfile: results/greetings.txt
        sleeptime: 0
    workflow:
      type: serial
      specification:
        steps:
          - environment: 'python:2.7'
            commands:
              - python "${helloworld}"
                  --inputfile "${inputfile}"
                  --outputfile "${outputfile}"
                  --sleeptime ${sleeptime}
    outputs:
      files:
       - results/greetings.txt

Assume we want to build a system that allows users to run the Hello world demo via a (web-based) interface where they provide a text file with person names and a sleep period value. There are three main parts to such a system. First, we need to display a form where the user can select (upload) a text file and enter a sleep time value. Second, after the user submits their input data, we need to create an updated version of the workflow specification as shown above where we replace the value of ``inputfile`` and ``sleeptime`` with the user-provided values. We then pass the modified workflow specification to a REANA instance for execution. There are several way for implementing such a system. Parameterized workflow templates are part of the solution that is implemented for **flowServ**.



What are Parameterized Workflow Templates?
==========================================

Similar to REANA workflow specifications, parameterized workflow templates are serialized in YAML or JSON format. Each template has up to six top-level elements: ``workflow``, ``parameters``, ``parameterGroups``, ``outputs``, ``results``, and ``postproc``. Only the ``workflow`` element is mandatory in a workflow template.

The ``workflow`` element contains the workflow specification. The structure and syntax of this specification is dependent on the backend (engine) that is used to execute the final workflow. If the `REANA Workflow Engine <https://github.com/scailfin/benchmark-reana-backend>`_ is being used, the workflow specification is expected to follow the the common syntax for REANA workflow specifications.

Template Parameters
-------------------

The ``parameters`` section defines those parts of the workflow that are variable with respect to user inputs. We refer to these as *template parameters*. Template parameters can for example be used to define input and output values for workflow steps or identify Docker container images that contain the code for individual workflow steps. The detailed parameter declarations are intended to be used by front-end tools to render forms that collect user input.

An example template for the **Hello World example** is shown below.

.. code-block:: yaml

    workflow:
        inputs:
          files:
            - code/helloworld.py
            - $[[names]]
          parameters:
            helloworld: code/helloworld.py
            inputfile: $[[names]]
            outputfile: results/greetings.txt
            sleeptime: $[[sleeptime]]
        workflow:
          type: serial
          specification:
            steps:
              - environment: 'python:2.7'
                commands:
                  - python "${helloworld}"
                      --inputfile "${inputfile}"
                      --outputfile "${outputfile}"
                      --sleeptime ${sleeptime}
        outputs:
          files:
           - results/greetings.txt
    parameters:
        - name: names
          label: Person names
          description: Text file containing person names
          dtype: file
        - name: sleeptime
          label: Sleep period
          description: Sleep period in seconds
          dtype: int


In this example, the workflow section is a REANA workflow specification. The main modification to the workflow specification is a simple addition to the syntax in order to allow references to template parameters. Such references are always enclosed in ``$[[...]]``. The parameters section is a list of template parameter declarations. Each parameter declaration has a unique identifier. The identifier is used to reference the parameter from within the workflow specification (e.g., ``$[[sleeptime]]`` to reference the user-provided value for the sleep period). Other elements of the parameter declaration are a human readable short name, a parameter description, and a specification of the data type. Refer to the `Template Parameter Specification <https://github.com/scailfin/flowserv-core/blob/master/docs/parameters.rst>`_ for a full description of the template parameter syntax.

Parameter declarations are intended to be used by front-end tools to render forms that collect user input. Given a set of user-provided values for the template parameters, the references to parameters are replaced withing the workflow specification with the given values to generate a valid workflow that can be executed by the respective workflow engine.


Grouping of Template Parameters
-------------------------------

Template parameters can be grouped for display purposes. In a front-end application, each parameter group should be rendered within a separate visual components. The details are dependent on the application.

The structure for the ``parameterGroups`` element in a workflow template is as follows:

.. code-block:: yaml

    parameterGroups:
        - name: 'Unique module name'
          title: 'Module title for display purposes'
          index: 'Index position of the parameter block for ordering during visualization'

The group that a parameter belongs to is reference by the unique group name in the ``parameterGroups`` element of the parameter declaration.


Workflow Outputs
----------------

Workflow specifications like those defined by `REANA <http://www.reanahub.io/>`_ include a list of output files and directories that are generated by each workflow run. The workflow template allows a user to further specify properties for all/some of these output files that are used by front-end applications for display purposes.

If an ``outputs`` element is present in a workflow template only those files that are listed in the section will be available for individual download via the API. Otherwise, if no ``outputs`` element is present all files that are returned by the workflow are accessible via the API. Note that the granularity depends on the (implementation-specific)  listing of result files for the workflow specification.

The structure for the ``outputs`` element in a workflow template is as follows:

.. code-block:: yaml

    outputs:
        - source: 'Relative path to the file in the run result folder'
          key: 'Unique user-defined key for the resource that can be used for
            accessing the resource in a dictionary (e.g., in the flowapp
            result object)'
          title: 'Header when displaying the file contents (optional)'
          caption: 'Caption when displaying the file contents (optional)'
          format: 'Object containing information about file format (optional)'
          widget: 'Object containing information that specifies the widget to be
            used for displaying the file content (optional)'

The structure of the ``format`` and ``widget`` element is not further specified. These elements are interpreted by the front-end applications only. See the definition of default file formats and widgets page for details on the supported values for these elements.


Benchmark Templates
===================

The definition of workflow templates is intended to be generic to allow usage in a variety of applications. With respect to *Reproducible Open Benchmarks* we define extensions of workflow templates that are used to generate the benchmark leader board and compute benchmark metrics.


**Benchmark Templates** extend the base templates with information about the schema of the benchmark results. The idea is that benchmark workflows contain steps towards the end that evaluate the results of a benchmark run. These evaluation results are stored in a simple JSON or YAML file. Result files are usedto create the benchmark leader board.


Benchmark Results
-----------------

Benchmark templates add a ``results`` section to a parameterized workflow template.

.. code-block:: yaml

    workflow:
        version: 0.3.0
        inputs:
          files:
            - code/analyze.py
            - code/helloworld.py
            - $[[names]]
          parameters:
            inputfile: $[[names]]
            outputfile: results/greetings.txt
            sleeptime: $[[sleeptime]]
            greeting: $[[greeting]]
        workflow:
          type: serial
          specification:
            steps:
              - environment: 'python:3.7'
                commands:
                  - python code/helloworld.py
                      --inputfile "${inputfile}"
                      --outputfile "${outputfile}"
                      --sleeptime ${sleeptime}
                      --greeting ${greeting}
                  - python code/analyze.py
                      --inputfile "${outputfile}"
                      --outputfile results/analytics.json
        outputs:
          files:
           - results/greetings.txt
           - results/analytics.json
    parameters:
        - name: names
          label: 'Input file'
          datatype: file
          as: data/names.txt
        - name: sleeptime
          datatype: int
          defaultValue: 10
        - name: greeting
          datatype: string
          defaultValue: 'Hello'
    results:
        file: results/analytics.json
        schema:
            - name: avg_count
              label: 'Avg. Chars per Line'
              type: decimal
            - name: max_len
              label: 'Max. Output Line'
              type: decimal
            - name: max_line
              label: 'Longest Output'
              type: string
              required: False
        orderBy:
            - name: avg_count
              sortDesc: true
            - name: max_len
              sortDesc: false


The ``results`` section has three parts: (1) a reference to the result ``file`` that contains the benchmark run results, (2) the specification of the elements (columns) in the benchmark result ``schema``, and (3) the default sort order (``orderBy``) when generating a leader board. The schema is used to extract information from the result file and store the results in a database. In the given example, the benchmark results contain the average number of characters per line that is written by ``helloworld.py``, and the length and text of the longest line in the output. When generating the leader board results are sorted by the average number of characters (in descending order) and the length of the longest line (in ascending order).

The benchmark results are generated by the second command in the workflow step by the ``analyze.py`` script that is part of the benchmark template.

.. code-block:: python

    """Analytics code for the adopted hello world Demo. Reads a text file (as
    produced by the helloworld.py code) and outputs the average number of characters
    per line and the number of characters in the line with the most characters.
    """

    import argparse
    import errno
    import os
    import json
    import sys


    def main(inputfile, outputfile):
        """Write greeting for every name in a given input file to the output file.
        The optional waiting period delays the output between each input name.
        """
        # Count number of lines, characters, and keep track of the longest line
        max_line = ''
        total_char_count = 0
        line_count = 0
        with open(inputfile, 'r') as f:
            for line in f:
                line = line.strip()
                line_length = len(line)
                total_char_count += line_length
                line_count += 1
                if line_length > len(max_line):
                    max_line = line
        # Create results object
        results = {
            'avg_count': total_char_count / line_count,
            'max_len': len(max_line),
            'max_line': max_line
        }
        # Write analytics results. Ensure that output directory exists:
        # influenced by http://stackoverflow.com/a/12517490
        dir_name = os.path.dirname(outputfile)
        if dir_name != '':
            if not os.path.exists(dir_name):
                try:
                    os.makedirs(dir_name)
                except OSError as exc:  # guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise
        with open(outputfile, "w") as f:
            json.dump(results, f)


    if __name__ == '__main__':
        args = sys.argv[1:]

        parser = argparse.ArgumentParser()
        parser.add_argument("-i", "--inputfile", required=True)
        parser.add_argument("-o", "--outputfile", required=True)

        parsed_args = parser.parse_args(args)

        main(inputfile=parsed_args.inputfile, outputfile=parsed_args.outputfile)


Result Schema Specification
---------------------------

The result schema specification defines a list of columns that correspond to columns in a table that is created in an underlying relational database to store benchmark results. For each column specification the following elements are allowed:

- **name**: Unique column identifier. The value is used as the column name in the created database table.
- **label**: Human-readable name that is used when displaying leader boards in a front-end.
- **type**: Data type of the result values. The supported types are ``decimal``, ``int``, and ``string``. These type are translated into the relational database types ``DOUBLE``, ``INTEGER``, and ``TEXT``, respectively.
- **required**: Boolean value that corresponds to a ``NOT NULL`` constraint. If the value is ``true`` it is expected that the generated benchmark result contains a value for this column. The default value is ``true``.

The first three elements (``name``, ``label``, and ``type``) are mandatory.


Generating Leader Board
-----------------------

Leader boards are generated from benchmark results in the database table. The default sort order for results determines the ranking of entries in the leader board. It is defined in the ``orderBy`` section of the benchmark result specification. The ``orderBy`` section is a list of columns together with the sort order for column values. This list corresponds to an ORDER BY clause in the SQL query that is used to retrieve benchmark results.

Each entry in the ``orderBy`` list has the following elements:

- **name**: Unique column identifier
- **sortDesc**: Boolean value to determine the sort order (true: DESCENDING or false: ASCENDING).

Only the ``name`` element is mandatory. The value has to match one of the column identifiers in the ``schema`` section. By default all columns are sorted in descending order. If no ``orderBy`` element is given the first column in the ``schema`` is used as the sort column.
