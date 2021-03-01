===================
Template Parameters
===================

Template parameters are used to define the variable parts of workflow templates. One of the primary use cases for parameter declarations is to render input forms to collect user input data.

The mandatory elements of parameter declarations are:

- **name**: Unique identifier for the parameter. This identifier is used in the template workflow specification to reference the parameter value.
- **label**: Descriptive short-name for the parameter (to be displayed in a front-end input form).
- **dtype**: Type of the expected value. Valid data types are ``bool``, ``select``, ``file``, ``float``, ``int``, and ``string``.
- **index**: The index defines the order in which parameters are presented in the front-end input form.
- **isRequired**: Boolean flag indicating whether the user is required to provide a value for the parameter or not.

In addition, several optional elements can be given for a parameter declaration:

- **description**: Additional descriptive information about the parameter (to be displayed in a front-end input form).
- **defaultValue**: Default value for the parameter.
- **module**: Identifier of the group (module) that the parameter belongs to.

Depending on the data type of a parameter, additional element can be present in the declaration.


Enumeration Parameters
----------------------

Parameters of type ``select`` have a mandatory element ``values`` that specifies the valid parameter values. Each entry in the values list contains (up-to) three elements:

- **name**: Display name for the value.
- **value**: Actual value if this item is selected by the user.
- **isDefault**: Optional element to declare a list item as the default value (for display purposes).

An example declaration for an enumeration parameter is shown below:

.. code-block:: yaml

    - name: 'imageType'
      label: 'Image Type'
      description: 'The type of micrscopy used to generate images'
      type: 'select'
      defaultValue: 'brightfield'
      values:
          - name: 'Brightfield'
            value: 'brightfield'
            isDefault: true
          - name: 'Phasecontrast'
            value: 'phasecontrast'
            isDefault: false
      isRequired: true


Input File Parameters
---------------------

Parameters of type ``file`` have one additional optional element ``target``. The file target specifies the (relative) target path for an uploaded input file in the run folder. If the target is not specified in the parameter declaration it can be provided by the user as part of the arguments for a workflow run. Note that the default value for a file parameter points to an existing file in the workflow template's file structure but is also used as the default target path.

An example declaration for a file parameter is shown below:

.. code-block:: yaml

    - name: 'names'
      label: 'Names File'
      dtype: 'file'
      target: 'data/names.txt'
      index: 0
      isRequired: true


Numeric Parameters
------------------

Parameters of type ``float`` or ``int`` have an optional ``range`` element to specify constraints for valid input values. Range constraints are intervals that are represented as strings. Round brackets are used to define open intervals and square brackets define closed intervals. A missing interval boundary is interpreted as (positive or negative) infinity. An open interval does not include the endpoints and a closed interval does.

An example declaration for a integer parameter is shown below:

.. code-block:: yaml

    - name: 'maxProportion'
      label: 'Max. Proportion'
      dtype: 'float'
      index: 3
      defaultValue: 0.75
      range: '[0,1]'
