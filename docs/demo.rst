===============================================
How To Run 'Hello World' Demo from Command Line
===============================================

Below is the sequence of steps that can be used to run the ROB Hello World demo to test if an installation of **flowserv** is working properly. This example assumes that you have installed flowserv-core in you Python environment.

.. code-block:: console

    # Create/Go to an empty directory (optional)
    $> cd /home/user/flowserv-test

    # Set the following environment variables
    # Directory for all workflow files. Use a sub-directory
    # in the current workin directory.
    $> export FLOWSERV_API_DIR=$PWD/.flowserv
    # Database file (uses SQLite database)
    $> export FLOWSERV_DATABASE=sqlite:///$FLOWSERV_API_DIR/flowserv.db
    # Open access policy to avoid having to register users and login
    $> export FLOWSERV_AUTH=open

    # Create the database
    flowserv init

    # Install the 'Hello World' demo (-k specifies the identifier)
    $> flowserv app install -k helloworld helloworld

    # Set workflow identifier to point to the installed Hello World demo
    $> export FLOWSERV_APP=helloworld

    # Upload a file with names (need to create the file first)
    # Note that the file identifier will be different.
    $> flowserv files upload -i names.txt
    Uploaded 'names.txt' with ID 2c98a278acdb41a3bb572ae4fbea5177.

    # Start a new workflow run. Will ask you to input run parameters.
    # Input files are specified by their identifier
    $> flowserv runs start

    Select file identifier from uploaded files:

    ID                               | Name      | Created at
    ---------------------------------|-----------|--------------------
    2c98a278acdb41a3bb572ae4fbea5177 | names.txt | 2021-02-04T20:32:35

    Names File (file) $> 2c98a278acdb41a3bb572ae4fbea5177
    Sleep for (sec.) (float) [default '10'] $> 1
    Greeting (string) [default 'Hello'] $> Hey
    started run 07993089f08f42cd89485c3a53f11766 is SUCCESS

    # Show details for the completed run (all identifier will be different)
    $> flowserv runs show 07993089f08f42cd89485c3a53f11766
    ID: 07993089f08f42cd89485c3a53f11766
    Started at: 2021-02-04T20:32:56
    Finished at: 2021-02-04T20:33:00
    State: SUCCESS

    Arguments:
      names = 2c98a278acdb41a3bb572ae4fbea5177 (data/names.txt)
      sleeptime = 1.0
      greeting = Hey

    Files:
      db7741f03b3b475ab8288d66871a0b3e (results/greetings.txt)
      0cebe830b8074a3faf2c95dc756f3a77 (results/analytics.json)

    # Download the greetings file (identifier will be different)
    $> flowserv runs download file -f db7741f03b3b475ab8288d66871a0b3e -o greetings.txt 07993089f08f42cd89485c3a53f11766