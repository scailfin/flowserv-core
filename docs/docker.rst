===================================
Build Docker Container for flowServ
===================================

Build Docker container image containing the flowServ code base:

.. code-block:: bash

    docker image build -t flowserv:0.8.0 .


Push container image to DockerHub.

.. code-block:: bash

    docker image tag flowserv:0.8.0 heikomueller/flowserv:0.8.0
    docker image push heikomueller/flowserv:0.8.0
