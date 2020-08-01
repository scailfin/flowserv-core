# Docker Container for flowServ

Build Docker container image containing the flowServ code base:

```
docker image build -t flowserv:0.2.0 .
```

Push container image to DockerHub.

```
docker image tag flowserv:0.2.0 heikomueller/flowserv:0.2.0
docker image push heikomueller/flowserv:0.2.0
```
