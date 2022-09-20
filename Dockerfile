FROM python:3.9
COPY flowserv /app/flowserv
COPY README.rst /app/README.rst
COPY setup.py /app/setup.py
WORKDIR /app
RUN pip install /app[gui]
RUN rm -Rf /app
WORKDIR /
