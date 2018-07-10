FROM python:alpine
MAINTAINER francois.lanusse@gmail.com

RUN apk add --no-cache git

# Enable root mode, so we can install things more easily
USER root

RUN pip install -U pip setuptools
RUN pip install ruamel.yaml
RUN pip install git+https://github.com/LSSTDESC/ceci.git@cwltool

USER vagrant
