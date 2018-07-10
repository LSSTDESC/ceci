FROM python:3
MAINTAINER francois.lanusse@gmail.com

# Enable root mode, so we can install things more easily
USER root

RUN pip install git+https://github.com/LSSTDESC/ceci.git@cwltool

USER vagrant
