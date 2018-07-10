FROM python:3
MAINTAINER francois.lanusse@gmail.com

RUN pip install -U pip setuptools
RUN pip install ruamel.yaml==0.15.42 cwlgen pyyaml parsl descformats cwltool
RUN pip install --no-deps git+https://github.com/LSSTDESC/ceci.git@cwltool
